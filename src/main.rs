
//! telegraph: an MCP server that lets Vibe run shell commands and manage
//! files on the Linux laptop this process runs on.
//!
//! Modes:
//!   telegraph              stdio transport (for `vibe mcp add ...` on the laptop)
//!   telegraph --http       Streamable HTTP on 127.0.0.1:8766 with bearer-token
//!                            auth (token auto-generated at ~/.telegraph/token),
//!                            for the Vibe Work custom connector behind an HTTPS tunnel.
//!
//! Build: cargo build --release   (a fully static musl binary also works)

use std::path::PathBuf;
use std::sync::Arc;

use rmcp::transport::streamable_http_server::{session::local::LocalSessionManager, StreamableHttpService};
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ContentBlock},
    schemars,
    tool, tool_handler, tool_router,
    transport::{stdio, StreamableHttpServerConfig},
    ErrorData as McpError, ServerHandler, ServiceExt,
};
use serde::Deserialize;

use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use axum::Router;

const MAX_OUTPUT_BYTES: usize = 100_000;
const DEFAULT_TIMEOUT_SECS: u64 = 120;
const MAX_TIMEOUT_SECS: u64 = 600;
const DEFAULT_HTTP_BIND: &str = "127.0.0.1:8766";
const MAX_DIR_ENTRIES: usize = 500;
const DEFAULT_READ_LINES: usize = 500;

// ---------------------------------------------------------------------------
// Server + tools
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RemoteServer;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ExecParams {
    /// Shell command to run on the laptop. Executed with `bash -lc`, so the
    /// login profile PATH applies (includes ~/.cargo/bin, ~/.local/bin).
    command: String,
    /// Working directory for the command. Defaults to $HOME.
    #[serde(default)]
    cwd: Option<String>,
    /// Timeout in seconds. Default 120, max 600.
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ReadFileParams {
    /// Absolute path of the file on the laptop.
    path: String,
    /// Zero-based line number to start reading from.
    #[serde(default)]
    offset: Option<usize>,
    /// Maximum number of lines to return. Default 500.
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct WriteFileParams {
    /// Absolute path of the file on the laptop.
    path: String,
    /// Full file content to write. Replaces the file if it exists.
    content: String,
    /// Create missing parent directories. Default true.
    #[serde(default = "default_true")]
    create_dirs: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListDirParams {
    /// Absolute directory path on the laptop. Defaults to $HOME.
    #[serde(default)]
    path: Option<String>,
}

#[tool_router]
impl RemoteServer {
    #[tool(
        name = "exec",
        description = "Run a shell command on the remote Linux laptop via `bash -lc` and return stdout, stderr and the exit code.",
        annotations(title = "Run command on laptop", destructive_hint = true, idempotent_hint = false)
    )]
    async fn exec(
        &self,
        Parameters(ExecParams { command, cwd, timeout_secs }): Parameters<ExecParams>,
    ) -> Result<CallToolResult, McpError> {
        let timeout = timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS).min(MAX_TIMEOUT_SECS);
        let cwd = cwd.map(PathBuf::from);
        if let Some(p) = &cwd {
            if !p.is_dir() {
                return Ok(tool_error(format!("cwd does not exist: {}", p.display())));
            }
        }

        let mut cmd = tokio::process::Command::new("bash");
        cmd.args(["-lc", &command])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        if let Some(p) = &cwd {
            cmd.current_dir(p);
        }

        let child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => return Ok(tool_error(format!("failed to spawn bash: {e}"))),
        };

        let output = match tokio::time::timeout(
            std::time::Duration::from_secs(timeout),
            child.wait_with_output(),
        )
        .await
        {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => return Ok(tool_error(format!("command failed: {e}"))),
            Err(_) => {
                return Ok(tool_error(format!(
                    "command timed out after {timeout}s (process killed)"
                )))
            }
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let mut text = match output.status.code() {
            Some(0) => "exit code: 0\n".to_string(),
            Some(c) => format!("exit code: {c}\n"),
            None => "exit code: killed by signal\n".to_string(),
        };
        if !stdout.trim().is_empty() {
            text.push_str("--- stdout ---\n");
            text.push_str(&truncate(&stdout, MAX_OUTPUT_BYTES));
            text.push('\n');
        }
        if !stderr.trim().is_empty() {
            text.push_str("--- stderr ---\n");
            text.push_str(&truncate(&stderr, MAX_OUTPUT_BYTES));
            text.push('\n');
        }
        Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
    }

    #[tool(
        name = "read_file",
        description = "Read a text file from the laptop, line-based with offset and limit.",
        annotations(title = "Read file on laptop", read_only_hint = true)
    )]
    async fn read_file(
        &self,
        Parameters(ReadFileParams { path, offset, limit }): Parameters<ReadFileParams>,
    ) -> Result<CallToolResult, McpError> {
        let offset = offset.unwrap_or(0);
        let limit = limit.unwrap_or(DEFAULT_READ_LINES);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => return Ok(tool_error(format!("cannot read {path}: {e}"))),
        };
        let lines: Vec<&str> = content.lines().collect();
        if offset >= lines.len() {
            return Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "(empty: file has {} lines, offset {offset} is past the end)",
                lines.len()
            ))]));
        }
        let end = (offset + limit).min(lines.len());
        let mut text = format!("lines {}-{} of {}\n", offset, end - 1, lines.len());
        text.push_str(&lines[offset..end].join("\n"));
        Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
    }

    #[tool(
        name = "write_file",
        description = "Write (create or replace) a text file on the laptop. Returns the number of bytes written.",
        annotations(title = "Write file on laptop", destructive_hint = true, idempotent_hint = false)
    )]
    async fn write_file(
        &self,
        Parameters(WriteFileParams { path, content, create_dirs }): Parameters<WriteFileParams>,
    ) -> Result<CallToolResult, McpError> {
        let path = PathBuf::from(&path);
        if create_dirs {
            if let Some(parent) = path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    return Ok(tool_error(format!(
                        "cannot create parent dir {}: {e}",
                        parent.display()
                    )));
                }
            }
        }
        match std::fs::write(&path, &content) {
            Ok(()) => Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "wrote {} bytes to {}",
                content.len(),
                path.display()
            ))])),
            Err(e) => Ok(tool_error(format!("cannot write {}: {e}", path.display()))),
        }
    }

    #[tool(
        name = "list_dir",
        description = "List a directory on the laptop: entry names, types and file sizes, directories first.",
        annotations(title = "List directory on laptop", read_only_hint = true)
    )]
    async fn list_dir(
        &self,
        Parameters(ListDirParams { path }): Parameters<ListDirParams>,
    ) -> Result<CallToolResult, McpError> {
        let dir = path.map(PathBuf::from).unwrap_or_else(home_dir);
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) => return Ok(tool_error(format!("cannot read {}: {e}", dir.display()))),
        };
        let mut rows: Vec<(bool, String, u64)> = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            rows.push((is_dir, name, size));
            if rows.len() > MAX_DIR_ENTRIES * 2 {
                break;
            }
        }
        rows.sort_by(|a, b| (!a.0).cmp(&!b.0).then_with(|| a.1.cmp(&b.1)));
        let truncated = rows.len() > MAX_DIR_ENTRIES;
        rows.truncate(MAX_DIR_ENTRIES);
        let mut text = String::new();
        for (is_dir, name, size) in &rows {
            if *is_dir {
                text.push_str(&format!("{name}/\n"));
            } else {
                text.push_str(&format!("{name}\t{size} bytes\n"));
            }
        }
        if truncated {
            text.push_str(&format!("(truncated at {MAX_DIR_ENTRIES} entries)\n"));
        }
        if text.is_empty() {
            text.push_str("(empty directory)\n");
        }
        Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
    }

    #[tool(
        name = "sysinfo",
        description = "Report the laptop's current state: hostname, kernel, uptime, load, memory, disk usage and battery.",
        annotations(title = "Laptop system info", read_only_hint = true)
    )]
    async fn sysinfo(&self) -> Result<CallToolResult, McpError> {
        let mut lines: Vec<String> = Vec::new();
        if let Ok(h) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
            lines.push(format!("host: {}", h.trim()));
        }
        if let Ok(k) = std::fs::read_to_string("/proc/sys/kernel/osrelease") {
            lines.push(format!("kernel: {}", k.trim()));
        }
        if let Ok(u) = std::fs::read_to_string("/proc/uptime") {
            if let Some(secs) = u.split_whitespace().next().and_then(|v| v.parse::<u64>().ok()) {
                lines.push(format!(
                    "uptime: {}d {}h {}m",
                    secs / 86400,
                    (secs % 86400) / 3600,
                    (secs % 3600) / 60
                ));
            }
        }
        if let Ok(l) = std::fs::read_to_string("/proc/loadavg") {
            lines.push(format!("load: {}", l.trim()));
        }
        if let Ok(mi) = std::fs::read_to_string("/proc/meminfo") {
            let get = |key: &str| {
                mi.lines()
                    .find(|l| l.starts_with(key))
                    .and_then(|l| l.split_whitespace().nth(1))
                    .and_then(|v| v.parse::<u64>().ok())
            };
            if let (Some(total), Some(avail)) = (get("MemTotal:"), get("MemAvailable:")) {
                lines.push(format!("mem: {} MiB available of {} MiB", avail / 1024, total / 1024));
            }
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| "/".into());
        if let Ok(out) = tokio::process::Command::new("df")
            .args(["-hP", "/", &home])
            .output()
            .await
        {
            for l in String::from_utf8_lossy(&out.stdout).lines() {
                if !l.starts_with("Filesystem") {
                    lines.push(format!("disk: {l}"));
                }
            }
        }
        if let Ok(psupplies) = std::fs::read_dir("/sys/class/power_supply") {
            for entry in psupplies.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let cap = std::fs::read_to_string(entry.path().join("capacity"))
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default();
                let status = std::fs::read_to_string(entry.path().join("status"))
                    .map(|s| s.trim().to_string())
                    .unwrap_or_default();
                if !cap.is_empty() {
                    lines.push(format!("battery {name}: {cap}% ({status})"));
                }
            }
        }
        if lines.is_empty() {
            lines.push("(nothing readable from /proc or /sys)".into());
        }
        Ok(CallToolResult::success(vec![ContentBlock::text(lines.join("\n"))]))
    }
}

#[tool_handler(
    name = "telegraph",
    version = "0.1.0",
    instructions = "This server runs on the remote Linux laptop (KDE Plasma). All tools act on the laptop itself: exec runs shell commands (bash -lc), read_file/write_file/list_dir manage files, sysinfo reports system state. Paths are absolute Linux paths. Prefer exec for anything not covered by a dedicated tool."
)]
impl ServerHandler for RemoteServer {}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tool_error(msg: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(msg)])
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut cut = max;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}\n[truncated at {max} bytes]", &s[..cut])
}

fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

// ---------------------------------------------------------------------------
// HTTP mode: bearer-token auth + Streamable HTTP
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AuthState {
    token: String,
}

fn token_path() -> PathBuf {
    std::env::var("TELEGRAPH_TOKEN_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home_dir().join(".telegraph").join("token"))
}

fn load_or_create_token() -> std::io::Result<(String, PathBuf)> {
    let path = token_path();
    if let Ok(existing) = std::fs::read_to_string(&path) {
        let token = existing.trim().to_string();
        if !token.is_empty() {
            return Ok((token, path));
        }
    }
    let mut buf = [0u8; 32];
    use std::io::Read;
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    let token: String = buf.iter().map(|b| format!("{b:02x}")).collect();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, &token)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok((token, path))
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

async fn auth_middleware(
    State(state): State<Arc<AuthState>>,
    headers: HeaderMap,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let ok = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .map(|t| constant_time_eq(t, &state.token))
        .unwrap_or(false);
    if ok {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None | Some("--stdio") => run_stdio().await,
        Some("--http") => run_http(args.get(1).map(String::as_str)).await,
        Some("--help") | Some("-h") => {
            eprintln!("telegraph: MCP server exposing this Linux machine to Vibe");
            eprintln!();
            eprintln!("Usage:");
            eprintln!("  telegraph            stdio transport (default; for `vibe mcp add`)");
            eprintln!("  telegraph --http [ADDR]  Streamable HTTP (default {DEFAULT_HTTP_BIND}), bearer auth");
            Ok(())
        }
        Some(other) => {
            eprintln!("unknown argument: {other} (see --help)");
            std::process::exit(2);
        }
    }
}

async fn run_stdio() -> Result<(), Box<dyn std::error::Error>> {
    // stdout carries MCP protocol messages; logs go to stderr only.
    let service = RemoteServer.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}

async fn run_http(bind: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let bind = bind.unwrap_or(DEFAULT_HTTP_BIND);
    let bind_addr: std::net::SocketAddr = bind
        .parse()
        .map_err(|e| format!("invalid bind address {bind:?}: {e}"))?;

    let (token, token_file) = load_or_create_token()?;
    let state = Arc::new(AuthState { token: token.clone() });

    let service: StreamableHttpService<RemoteServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(RemoteServer),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default(),
        );

    let mcp_router = Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(state, auth_middleware));

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .merge(mcp_router);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    eprintln!("telegraph listening on http://{bind}/mcp (POST, Streamable HTTP)");
    eprintln!("bearer token file: {}", token_file.display());
    eprintln!("token: {token}");
    eprintln!("point Vibe Work's custom connector at an HTTPS tunnel to this address with this bearer token");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
