# telegraph

Agent-integrated MCP shell connector for Vibe. Runs on a Linux machine; Vibe agents run shell commands and manage files on it remotely.

## Usage

    cargo build --release

stdio (Vibe CLI):

    vibe mcp add remote --transport stdio --command /path/to/telegraph

Streamable HTTP (custom connector, bearer token at `~/.telegraph/token`):

    telegraph --http [ADDR]    # default 127.0.0.1:8766, endpoint POST /mcp

Tools: `exec`, `read_file`, `write_file`, `list_dir`, `sysinfo`
