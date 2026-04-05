# procmux

Dumb process multiplexer. Manages named subprocesses over a Unix socket. Buffers output when the client is disconnected. Zero semantic awareness — no knowledge of any higher-level protocol, framework, or application layer.

Both **Python** and **Rust** implementations are provided, sharing the same wire protocol and feature set.

## Architecture

```
Client (Python asyncio / Rust tokio)
    |
Unix socket (JSON lines)
    |
procmux server
    |--- subprocess 1 (stdin/stdout/stderr pipes)
    |--- subprocess 2
    |--- subprocess N
```

Single client at a time. When the client disconnects, output is buffered. On reconnect and subscribe, buffered messages replay synchronously.

## Implementations

| | Python | Rust |
|---|---|---|
| **Server** | `python -m procmux <socket>` | `cargo run -- <socket>` |
| **Client lib** | `from procmux import ProcmuxConnection` | `use procmux::ProcmuxConnection` |
| **Details** | [py/README.md](py/README.md) | [rs/README.md](rs/README.md) |
| **Examples** | [examples/python/](examples/python/) | [examples/rust/](examples/rust/) |

## Wire Protocol

JSON lines over Unix socket. Both implementations use the same message format.

Client sends `CmdMsg` or `StdinMsg`, server replies with `ResultMsg`, `StdoutMsg`, `StderrMsg`, or `ExitMsg`.

### Client → Server

| Message | Fields | Description |
|---|---|---|
| `CmdMsg` | `cmd`, `name`, `cli_args`, `env`, `cwd`, `env_inherit` | spawn, kill, interrupt, subscribe, unsubscribe, list, status |
| `StdinMsg` | `name`, `data` (dict) | Forward JSON to process stdin |

### Server → Client

| Message | Fields | Description |
|---|---|---|
| `ResultMsg` | `ok`, `error`, `pid`, `already_running`, `replayed`, `status`, `exit_code`, `idle`, `processes`, `uptime_seconds` | Command response |
| `StdoutMsg` | `name`, `data` (dict) | JSON from process stdout |
| `StderrMsg` | `name`, `text` | Raw stderr line |
| `ExitMsg` | `name`, `code` | Process exited |

### Commands

| Command | Description |
|---|---|
| `spawn` | Start a new subprocess. Fields: `name`, `cli_args`, `env` (overrides), `cwd`, `env_inherit` (default: true) |
| `kill` | Terminate a process (SIGTERM → SIGKILL escalation) |
| `interrupt` | Send SIGINT to a process group |
| `subscribe` | Start receiving output for a process (replays buffered messages) |
| `unsubscribe` | Stop receiving output (messages are buffered) |
| `list` | List all managed processes with status |
| `status` | Server uptime |

## Features

- Per-process output buffering during client disconnection
- Per-process stdio logging (rotating files)
- Idle detection via stdin/stdout timestamp comparison
- Graceful shutdown on SIGTERM/SIGINT (terminates all subprocesses)
- Process group isolation (new sessions for signal handling)
- Environment inheritance control (`env_inherit` flag)
- 10 MB socket buffer limit

## License

[MIT](LICENSE)
