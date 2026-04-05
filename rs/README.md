# procmux — Rust

Rust implementation of the procmux subprocess multiplexer.

## Requirements

- Rust 2024 edition (1.85+)

## Build

```bash
cargo build --release
```

## Server

```bash
cargo run -- /path/to/socket.sock
```

Set `LOG_LEVEL=debug` for verbose logging. Logs go to stderr.

Per-process stdio is logged to rotating files in a `logs/` directory next to the socket. Override with `PROCMUX_STDIO_LOG_DIR`.

## Client

```rust
use std::collections::HashMap;
use procmux::ProcmuxConnection;
use procmux::client::ProcessMsg;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn = ProcmuxConnection::connect("/path/to/socket.sock").await?;

    // Spawn a process
    let mut rx = conn.register_process("worker-1").await;
    conn.send_command(
        "spawn",
        "worker-1",
        vec!["python".into(), "run.py".into()],
        HashMap::new(),
        None,
    ).await?;

    // Subscribe to output
    conn.send_simple_command("subscribe", "worker-1").await?;

    // Send JSON to stdin
    conn.send_stdin("worker-1", serde_json::json!({"text": "hello"})).await?;

    // Read output
    if let Some(msg) = rx.recv().await {
        match msg {
            ProcessMsg::Stdout(s) => println!("stdout: {}", s.data),
            ProcessMsg::Stderr(s) => println!("stderr: {}", s.text),
            ProcessMsg::Exit(e) => println!("exited: {:?}", e.code),
            ProcessMsg::ConnectionLost => println!("connection lost"),
        }
    }

    // Kill and clean up
    conn.send_simple_command("kill", "worker-1").await?;
    conn.close();
    Ok(())
}
```

See [examples/rust/basic.rs](../examples/rust/basic.rs) for a runnable example (`cargo run --example basic`).

## API

### Client (`procmux::client`)

| Export | Description |
|---|---|
| `ProcmuxConnection` | Async Unix socket client with message demux |
| `ProcessMsg` | Enum: `Stdout`, `Stderr`, `Exit`, `ConnectionLost` |

### Server (`procmux::server`)

| Export | Description |
|---|---|
| `ProcmuxServer` | Server that manages subprocesses |

### Protocol (`procmux::protocol`)

`ClientMsg`, `ServerMsg`, `CmdMsg`, `StdinMsg`, `ResultMsg`, `StdoutMsg`, `StderrMsg`, `ExitMsg`

## Dependencies

- `tokio` — async runtime
- `serde` / `serde_json` — serialization
- `tracing` — structured logging
- `nix` — Unix signal/process handling
- `chrono` — timestamps
- `anyhow` / `thiserror` — error handling
