//! Basic procmux example: spawn a process, send stdin, read stdout, kill it.
//!
//! Demonstrates the core procmux workflow using the Rust client:
//!   1. Start the procmux server in the background
//!   2. Connect a client
//!   3. Spawn a subprocess that echoes JSON lines
//!   4. Subscribe to its output
//!   5. Send data via stdin and read the echoed response
//!   6. Kill the process and clean up
//!
//! Usage:
//!     cargo run --example basic

use std::collections::HashMap;
use std::time::Duration;

use procmux::client::{ProcessMsg, ProcmuxConnection};
use procmux::ProcmuxServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging (set LOG_LEVEL=debug for verbose output)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let socket_path = "/tmp/procmux-example-rs.sock";

    // Clean up any stale socket
    let _ = std::fs::remove_file(socket_path);

    // 1. Start the server in the background
    println!("Starting procmux server at {socket_path}...");
    let server = ProcmuxServer::new(socket_path);
    let server_handle = tokio::spawn(async move {
        server.run().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 2. Connect a client
    println!("Connecting client...");
    let conn = ProcmuxConnection::connect(socket_path).await?;
    println!("Connected!");

    // 3. Register a process queue and spawn a bash echo loop
    let mut rx = conn.register_process("echo-worker").await;
    let result = conn
        .send_command(
            "spawn",
            "echo-worker",
            vec![
                "bash".to_string(),
                "-c".to_string(),
                "while IFS= read -r line; do echo \"$line\"; done".to_string(),
            ],
            HashMap::new(),
            None,
        )
        .await?;
    println!("Spawned 'echo-worker' (pid={:?})", result.pid);

    // 4. Subscribe to output
    let sub = conn.send_simple_command("subscribe", "echo-worker").await?;
    println!("Subscribed (replayed={:?} buffered messages)", sub.replayed);

    // 5. Send a JSON object via stdin
    conn.send_stdin(
        "echo-worker",
        serde_json::json!({"greeting": "hello", "from": "procmux"}),
    )
    .await?;
    println!("Sent stdin message");

    // 6. Read the echoed output
    let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("process queue closed"))?;
    match &msg {
        ProcessMsg::Stdout(stdout) => {
            println!("Received: Stdout -> {}", serde_json::to_string_pretty(&stdout.data)?);
        }
        ProcessMsg::Stderr(stderr) => {
            println!("Received: Stderr -> {}", stderr.text);
        }
        ProcessMsg::Exit(exit) => {
            println!("Received: Exit (code={:?})", exit.code);
        }
        ProcessMsg::ConnectionLost => {
            println!("Connection lost!");
        }
    }

    // 7. Check server status
    let status = conn.send_simple_command("status", "").await?;
    println!("Server uptime: {:?}s", status.uptime_seconds);

    // 8. List all managed processes
    let proc_list = conn.send_simple_command("list", "").await?;
    println!("Managed processes: {:?}", proc_list.processes);

    // 9. Kill the process and clean up
    conn.send_simple_command("kill", "echo-worker").await?;
    println!("Killed 'echo-worker'");

    conn.close();
    server_handle.abort();
    let _ = std::fs::remove_file(socket_path);
    println!("Done!");

    Ok(())
}
