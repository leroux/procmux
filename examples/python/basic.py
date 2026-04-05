"""Basic procmux example: spawn a process, send stdin, read stdout, kill it.

Demonstrates the core procmux workflow using the Python client:
  1. Start the procmux server (or connect to an existing one)
  2. Spawn a subprocess that echoes JSON lines
  3. Subscribe to its output
  4. Send data via stdin and read the echoed response
  5. Kill the process and clean up

Usage:
    python examples/python/basic.py
"""

import asyncio
import sys
import os

# Add the Python package to the path (for running from repo root)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "py"))

from procmux import ensure_running


async def main():
    socket_path = "/tmp/procmux-example.sock"

    # 1. Start server (or connect if already running)
    print(f"Connecting to procmux at {socket_path}...")
    conn = await ensure_running(socket_path)
    print("Connected!")

    # 2. Register a process queue and spawn a bash echo loop
    #    The subprocess reads JSON lines from stdin and echoes them back.
    queue = conn.register_process("echo-worker")
    result = await conn.send_command(
        "spawn",
        name="echo-worker",
        cli_args=["bash", "-c", "while IFS= read -r line; do echo \"$line\"; done"],
    )
    print(f"Spawned 'echo-worker' (pid={result.pid})")

    # 3. Subscribe to output (this also replays any buffered messages)
    sub = await conn.send_command("subscribe", name="echo-worker")
    print(f"Subscribed (replayed={sub.replayed} buffered messages)")

    # 4. Send a JSON object via stdin
    await conn.send_stdin("echo-worker", {"greeting": "hello", "from": "procmux"})
    print("Sent stdin message")

    # 5. Read the echoed output
    msg = await asyncio.wait_for(queue.get(), timeout=5.0)
    if msg is None:
        print("Connection lost!")
        return

    print(f"Received: {type(msg).__name__} -> {msg}")

    # 6. Check server status
    status = await conn.send_command("status")
    print(f"Server uptime: {status.uptime_seconds}s")

    # 7. List all managed processes
    proc_list = await conn.send_command("list")
    print(f"Managed processes: {proc_list.processes}")

    # 8. Kill the process and clean up
    await conn.send_command("kill", name="echo-worker")
    conn.unregister_process("echo-worker")
    print("Killed 'echo-worker'")

    await conn.close()
    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
