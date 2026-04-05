# procmux — Python

Python implementation of the procmux subprocess multiplexer.

## Requirements

- Python 3.12+
- `pydantic>=2.0`

## Setup

```bash
cd py
pip install -e .
```

## Server

```bash
python -m procmux /path/to/socket.sock
```

Set `LOG_LEVEL=DEBUG` for verbose logging. Logs go to stderr.

Per-process stdio is logged to rotating files in a `logs/` directory next to the socket. Override with `PROCMUX_STDIO_LOG_DIR`.

## Client

```python
import asyncio
from procmux import ProcmuxConnection, ensure_running

async def main():
    # Connect (starts server if needed)
    conn = await ensure_running("/path/to/socket.sock")

    # Spawn a process
    queue = conn.register_process("worker-1")
    result = await conn.send_command(
        "spawn", name="worker-1", cli_args=["python", "run.py"]
    )

    # Subscribe to output (replays any buffered messages)
    await conn.send_command("subscribe", name="worker-1")

    # Send JSON to stdin
    await conn.send_stdin("worker-1", {"type": "message", "text": "hello"})

    # Read output from the per-process queue
    msg = await queue.get()  # StdoutMsg | StderrMsg | ExitMsg | None

    # Kill
    await conn.send_command("kill", name="worker-1")
    conn.unregister_process("worker-1")

    await conn.close()

asyncio.run(main())
```

See [examples/python/basic.py](../examples/python/basic.py) for a runnable example.

## API

### Client

| Export | Description |
|---|---|
| `ProcmuxConnection` | Async Unix socket client with message demux |
| `connect(socket_path)` | Connect to existing server (returns `None` if unavailable) |
| `ensure_running(socket_path)` | Connect or start server, with retry and timeout |
| `start(socket_path)` | Start server as subprocess |

### Server

| Export | Description |
|---|---|
| `ProcmuxServer` | Server that manages subprocesses |
| `ManagedProcess` | Dataclass tracking a subprocess (status, buffer, idle state) |

### Protocol

`CmdMsg`, `StdinMsg`, `ResultMsg`, `StdoutMsg`, `StderrMsg`, `ExitMsg`
