# Virtual Serial Lab

## Setup

Terminal 1:

```bash
./scripts/create_virtual_serial.sh
```

Creates PTY links:

- Controller: `/tmp/dart-controller`
- Simulator: `/tmp/dart-pump`

Terminal 2:

```bash
uv run intelipump-simulator-serial --port /tmp/dart-pump --log-frames
```

Terminal 3:

```bash
uv run intelipump-controller \
  --port /tmp/dart-controller \
  --addresses 1,2 \
  --log-frames
```

Omit `--duration` to run continuously until Ctrl+C / SIGTERM. For timed lab
tests use `--duration 30`. Do not pass `--duration 0` (rejected; omit instead).

## Pytest marker

```bash
uv run pytest -m serial_integration
```

Skips cleanly when `socat` or PTYs are unavailable.

## PTY vs RS-485

Successful PTY tests prove framing, polling, and session logic. They do **not**
prove electrical compatibility, half-duplex turnaround, ground bias, or driver
enable timing on real RS-485.
