# USB-RS485 Adapters (Phase 10)

## Role

Phase 10 uses **two isolated USB-RS485 adapters** on a Raspberry Pi (or lab host):

| Adapter | Role |
| --- | --- |
| A | Controller runtime |
| B | Simulator serial bridge |

Adapters talk **only to each other**. No Wayne dispenser, no GPIO TX-enable, no watchdog MCU.

## Identity

Prefer stable Linux paths:

```text
/dev/serial/by-id/...
```

Do not rely solely on `/dev/ttyUSB0` numbering.

Discover:

```bash
uv run intelipump-rs485-bench list-adapters --json
```

## Serial settings (required)

| Setting | Value |
| --- | --- |
| Baud | 9600 |
| Data bits | 8 |
| Parity | Odd |
| Stop bits | 1 |
| Flow control | Off |

Odd parity failures are fatal. The stack **never** silently falls back to 8N1.

## Automatic direction control

Most USB-RS485 dongles use automatic DE/RE direction control. Record the adapter
brand/model and whether auto-direction is used (`automatic_direction_control=true`
in bench config).

## Explicit failures

The bench maps and surfaces:

- port not found
- permission denied
- port busy
- unsupported odd parity
- write timeout
- stale file descriptor / disconnect during I/O
- two configured paths resolving to the same physical adapter
