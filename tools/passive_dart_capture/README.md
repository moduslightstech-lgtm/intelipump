# Passive Wayne DART RS-485 Capture / Analysis

Strictly **LISTEN_ONLY** tool for merged-bus Wayne DART capture and offline analysis.

- Never transmits any byte
- Never calls `serial.write` / asserts RTS or RS-485 DE
- Never imports active controller command paths (RESET / AUTHORIZE / price write)
- Does not guess undocumented command meanings
- Does **not** auto-run against hardware — a subcommand is required (`dry-run`, `capture`, …)

See [REQUIREMENTS.md](./REQUIREMENTS.md) for reuse boundaries and banned patterns.

## Workspace

Implemented under:

`/Users/babatundealaraje/Documents/moduslights/intelipump-fdc/tools/passive_dart_capture/`

## Defaults

| Setting | Value |
|---------|-------|
| Device | `/dev/ttyUSB0` |
| Baud / framing | 9600, 8 data bits, **odd** parity, 1 stop bit |
| Read timeout | 0.05 s |
| Evidence dir | `tools/passive_dart_capture/evidence/<sessionId>.jsonl` |
| Reports dir | `tools/passive_dart_capture/reports/` |
| Chunk direction | `MERGED_BUS` |
| Frame direction | `UNKNOWN` or `INFERRED` (with reason; never claimed without reason) |
| Source tag | `EPUMP_PASSIVE_CAPTURE` |

## Serial open limitations

pyserial typically opens the device **read/write at the OS level**. This tool never calls write/RTS/DE, so behavior is RX-only in software — but **physical TX inhibit is not proven in software**. For hardware capture, use a listen-only / RX-only RS-485 adapter (or disconnect TX / tie DE inactive). Prefer stopping other controllers on the bus before capturing.

## Usage

From the repo root (venv activated, `src` on `PYTHONPATH` via project install or `uv run`):

```bash
# Hardware-free pipeline check (fixture → evidence → reports under a temp dir)
python -m tools.passive_dart_capture dry-run
python -m tools.passive_dart_capture dry-run --keep-temp
python -m tools.passive_dart_capture dry-run --work-dir /tmp/pdc-dry --session-id demo

# Passive capture — stop with Ctrl+C / SIGINT (does NOT auto-run; operator must start)
python -m tools.passive_dart_capture capture --device /dev/ttyUSB0

# Optional session id / evidence dir
python -m tools.passive_dart_capture capture --session-id lab-001 --evidence-dir ./tools/passive_dart_capture/evidence

# Operator markers (append to same JSONL; works while capture is running)
python -m tools.passive_dart_capture marker lab-001 NOZZLE_LIFTED --note "hose A"
python -m tools.passive_dart_capture marker lab-001 END_CAPTURE

# Offline analysis
python -m tools.passive_dart_capture analyze lab-001

# Compare two sessions
python -m tools.passive_dart_capture compare lab-001 lab-002
```

### Dry-run

`dry-run` feeds `tests/fixtures/merged_bus_sample.hex` (polls, ACKs, DATA, fragmented poll, CRC-invalid, trailing partial) through the real capture/assembler/parser/writer/analyzer path. It **never** opens `/dev/ttyUSB0` or any serial device.

### Markers

`STARTUP`, `EPUMP_CONNECTED`, `NOZZLE_LIFTED`, `NOZZLE_RETURNED`, `DISPLAY_CHANGED`, `RESET_OBSERVED`, `AUTHORIZED_OBSERVED`, `FILLING_OBSERVED`, `TRANSACTION_COMPLETED`, `END_CAPTURE`

### Analyzer outputs

Under `tools/passive_dart_capture/reports/`:

- `<sessionId>-timeline.csv` / `.md`
- `<sessionId>-status-transitions.csv`
- `<sessionId>-nozio-transitions.csv`
- `<sessionId>-unknown-transactions.csv`
- `<sessionId>-summary.json`

Analytics use **complete frame records only** (not `serial_chunk`), deduped by `sessionId+frameSequence`.

## NOZIO decode (capture analysis)

Documented-mask labeling local to this tool’s parser:

- `logicalNozzle = raw & 0x0F`
- `nozzlePosition = OUT` if `raw & 0x10` else `IN`
- `reservedBits = raw & 0xE0` (warn if nonzero)

This is offline evidence labeling, separate from production state-machine nozzle UNKNOWN profile work.

## Tests

```bash
PYTHONPATH=src:. uv run pytest tools/passive_dart_capture/tests -q
```

## Safety confirmation

This package has **no transmit code path**: no `serial.write`, no RTS/DE assert, no command replay, no imports of `real_wayne_price` write/reset/authorize sessions or `SerialTransport`. Enforced by `tests/test_static_safety.py`.
