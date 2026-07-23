# RS-485 Office Bench (Phase 10)

## Topology

```
Raspberry Pi 5
  ├── USB-RS485 Adapter A  → controller runtime
  └── USB-RS485 Adapter B  → simulator serial bridge

A ↔ B only (office LAB). No Wayne dispenser. No GPIO TX-enable. No watchdog MCU.
```

See also: [rs485-adapters.md](../hardware/rs485-adapters.md),
[rs485-bench-wiring.md](../hardware/rs485-bench-wiring.md),
[rs485-bench-report-template.md](rs485-bench-report-template.md).

## Safety

- Environment: `LAB`
- Mode: `LISTEN_ONLY`
- Active commands remain disabled
- Prefer `/dev/serial/by-id/...` stable paths (Linux)
- Never silently fall back from odd parity to 8N1
- Without physical adapters: `physical_hil_status = NOT_RUN` (not PASS)

## Discover adapters

```bash
uv run intelipump-rs485-bench list-adapters --json
```

## Validate 9600 8O1

```bash
uv run intelipump-rs485-bench validate --port /dev/serial/by-id/<adapter>
```

## Run HIL (physical readiness command)

```bash
uv run intelipump-rs485-bench run \
  --controller-port /dev/serial/by-id/<controller-adapter> \
  --simulator-port /dev/serial/by-id/<simulator-adapter> \
  --addresses 1,2 \
  --duration 300 \
  --baud 9600 \
  --response-timeout-ms 100 \
  --log-frames \
  --evidence data/bench/rs485-bench-001.jsonl \
  --report data/bench/rs485-bench-001.md \
  --physical
```

Outputs:

- JSONL raw capture (+ sanitized copy)
- JSON evidence summary
- Markdown report

`protocol_target_ms` stays **25**. `response-timeout-ms` is the configured bench
timeout (commonly **100**) and is never used to redefine the protocol target.

## Pytest marker

Consistent marker: **`rs485_bench`**

```bash
uv run pytest -m rs485_bench
```

Skipped cleanly when two physical adapters are not configured via
`INTELIPUMP_BENCH__CONTROLLER_PORT` / `INTELIPUMP_BENCH__SIMULATOR_PORT`.

## systemd (LAB example only)

See `deployment/systemd/intelipump-rs485-bench.service`. Do **not** install or
enable automatically. Uses a non-root user, LAB env file, and by-id paths.

## Fault injection

Software-only (never electrical): delayed/dropped response, CRC corruption,
duplicate DATA, wrong sequence, NAK, partial frame, noise, DLE/SF split,
simulator restart, one/all addresses offline.
