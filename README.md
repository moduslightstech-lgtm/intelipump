# InteliPump FDC

Raspberry Pi 5 forecourt-controller research project for Wayne DART dispensers.

## Safety defaults

- Default mode is `LISTEN_ONLY`.
- Active dispenser commands are disabled.
- Remote authorization is disabled.
- No command may bypass the local state machine or safety gate.
- Never test active commands on an operational station without controlled bench validation and qualified on-site supervision.

## Requirements

- Python 3.12+
- uv
- Raspberry Pi OS or another Linux distribution
- Optional for virtual serial tests: `socat`

## Quick start

```bash
cd intelipump-fdc
uv sync --dev
cp .env.example .env
uv run intelipump-fdc
```

Health endpoint:

```bash
curl http://127.0.0.1:8000/api/v1/controller/health
```

### RS-485 office bench (Phase 10)

Marker: `rs485_bench` (skips without two configured adapters).

```bash
uv run intelipump-rs485-bench list-adapters --json
uv run intelipump-rs485-bench run \
  --controller-port /dev/serial/by-id/<controller-adapter> \
  --simulator-port /dev/serial/by-id/<simulator-adapter> \
  --addresses 1,2 \
  --duration 300 \
  --baud 9600 \
  --response-timeout-ms 100 \
  --log-frames \
  --evidence data/bench/rs485-bench-001.jsonl \
  --report data/bench/rs485-bench-001.md
uv run pytest -m rs485_bench
```

See `docs/testing/rs485-office-bench.md`. Until physical adapters are available,
reports use `physical_hil_status=NOT_RUN` (not PASS).

Run checks:

```bash
uv run pytest
uv run ruff check .
uv run mypy src
```

Create virtual serial ports:

```bash
./scripts/create_virtual_serial.sh
```

See `docs/development/implementation-plan.md`.


scp -r intelipump@100.84.152.50:/home/intelipump/intelipump/intelipump-fdc/tools/passive_dart_capture/reports/lab-002-* \
  /Users/babatundealaraje/Documents/moduslights/intelipump-fdc/tools/passive_dart_capture/reports
