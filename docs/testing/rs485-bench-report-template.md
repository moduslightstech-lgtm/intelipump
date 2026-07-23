# RS-485 Bench Report Template

Fill after a physical two-adapter run. Until adapters are available, set
`physical_hil_status = NOT_RUN` and do **not** claim PASS.

```markdown
# RS-485 Office Bench Report

- date:
- bench_name:
- physical_hil_status: NOT_RUN | PASS | FAIL | PENDING
- pi_model:
- os:
- python_version:
- controller_version:
- controller adapter identity / by-id path:
- simulator adapter identity / by-id path:
- wiring notes (A-to-A / B-to-B, ground?):
- termination:
- bias:
- serial: 9600 8O1
- duration_s:
- addresses:
- poll_count:
- DATA_count:
- EOT_count:
- ACK_count:
- NAK_count:
- CRC_errors:
- timeouts:
- retries:
- reconnects:
- latency: count/min/max/mean/p50/p95/p99/jitter
- protocol_target_ms: 25
- configured_bench_timeout_ms: 100
- fault results:
- capture file:
- sanitized capture:
- evidence JSON:
- decision:
- unresolved observations:
```

Generate automatically:

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

Without `--physical`, reports keep `physical_hil_status=NOT_RUN`.
