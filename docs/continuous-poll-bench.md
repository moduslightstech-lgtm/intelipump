# Continuous poll bench (CONTINUOUS_POLL_BENCH)

**Tool:** `intelipump-continuous-poll-bench`

Bounded **status-only** DART polls against **exactly one** confirmed pump
address. Default is a short session; optional extended POLL-only watch
(up to 300 s, Ctrl+C to stop early) for catching nozzle / DC3 transitions.
Intended to help determine whether regular status polling prevents
Wayne iGEM POS Communication Lost / Error 30.

Logical `--address 1` transmits captured `50 20 FA`; `--address 2` transmits
`51 20 FA`. Evidence includes `logicalAddress` and `wireAddress`.

Every poll cycle uses the shared
`send_status_poll_and_read_response()` path also used by
`intelipump-poll-bench` (one writer/reader, TX flush, no second drain path
during the response window).

`SHORT_CONTROL_70` (`50 70 FA`) is an interim control response: it is logged and
preserved, but the cycle continues within the bounded response window until a
`DATA_FRAME`, disconnect, or deadline. Status-data PASS requires
`dataResponses > 0` (not merely control frames).
`validResponses` is an alias of `protocolFramesReceived` (any recognized frame).

This is **not** production polling. No authorization, transactions, presets,
price changes, resets, MQTT commands, or daemon mode.

## Environment

```bash
export INTELIPUMP_ENVIRONMENT=LAB
export INTELIPUMP_CONTROLLER__MODE=CONTINUOUS_POLL_BENCH
export INTELIPUMP_MQTT__ENABLED=false
# active commands / remote auth / automatic auth / command replay must stay off
```

Stop the controller service before any serial bench:

```bash
sudo systemctl stop intelipump.service
```

## Simulator validation (LAB)

Use this path first. Do **not** point this at the real Wayne until a later
approved lab run.

1. Start the serial simulator on `/dev/intelipump-simulator` only.
2. Confirm `/dev/intelipump-controller` is free.
3. Run:

```bash
intelipump-continuous-poll-bench \
  --port /dev/intelipump-controller \
  --address 1 \
  --duration-seconds 3 \
  --poll-interval-ms 300 \
  --response-timeout-ms 250 \
  --evidence-dir data/bench/continuous-poll \
  --simulator-validation \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-emergency-isolation-ready \
  --confirm-no-fuel-test \
  --confirm-authorization-disabled \
  --confirm-status-poll-only \
  --confirm-bounded-duration
```

Evidence: `continuous-poll-bench-*.jsonl` and `.md` under `--evidence-dir`.

Results:

- `CONTINUOUS_POLL_BENCH_PASS` — ≥1 valid response, no safety/protocol faults
- `CONTINUOUS_POLL_BENCH_INCONCLUSIVE` — only timeouts, no unsafe condition
- `CONTINUOUS_POLL_BENCH_FAIL` — CRC/protocol/unexpected/serial/ownership/safety

## Hard limits

| Limit | Real Wayne (short) | Real Wayne (`--confirm-extended-watch`) | Simulator short | Simulator extended |
| --- | --- | --- | --- | --- |
| Duration | 1–5 s (default 3) | 1–300 s | 1–30 s | 1–300 s |
| Poll interval | 300–1000 ms (default 300) | same | 50–1000 ms | same |
| Response timeout | default 250 ms; must be &lt; interval | same | same | same |
| Max writes | 50 | derived, cap 2000 | derived (cap 600) | derived (cap 2000) |
| Addresses | exactly one | exactly one | exactly one | exactly one |

Scheduler is monotonic: missed deadlines skip forward (`schedule_lag_ms`);
never catch up with back-to-back polls. Timeouts do not extend duration.
SIGINT / SIGTERM stops cleanly (`stopReason=operator_interrupt`).

Still **no RESET / AUTHORIZE / price / daemon** in any path. Option 2 may
send gated CD1 RETURN_STATUS only when explicitly confirmed.

## Extended POLL-only watch (lab)

Use when you need longer observation (e.g. lift nozzle and watch DC3 / NOZIO)
without raising the short-path default:

```bash
uv run intelipump-continuous-poll-bench \
  --port /dev/intelipump-controller \
  --address 1 \
  --duration-seconds 120 \
  --poll-interval-ms 300 \
  --response-timeout-ms 250 \
  --evidence-dir data/bench/continuous-poll \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-emergency-isolation-ready \
  --confirm-no-fuel-test \
  --confirm-authorization-disabled \
  --confirm-status-poll-only \
  --confirm-bounded-duration \
  --confirm-extended-watch
```

Stop early with Ctrl+C. Omit `--simulator-validation` only for the owned lab
Wayne after the usual stop-service / isolation checks.

## POLL + RETURN_STATUS cadence (option 2, lab)

Closer to ePump: L2 POLL plus gated CD1 RETURN_STATUS (`01 01 00`) every N
polls. Still **no** RESET / AUTHORIZE. Requires extended watch. Do **not**
pass `--confirm-status-poll-only`.

```bash
uv run intelipump-continuous-poll-bench \
  --port /dev/intelipump-controller \
  --address 2 \
  --duration-seconds 120 \
  --poll-interval-ms 300 \
  --response-timeout-ms 250 \
  --return-status-every-n-polls 2 \
  --sequence 0 \
  --ack-timeout-ms 200 \
  --evidence-dir data/bench/continuous-poll \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-emergency-isolation-ready \
  --confirm-no-fuel-test \
  --confirm-authorization-disabled \
  --confirm-bounded-duration \
  --confirm-extended-watch \
  --confirm-return-status-cadence \
  --confirm-no-reset-no-authorize
```

During the run: nozzle in → start → lift/hold → return → Ctrl+C or wait.
Then decode NOZIO from the JSONL (look for `01`→`11` OUT).

## Continuous until Ctrl+C (lab)

Polls forever (no duration / max-writes stop) until SIGINT/SIGTERM. Not a
systemd daemon — technician must stay present and stop with Ctrl+C.

Omit `--confirm-bounded-duration` and `--duration-seconds` (ignored).

```bash
uv run intelipump-continuous-poll-bench \
  --port /dev/intelipump-controller \
  --address 2 \
  --poll-interval-ms 300 \
  --response-timeout-ms 250 \
  --return-status-every-n-polls 2 \
  --sequence 0 \
  --ack-timeout-ms 200 \
  --evidence-dir data/bench/continuous-poll \
  --confirm-owned-lab-pump \
  --confirm-technician-present \
  --confirm-emergency-isolation-ready \
  --confirm-no-fuel-test \
  --confirm-authorization-disabled \
  --confirm-until-ctrl-c \
  --confirm-return-status-cadence \
  --confirm-no-reset-no-authorize
```

## Real Wayne (later approved lab only)

Without `--simulator-validation`:

- refuses any running simulator process
- refuses simulator ownership of adapters
- `targetType=OWNED_LAB_WAYNE`
- short path: max duration 5 s / max 50 writes
- extended watch: max 300 s / writes derived (cap 2000); requires
  `--confirm-extended-watch`

Do not run against the real dispenser until explicitly approved.

## Safety

Normal `intelipump.service` / controller loop refuses to start polling or the
command queue in `CONTINUOUS_POLL_BENCH`. This mode is usable only by
`intelipump-continuous-poll-bench`.
