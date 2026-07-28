# Step-by-Step Implementation Plan

## Phase 0 - Foundation
uv project, typed configuration, structured logging, FastAPI health endpoint, tests, safety enforcement.

## Phase 1 - Pure DART line utilities
CRC, escaping, control-byte classification, framing, parsing, sequence helpers, BCD. No serial I/O.

## Phase 2 - Captured-frame validation
Prove POLL/EOT/ACK decoding, DATA CRC, escaping, address, and sequence extraction.

## Phase 3 - Application layer
Typed commands and responses. Read-only decoding first.

## Phase 4 - Pump state machine
Transitions, guards, command eligibility, restart reconciliation.

## Phase 5 - Virtual pump simulator
Polling, EOT, DATA, ACK/NAK, nozzle lift, price, authorize, filling, stop, totals, restart.

## Phase 6 - Serial transport and polling
Per-pump sessions, retries, timeouts, and non-blocking polling.

## Phase 7 - Persistence
SQLite WAL, transactions, snapshots, audit, command attempts, sync queue.

## Phase 8 - Local API and live events
Status, transactions, WebSocket/SSE. Active endpoints disabled by default.

## Phase 9 - Cloud integration
LAB-only IDs and topics; simulated records clearly marked.

## Phase 10 - Physical RS-485 bench
Two isolated USB-RS485 adapters.

## Phase 11 - Watchdog and deployment hardening
11A internal liveness; 11B systemd sd_notify watchdog; 11C serial/pump
communication health (implemented). Later 11D–11H: backup, host monitoring,
health CLI, hardware watchdog prep. See `docs/phase-11-deployment-hardening.md`
and `docs/phase-11c-wayne-passive-lab-test.md`.

## Passive Wayne lab capture (between Phase 11C and Phase 12)
Receive-only observation tooling and runbook for the privately owned lab
dispenser. See `docs/wayne-passive-lab-runbook.md`. No authorization.
Does not start Phase 11D–11H.

## Real-pump poll bench (POLL_ONLY_BENCH)
Bounded verified status poll via `intelipump-poll-bench` against the owned
lab dispenser. See `docs/poll-bench-real-wayne.md`. No authorization.
Does not start Phase 11D–11H.

## Continuous poll bench (CONTINUOUS_POLL_BENCH)
Bounded status-only polling via `intelipump-continuous-poll-bench`
(one address; real-Wayne short max 5s / 50 writes; optional extended
watch max 300s with `--confirm-extended-watch`, Ctrl+C stop; optional
gated CD1 RETURN_STATUS cadence with `--confirm-return-status-cadence`,
still no RESET/AUTHORIZE). See `docs/continuous-poll-bench.md`.
Not production polling. Does not start Phase 11D–11H.

## Captured ePump / Wayne iGEM wire-address profile
Logical sides 1/2 map to wire ADR `0x50`/`0x51`; status polls are
`50 20 FA` / `51 20 FA`. See `docs/protocol-notes/dart-line-summary.md`.
No authorization or non-poll replay.

## Phase 12 - Disabled Wayne electronic-head bench
Receive-only first, then controlled commands.

## Phase 13 - Controlled field pilot
Validation, not initial coding.
