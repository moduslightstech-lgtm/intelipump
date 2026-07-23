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

## Phase 11 - Watchdog MCU
Heartbeat and TX-enable gating.

## Phase 12 - Disabled Wayne electronic-head bench
Receive-only first, then controlled commands.

## Phase 13 - Controlled field pilot
Validation, not initial coding.
