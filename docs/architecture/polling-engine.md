# Polling Engine (Phase 6)

## Algorithm

Round-robin across configured DART addresses:

1. Optional LAB simulator-only outbound DATA (status read)
2. Send POLL
3. Wait up to `response_timeout_ms` (default 25) for one assembled frame
4. Handle EOT / DATA / NAK / timeout
5. On DATA: CRC check → sequence check → decode → state machine → ACK
6. Sleep `inter_poll_delay_ms`, then next address
7. After a full cycle, sleep `idle_sleep_ms` to avoid busy loops

One pump’s failures are isolated; other addresses continue.

## Retry / timeout

- Timeout increments per-pump counters
- Bounded `max_retries` re-POLLs for that address only
- After `max_consecutive_timeouts`, mark DISCONNECTED and emit `COMMUNICATION_LOST`

## Duplicate policy

Identical RX sequence to last accepted DATA → ACK again, do **not** re-apply
application/state-machine events.

## Sequence policy

Configurable `SPEC_F_TO_1` (default) or `OBSERVED_F_TO_0` for wrap after `0xF`.
