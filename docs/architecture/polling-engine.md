# Polling Engine (Phase 6+)

## Algorithm

Round-robin across configured DART addresses:

1. Optional LAB simulator-only outbound DATA (status read / gated commands)
2. Send POLL (optional `tx_delay_ms` on physical serial)
3. Wait up to `response_timeout_ms` (**default 120**) from **write-complete**
   monotonic time, continuing through temporary empty queue reads
4. Process **all** correlated DATA for that address until **EOT** or deadline;
   ACK each CRC-valid DATA using the frame’s sequence nibble
5. Distinguish: valid DATA / recognized short bus response / no response
6. Sleep `inter_poll_delay_ms`, then next address
7. After a full cycle, sleep `idle_sleep_ms` to avoid busy loops

One pump’s failures are isolated; other addresses continue. RX is demultiplexed
into per-wire-address queues (`0x50` / `0x51`) so one consumer never discards
another address’s frames.

## Ownership / timing

- A permanent (background) reader feeds a shared assembler + demux
- Frames carry first-byte / last-byte monotonic capture times
- Stale frames (`first_byte_time < tx_complete`) never confirm a new exchange
- OS `read_timeout_s` stays short (~10 ms); protocol deadline is separate

## Retry / timeout

- No correlated response in the window → missed bus / timeout counters
- Short bus responses (EOT) keep communication **online** without requiring
  application DATA; online ≠ **state synchronized**
- Bounded `max_retries` re-POLLs for that address only
- After `max_consecutive_timeouts`, mark DISCONNECTED and emit `COMMUNICATION_LOST`

## Duplicate / sequence policy

- Identical RX sequence to last accepted DATA → ACK again, do **not** re-apply
  application/state-machine events
- Soft RX sequence (default): ACK CRC-valid DATA by frame seq and resync expected
- Controller TX sequence advances **only** after a matching ACK; retries reuse
  the same sequence
- Configurable `SPEC_F_TO_1` (default) or `OBSERVED_F_TO_0` for wrap after `0xF`

## Application confirmation

Wire `LINK_ACKNOWLEDGED` ≠ `APPLICATION_CONFIRMED`. Optional confirm polls until
the expected DC1 status is observed **after** command TX time. Feature flags
default to poll-and-observe (no automatic authorize/reset/price/publish).
