# Wayne Main Controller — Minimum Transplantable Fix Plan

**Branch:** `v3`  
**Companion:** [`wayne-working-vs-main-gap-analysis.md`](wayne-working-vs-main-gap-analysis.md)  
**Rule:** Extract **proven protocol / session behavior** from `wayne_dart_controller/` only. Preserve main controller service structure, logging, config, MQTT/events, persistence, DI, tests, and packaging. **Do not** copy sale anti-patterns (see § Anti-patterns).  
**This document is a plan only — no production implementation in this change set.**

---

## Guiding principles

1. Prefer adapting patterns already proven in `bench_poll/` and `real_wayne_price/session_helpers.py` into `ControllerLoop` / transport, rather than importing the test controller as a second stack.
2. Keep application, line protocol, and transport independent (workspace architecture rule).
3. Default mode remains **LISTEN_ONLY**; active commands stay behind safety / feature flags.
4. Never equate wire `ACK` with application confirmation.
5. Communication **online/healthy** ≠ **state synchronized**.

---

## Phase 1 — Communication foundation

**Goal:** Reliable, low-latency RX with ownership and dual-address isolation.

| Change | Exact main files to modify | Proven behavior to transplant | Notes |
|---|---|---|---|
| Permanent (or equivalent) serial reader thread; sole `serial.read` caller; chunk capture timestamps | `protocol/dart/transport/serial.py` **or** new helper under `protocol/dart/transport/` / `controller/`; wire into `controller/controller_loop.py` | `wayne_dart_controller/.../serial_transport._rx_worker`; also `bench_poll/serial_reader.py` | Prefer evolving bench reader into shared transport primitive used by ControllerLoop |
| Short OS read timeout; software response deadline separate | `protocol/dart/transport/serial.py` (`SerialConfig.read_timeout_s`); `controller/poll_scheduler.py` (`response_timeout_ms`) | Working: 10 ms read / 120 ms window | Raise default `response_timeout_ms` to **≥100–120** for real Wayne; keep `read_timeout_s` ~10–20 ms |
| Keep `read_serial_chunk` (in_waiting / read(1)); optional cap closer to 64 | `protocol/dart/transport/serial.py` | Working `read(min(waiting, 64))` | Do not revert to blocking `read(256)` |
| Per-address frame/chunk demux (independent queues for wire `0x50`/`0x51`) | `controller/controller_loop.py`; possibly new `controller/rx_demux.py` | `CentralFrameDispatcher` | Logical addresses 1/2 map via existing `addressing.encode_wire_address` |
| Drain or timestamp-gate RX before POLL/DATA TX | `controller/controller_loop.py` (`_write_frame` / `_poll_one` / `_maybe_send_outbound`) | Working stale `first_byte_time < write_time`; lab `drain_pending_rx` | Prefer ownership-by-timestamp (evidence-friendly) over silent OS flush when possible |
| Configurable `tx_delay_ms` / `ack_delay_ms` | `controller/poll_scheduler.py`; apply in `controller_loop._write_frame` | Working 35 ms TX / 5 ms ACK | Config defaults; overridable for simulator |

**Exit criteria:** Dual-address poll shows owned responses with first-byte latency stats in the 10–50 ms band without false timeouts under quiet bus.

---

## Phase 2 — Parsing and direction

**Goal:** Controller-owned RX always interprets pump→controller layouts; reject ambiguity on active TX paths.

| Change | Exact main files to modify | Proven behavior | Notes |
|---|---|---|---|
| On poll/session RX DATA, force P2C direction (DC1/DC3) explicitly at session boundary | `controller/pump_session.py` (`_decode_and_apply`); optionally thin wrapper over `application/decoder.py` | `DARTTransactionParser` + `FrameDirection.PUMP_TO_CONTROLLER` | Mapper already passes `resolve_as_dc1/dc3`; make direction non-optional for controller RX |
| Ensure CD1 command bytes are never applied as DC1 status on master TX/echo paths | `controller/pump_session.py`; `state_machine/wayne_mapper.py` if needed | Direction split in working parser | Add regression: CD1 `01 01 00` not DC1 RESET/etc. |
| Keep CRC reject → no decode / no ACK | already in `pump_session._on_data` | Working CRC candidate fail | No change beyond tests |
| Keep NOZIO masks `0x01` IN / `0x11` OUT | `application/nozio.py` | Working parser | No change |
| Quiet-gap / partial-frame expire between polls | `controller/controller_loop.py`; `line/legacy_stream.py` (`expire_partial`) | Working quiet gap discard | Log discarded bytes; don’t silent-drop without metrics |

**Exit criteria:** Direction unit tests pass; ambiguous TRANS `0x01` cannot flip state without explicit P2C context.

---

## Phase 3 — Per-address state

**Goal:** Isolate sequence and sync state the way the working master does, without adopting its sale latch.

| Change | Exact main files to modify | Proven behavior | Notes |
|---|---|---|---|
| Confirm TX seq is per-session and advances **only** after matching ACK | `controller/session_models.py`; `controller_loop._maybe_send_outbound` | `PumpState.advance_seq_ctrl` | Already mostly true — harden + document |
| Soften / recover `expected_rx_sequence`: ACK CRC-valid DATA by frame seq; resync expected after accept | `controller/pump_session.py` (`_on_data`) | Working ACKs any DATA seq | Keep duplicate-ACK policy; avoid permanent FAULTED on first mismatch |
| Distinguish `communication_healthy` vs `synchronized` (known DC1 + known NOZIO) | `controller/session_models.py`; `pump_session.py`; optional API fields in `api/` | Working `online` vs `synchronized` | Sync does not auto-authorize |
| Sync loop: poll until synchronized; optional gated CD1 RETURN_STATUS when UNKNOWN | `controller/controller_loop.py` or startup path in `api/lifespan.py` / `controller/cli.py` | `wayne_dart_controller/src/main.py` sync | Behind config; LISTEN_ONLY may still sync observe-only |
| Offline / missed-poll thresholds remain per address | already `PumpSession.on_timeout` | Working consecutive missed polls | Align naming/docs with “online vs synchronized” |

**Exit criteria:** After restart, both addresses reach synchronized independently; seq desync recovers without manual restart.

---

## Phase 4 — Poll and ACK session behavior

**Goal:** Match working poll→response→ACK→EOT session semantics.

| Change | Exact main files to modify | Proven behavior | Notes |
|---|---|---|---|
| After POLL, wait for **deadline**, continuing through empty reads | `controller_loop._read_one_frame` → replace/extend with `_read_poll_session` | `WayneDartMaster.poll_pump` | Empty ≠ timeout |
| Accept **multiple** long DATA frames per poll; ACK each; stop on EOT or deadline | `controller/controller_loop.py`; `pump_session.handle_response_frame` | Working multi-frame loop | Update polling-engine.md |
| Stale frames (`capture < tx_complete`) never terminate the wait as “success” | new session reader helper | Working stale continue | Metrics: `stale_frame_count` |
| ACK address + sequence must match the DATA being acknowledged | already `build_ack(wire, seq)` | Working `0xC0 \| nibble` | Add tests |
| Outbound DATA: retries reuse **same** sequence; advance only after matching ACK with `not_before` | `controller_loop._maybe_send_outbound`; `controller/outbound.py` | `send_transaction` retries | Honor idempotency class / safety |
| Stale ACK must not confirm a new command | same + pattern from `real_wayne_price/session_helpers.wait_for_ack_frame` | Working + lab | Critical for RESET/AUTHORIZE |

**Exit criteria:** Captured dual-pump poll sessions show DATA→ACK→(DATA→ACK)*→EOT without orphan DATA.

---

## Phase 5 — Application confirmation

**Goal:** Separate link ACK from application-level success.

| Change | Exact main files to modify | Proven behavior | Notes |
|---|---|---|---|
| Introduce result distinction: link ACK’d vs application confirmed | `controller/session_models.py` or small `controller/exchange_result.py`; used by outbound / future command services | `ExchangeResult` | Do not rename MQTT schemas casually — map internally |
| After RESET link ACK, poll until DC1 RESET observed **after** command TX time (bounded) | new method on `PumpSession` or command service; caller in gated command path | `WayneDartMaster.reset(confirm_application=True)` | Feature-flag; physical enable required |
| After AUTHORIZE link ACK, poll until DC1 AUTHORIZED after command TX time | same | `authorize(confirm_application=True)` | Same gates |
| Never report APPLICATION_CONFIRMED on link ACK alone | API / events / persistence bridge consumers | Working soft-fallback returns LINK only | Explicit event fields |

**Exit criteria:** Lab RESET/AUTHORIZE evidence shows status change timestamps strictly after command TX.

---

## Phase 6 — Sale lifecycle and event publishing

**Goal:** Publish sales only with valid evidence; keep main SM/persistence strengths.

| Change | Exact main files to modify | Proven behavior to **avoid** / correct rule | Notes |
|---|---|---|---|
| Enforce valid-sale evidence before finalize / cloud publish | `services/persistence_bridge.py`; `services/transaction_service.py`; `state_machine/` as needed | **Do not** copy working sale latch | Evidence: auth confirmed + FILLING seen + DC2 vol&amt increased + FILLING_COMPLETED + not ABORTED |
| Zero-volume lift-return → `ABORTED_NO_DELIVERY` (or equivalent), not completed sale | `state_machine/wayne_mapper.py` / transitions; persistence bridge | Working mislabel | New event/state name if needed |
| FILLING_COMPLETED without prior FILLING → no sale | same | Working may complete from IDLE/abort paths | Guard |
| Do not auto-reset / auto-authorize / auto-price at startup unless flags | `controller/` + safety config | Working `auto_startup_commands` | Default **off** |
| Skip RESET if already RESET and application-confirmed | command path | Working may re-RESET | Low risk optimization |

**Exit criteria:** Simulated/lab zero-delivery return never creates a paid transaction record.

---

## Anti-patterns — must NOT transplant

Migration **must not** copy:

1. Treating every `FILLING_COMPLETED` as a real sale  
2. Recording a sale when volume/amount are zero  
3. Promoting `ABORTED` → `COMPLETED`  
4. Issuing RESET when already RESET (unnecessary)  
5. Treating `LINK_ACKNOWLEDGED` as `APPLICATION_CONFIRMED`  
6. Auto authorize without an explicit feature flag (+ physical enable)  
7. Auto price change at startup without an explicit feature flag  

**Valid sale evidence requires all of:**

- Authorization application-confirmed  
- FILLING observed in the lifecycle  
- DC2 volume and amount increased  
- FILLING_COMPLETED observed  
- Not already ABORTED / aborted-no-delivery  

---

## Exact file list (union of planned production touches)

Primary (expected to change in later approved implementation):

- `src/intelipump_fdc/protocol/dart/transport/serial.py`
- `src/intelipump_fdc/protocol/dart/line/legacy_stream.py` (partial expire / metrics only if needed)
- `src/intelipump_fdc/controller/poll_scheduler.py`
- `src/intelipump_fdc/controller/controller_loop.py`
- `src/intelipump_fdc/controller/pump_session.py`
- `src/intelipump_fdc/controller/session_models.py`
- `src/intelipump_fdc/controller/outbound.py`
- `src/intelipump_fdc/controller/cli.py` (timeout/delay flags wiring)
- Possibly new: `src/intelipump_fdc/controller/rx_demux.py` and/or `src/intelipump_fdc/controller/exchange_result.py`
- `src/intelipump_fdc/state_machine/wayne_mapper.py` (sale/abort guards only as needed)
- `src/intelipump_fdc/services/persistence_bridge.py`
- `src/intelipump_fdc/services/transaction_service.py`
- `docs/architecture/polling-engine.md` (behavior doc update when implementing)

Reuse-as-reference (prefer extract shared helpers; avoid duplicating forever):

- `src/intelipump_fdc/bench_poll/serial_reader.py`
- `src/intelipump_fdc/bench_poll/poll_io.py`
- `src/intelipump_fdc/real_wayne_price/session_helpers.py`

Config / safety (flag defaults only when implementing actives):

- `src/intelipump_fdc/controller/safety.py`
- `src/intelipump_fdc/core/config.py` (if new settings keys)

**Out of scope for transplant:** rewriting `wayne_dart_controller/`; modifying capture evidence; enabling actives by default.

---

## Required tests (list only — do not implement production behavior here)

Add (or extend) tests covering:

1. **0x50 / 0x51 isolation** — frame for one address never applied to the other session  
2. **Fragmented frame reconstruction** — DATA split across multiple reads assembles once  
3. **Multiple frames in one read** — short + DATA + EOT in one chunk handled correctly  
4. **CRC rejection** — bad CRC → no state mutation, no ACK  
5. **CD1 not as DC1** — controller command payload `01 01 xx` not mapped as pump status when direction is C2P / ambiguous without P2C resolve  
6. **NOZIO `0x01` IN / `0x11` OUT** — edge events correct  
7. **Poll wait continues through temporary empty queue** — Empty/timeout slice ≠ end of 120 ms window  
8. **ACK address + sequence matching** — ACK bytes match DATA wire ADR + seq nibble  
9. **Retries reuse sequence** — failed attempt does not advance TX seq  
10. **Sequence advances only after matching ACK**  
11. **Stale ACK does not confirm new command** — pre-TX ACK ignored via `not_before`  
12. **RESET confirmation after command** — APPLICATION_CONFIRMED only when DC1 RESET after TX time  
13. **AUTHORIZED confirmation after command** — same for AUTHORIZED  
14. **Zero-volume lift-return → ABORTED_NO_DELIVERY** — no sale record  
15. **FILLING_COMPLETED without FILLING → no sale**  

Suggested homes (when implementing): `tests/` under controller/protocol/state_machine/services mirroring existing layout; reuse patterns from `wayne_dart_controller/tests/test_crc_and_codecs.py` for CRC/direction/NOZIO cases without depending on that package at runtime.

---

## Suggested implementation order (after approval)

1. Phase 1 (foundation) + tests 1–3, 7  
2. Phase 4 (poll/ACK session) + tests 4, 8–11  
3. Phase 2–3 (direction + per-address sync/seq) + tests 5–6  
4. Phase 5 (application confirm) + tests 12–13 — still gated LISTEN_ONLY / flags  
5. Phase 6 (sale evidence) + tests 14–15  

Stop after each phase for review per `docs/development/implementation-plan.md` discipline.

---

## Approval checkpoint

**No production code is changed by this documentation task.** Proceeding to implement any phase requires explicit approval.
