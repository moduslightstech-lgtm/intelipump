# Wayne Working Test Controller vs Main Intelipump Controller — Gap Analysis

**Branch:** `v3` (current working tree)  
**Scope:** Compare proven communication behavior in `wayne_dart_controller/` with the production main controller under `src/intelipump_fdc/`.  
**Constraint:** Analysis only. No production code changes. Test controller and capture evidence untouched.

**Working reference evidence (operator log summary):** Active master on Pi successfully syncs wire addresses `0x50`/`0x51`; prices and RESET reach `LINK_ACKNOWLEDGED`; nozzle lift → AUTHORIZE → `APPLICATION_CONFIRMED`; return → ABORTED→IDLE→FILLING_COMPLETED→reset; poll→first-byte timing typically ~10–50 ms.

**Important caveat:** The working test controller may mislabel a zero-delivery abort as a completed sale (see § Anti-patterns). Gaps below extract **proven communication / session logic only**, not sale-business rules that should be copied.

---

## 1. Working controller — behavior inventory

### 1.1 Serial configuration

| Setting | Value | Source |
|---|---|---|
| Port | `/dev/ttyUSB0` (config) | `wayne_dart_controller/config/settings.json` |
| Baud / data / stop | 9600 / 8 / 1 | same |
| Parity | **ODD** | same |
| `read_timeout_sec` | **0.01** | same → `DARTSerialTransport.read_timeout` |
| `quiet_gap_timeout_sec` | **0.015** | incomplete buffers discarded after quiet gap |
| `tx_delay_sec` | **0.035** | pre-TX delay for non-ACK writes |
| `ack_delay_sec` | **0.005** | shorter turnaround before ACK TX |
| Chunk strategy | `in_waiting` → `read(min(waiting, 64))`, else `read(1)` | `serial_transport._rx_worker` |

### 1.2 Reader thread + frame accumulator

- **Dedicated RX daemon thread** (`DARTSerialTransport._rx_worker`) is the sole `serial.read` caller.
- Bytes append to a shared `bytearray` with **per-byte monotonic timestamps** (`buffer_byte_times`).
- Overflow: shift oldest bytes when buffer exceeds `max_buffer_size` (512).
- Quiet gap: if buffer non-empty and last byte older than `quiet_gap_timeout`, discard buffer (debug log).

### 1.3 Short-frame recognition

In `_parse_buffer`:

- Address hunt: leading byte must be in `0x50..0x6F`.
- Short frame if `ctrl ∈ {0x20, 0x70}` **or** `(ctrl & 0xF0) == 0xC0`, **and** third byte `0xFA`.
- Emitted as `Level2Frame(is_short=True)` with first/last byte times.

Note: EOT is treated as **exact** `ctrl == 0x70` in `poll_pump` (not full `0x70–0x7F` family).

### 1.4 Long-frame boundaries + CRC

- Candidate scan for `b"\x03\xFA"` from offset ≥ 2.
- Body = bytes before trailing `CRC_LE(2) + 03 FA`; CRC over `body` via `calculate_crc16_dart` (init `0x0000`, poly `0xA001`).
- On CRC match: dispatch long frame with `payload = body[2:]` (strip ADR+CTRL).
- On CRC fail: try next `03 FA` candidate (`crc_candidate_failure_count`).
- Incomplete candidate: leave bytes in buffer (wait for more / quiet-gap discard).

### 1.5 Address dispatch

`CentralFrameDispatcher` holds **independent `queue.Queue` per allowed address** (`0x50`, `0x51`). Frames for other addresses are dropped (not queued).

### 1.6 Poll construction / timing / response window

- Poll bytes: `[addr, 0x20, 0xFA]`.
- `write_frame(..., is_ack=False)` applies **tx_delay** then write+flush; returns **TX start monotonic**.
- Response window: **120 ms** from `poll_write_time`.
- Wait loop: `queue.get(timeout=min(0.01, remaining))`; on `queue.Empty` → **continue** (does not exit the poll window).
- Stale filter: if `frame.first_byte_time < poll_write_time`, count stale, optionally decode, **continue** waiting.
- Short `0x70` EOT → mark online, **break**.
- Long DATA → decode; if recognized → mark online, send ACK (`0xC0 | (ctrl & 0x0F)`), drain pump event queue; **continue** until EOT or deadline (**multiple long frames per poll** supported).

### 1.7 ACK construction / timing

- ACK: `[addr, 0xC0 | seq_nibble, 0xFA]` where seq nibble comes from the **received DATA control**.
- Sent via `write_frame(..., is_ack=True)` → **ack_delay 5 ms**, then write+flush.

### 1.8 Sequence handling + retries

- Per-pump `seq_num` starts at **`0x30`**; `get_reserved_seq_ctrl()` returns current value **without advancing**.
- `send_transaction`: build DATA with reserved ctrl; **retries reuse the same sequence**; advance **only** after short ACK whose low nibble matches reserved seq (`advance_seq_ctrl`: `0x30..0x3F` wrap via `% 16`).
- Result enum distinguishes `LINK_ACKNOWLEDGED` (wire ACK) vs later `APPLICATION_CONFIRMED` (status observed post-command).

### 1.9 Direction-aware transaction parsing

`DARTTransactionParser.parse_payload(..., direction=PUMP_TO_CONTROLLER)`:

| TRANS | LNG | Type |
|---|---|---|
| `0x01` | 1 | **DC1_STATUS** (not CD1) |
| `0x02` | 8 | DC2 volume/amount BCD |
| `0x03` | 4 | DC3 price + NOZIO (`bit4` OUT, low nibble logical nozzle) |
| `0x65` | * | DC101 partial label |

Controller→pump direction maps `0x01`→CD1, `0x02`→CD2, `0x05`→CD5.

### 1.10 Per-address state, sync, offline

`PumpState` per address: `online`, `synchronized`, `observed_status`, `nozzle_position` (starts **UNKNOWN**), lifecycle, seq, timestamps, event queue.

**Offline:** consecutive missed polls (no valid EOT/recognized DATA) ≥ `offline_missed_poll_threshold` (5) → `online=False`, `synchronized=False`.

**Sync (startup in `main.py`):** up to 30 polls; optionally CD1 RETURN_STATUS (`01 01 00`) when online but status/nozzle UNKNOWN. Requires **online AND known DC1 AND known NOZIO** for **3 consecutive** valid cycles before `synchronized=True`. Communication online ≠ synchronized.

### 1.11 Authorization / application confirmation / lifecycle

- `authorize`: CD2 allowed nozzles → CD1 AUTHORIZE (`01 01 06`); optional poll loop ≤0.6 s until `observed_status==AUTHORIZED` **and** `last_status_time > last_command_time` → `APPLICATION_CONFIRMED`.
- `reset`: CD1 RESET (`01 01 05`); same post-command status confirmation for RESET.
- Nozzle OUT edge → (optional pre-reset if not RESET) → authorize with app confirm.
- FILLING_COMPLETED → latched “sale completed” log + reset (**see anti-patterns** — may fire on zero-delivery abort path).

---

## 2. Main controller — runtime path (serial → state)

Trace (not filename-only):

1. **Open serial:** `SerialTransport.open` (`protocol/dart/transport/serial.py`) — 9600 8O1, default `read_timeout_s=0.02`, `read_chunk_size=256`.
2. **Poll loop:** `ControllerLoop.run` → round-robin `_poll_one` (`controller/controller_loop.py`).
3. **Optional outbound DATA:** `_maybe_send_outbound` pops `OutboundQueue` item, builds DATA via `build_data_frame`, single response wait for ACK matching `tx_sequence`, advances sequence on ACK.
4. **POLL TX:** `PumpSession.build_poll` → `build_poll(logical)` → wire `50 20 FA` / `51 20 FA`.
5. **RX:** `_read_one_frame(response_timeout_ms)` repeatedly calls `transport.read` → `read_serial_chunk` (in_waiting / read(1) / drain) → feeds **shared** `LegacyIgemStreamAssembler` → returns **first** assembled `DartLineFrame` or `None` at deadline.
6. **Handle:** `PumpSession.handle_response_frame`:
   - EOT → healthy, no ACK
   - DATA → CRC gate → **strict `expected_rx_sequence`** → decode → map → state machine → return ACK bytes
   - Unexpected control / seq mismatch → persistent fault, **no ACK**
7. **ACK TX:** immediate `_write_frame(ack)` — **no ack_delay**, no pre-TX RX drain.
8. **Defaults:** `PollSchedulerConfig.response_timeout_ms=25`, `inter_poll_delay_ms=5`, `idle_sleep_ms=20` (`controller/poll_scheduler.py`).

Bench / real-Wayne lab tools (`bench_poll/`, `real_wayne_price/`) already implement closer patterns (permanent reader thread, chunk timestamps, stale ownership, `drain_pending_rx`, ACK match). Those are **not** the production `ControllerLoop` path.

---

## 3. Behavior comparison matrix

Legend for **Likely causes communication failure?** — whether the difference can prevent reliable poll/ACK/command exchange on a live dual-address Wayne bus (not merely sale semantics).

| # | Feature / protocol behavior | Working (file + method) | Main (file + method) | What working does | What main currently does | Likely causes comm failure? | Recommended correction | Risk |
|---|---|---|---|---|---|---|---|---|
| 1 | Dedicated RX reader + per-byte timestamps | `serial_transport.py` `_rx_worker` | `SerialTransport.read` / `ControllerLoop._read_one_frame` (async, no permanent reader); bench has `PermanentSerialReader` | Background thread timestamps every read batch into buffer byte times | Async read in poll loop; first-byte event optional; no per-frame first/last byte times on assembler frames | **Yes** (contention, stale ownership, latency) | Adopt permanent reader + capture-time ownership like bench; keep main DI/events | **CRITICAL** |
| 2 | Independent RX queues per wire address `0x50`/`0x51` | `CentralFrameDispatcher` | Shared `LegacyIgemStreamAssembler` in `ControllerLoop` | Frames routed only to matching address queue | One assembler; poll for addr A may consume or be confused by B’s bytes | **Yes** | Per-address demux queues (or ownership filter by wire ADR + TX time) before session handle | **CRITICAL** |
| 3 | Clear / drain RX before TX (stale defense) | Implicit via `first_byte_time < write_time` skip | `ControllerLoop` none; `session_helpers.drain_pending_rx` only in lab actives | Stale frames ignored for ownership of this exchange | Leftover chunks/frames can be treated as response to new POLL/DATA | **Yes** | Drain or timestamp-gate RX before POLL/DATA TX (as real-Wayne helpers) | **CRITICAL** |
| 4 | Poll response window duration | `master_controller.poll_pump` **120 ms** | `PollSchedulerConfig.response_timeout_ms` **25** | Matches observed 10–50 ms first-byte with margin | Window shorter than many real first-byte latencies → false timeouts | **Yes** | Raise default/response config to ≥100–120 ms for real Wayne; keep transport read timeout short | **CRITICAL** |
| 5 | Continue wait after temporary empty queue / empty read | `poll_pump` `except queue.Empty: continue` | `_read_one_frame` continues while `chunk` empty until deadline | Empty is non-terminal | Empty is non-terminal (OK) | No (this pair OK) | Keep continue-through-empty; do not treat Empty as timeout | **LOW** |
| 6 | Exit after first frame vs multi-frame until EOT | `poll_pump` loop until EOT/`0x70` or deadline | `_poll_one` / `_read_one_frame` returns **first** frame only | Multiple DATA + ACK, then EOT | One frame per poll; later DATA in same response lost / un-ACKed | **Yes** | Session: after POLL, collect until EOT or deadline; ACK each valid DATA | **CRITICAL** |
| 7 | ACK after DATA with matching seq nibble | `poll_pump` / `send_transaction` | `PumpSession._on_data` → `build_ack` | Always ACK recognized CRC-valid DATA using **frame’s** seq | ACK only if seq == `expected_rx_sequence` (else reject, no ACK) | **Yes** | ACK CRC-valid pump DATA by frame seq; learn/sync expected seq from live traffic (don’t fault on first mismatch forever) | **CRITICAL** |
| 8 | ACK delay (RS-485 turnaround) | `write_frame(is_ack=True)` **5 ms** | `_write_frame` immediate | Short delay before ACK TX | No ack delay | **Likely** | Configurable `ack_delay_ms` (~5) before ACK TX | **HIGH** |
| 9 | Pre-TX delay for POLL/DATA | `tx_delay_sec` **35 ms** | none | Bus settle before master TX | Immediate TX after prior RX/ACK | **Likely** | Configurable `tx_delay_ms` for non-ACK writes | **HIGH** |
| 10 | Serial read timeout vs protocol deadline | read timeout **10 ms**; protocol window 120 ms | read timeout **20 ms**; protocol **25 ms** | Short OS timeout; long software window | Short OS timeout good; protocol window too short | **Yes** (deadline) | Keep short `read_timeout_s`; separate larger software response deadline | **CRITICAL** |
| 11 | `read(64)` / chunk latency | cap 64 with `in_waiting` | `read_serial_chunk` + chunk size 256 (already avoids naive `read(256)` block) | Low false latency | Already mitigated in transport | No (transport OK) | Keep `read_serial_chunk`; avoid reverting to fixed large `read(n)` | **LOW** |
| 12 | Response / first-byte timestamps for ownership | `Level2Frame.first_byte_time` / `last_byte_time` | Events have mono timestamps; frames lack ownership vs TX | Strict stale filter | Weak / absent in ControllerLoop | **Yes** | Stamp frames/chunks; reject `capture < tx_complete` | **CRITICAL** |
| 13 | Stale-frame handling on command ACK wait | `send_transaction` stale decode+continue; ACK match by addr+seq | `_maybe_send_outbound` single `_read_one_frame`; no stale filter | Stale ACK cannot confirm new command | Stale ACK/DATA may falsely match or confuse | **Yes** | `not_before` monotonic gate like `wait_for_ack_frame` | **CRITICAL** |
| 14 | Retries reuse same TX sequence | `send_transaction` loop; advance only after ACK | Outbound: on timeout **no retry / no seq hold**; advance only on ACK (good) but no retry reuse path | Same seq on retry | Timeout abandons item without retry-with-same-seq | Partial | Non-idempotent: bounded retries **same** seq; advance only on matching ACK | **HIGH** |
| 15 | Sequence advances only after matching ACK | `advance_seq_ctrl` after ACK nibble match | `tx_sequence` advanced only on ACK match in `_maybe_send_outbound` | Same intent | Same intent for TX | No (TX path OK if retries added carefully) | Preserve; add retries | **LOW** |
| 16 | Per-address TX sequence state | `PumpState.seq_num` | `PumpSessionState.tx_sequence` per session | Independent | Independent sessions exist | No | Keep per-session seq | **LOW** |
| 17 | Strict expected **RX** sequence from pump | Working accepts any DATA seq and ACKs it | `expected_rx_sequence` mismatch → fault, **no ACK** | Follows pump | Can refuse live pump seq after restart/desync | **Yes** | Sync expected RX from first valid DATA; duplicate policy keep; don’t withhold ACK solely for learned-seq drift without recovery | **CRITICAL** |
| 18 | Direction-aware TRANS `0x01` (DC1 vs CD1) | Parser with `FrameDirection.PUMP_TO_CONTROLLER` | Decoder emits `AMBIGUOUS_CD1_OR_DC1`; mapper forced `resolve_as_dc1=True` on poll DATA | Explicit DC1 on P2C | Effectively DC1 via mapper flags (works if always set) | Unlikely if mapper flags always set | Keep explicit P2C/C2P on controller-owned exchanges; never treat CD1 wire as DC1 on TX path | **MEDIUM** |
| 19 | NOZIO `0x01` IN / `0x11` OUT | `parser` bit4 + low nibble | `nozio.decode_nozio` same masks | Correct | Correct | No | Keep | **LOW** |
| 20 | CRC rejection (no decode / no ACK) | CRC fail → try next candidate; no dispatch | `_on_data` CRC false → fault, no ACK | Safe | Safe | No | Keep | **LOW** |
| 21 | Quiet-gap incomplete frame discard | 15 ms quiet discard | Assembler holds until SF / overflow / reconnect reset; `_read_one_frame` no quiet expire | Recovers stuck junk | Partial frames can block until overflow | Possible | Quiet-gap / partial expire between polls (bench `expire_partial`) | **MEDIUM** |
| 22 | EOT handling = link alive | Short `0x70` ends poll; sets online | `ControlType.EOT` → healthy, no ACK | Correct | Correct for single EOT | No if EOT seen | Ensure EOT not discarded when DATA expected earlier in same window | **MEDIUM** |
| 23 | Application confirmation after link ACK | `reset`/`authorize` `confirm_application` poll until post-cmd status | Outbound stops at wire ACK | Distinguishes LINK vs APPLICATION | No APPLICATION_CONFIRMED phase | Indirect (false “success”) | Add optional confirm phase behind safety flags | **HIGH** |
| 24 | Sync requires known DC1 + NOZIO | `main.py` sync loop | Health = EOT/DATA only; no `synchronized` gate | Won’t authorize until synchronized | Can claim healthy without status/nozzle baseline | Indirect | Add sync gate separate from communication health | **HIGH** |
| 25 | Online vs synchronized distinction | `online` + `synchronized` | `CommunicationHealth` only | Explicit | Collapsed | Indirect | Split “link up” vs “state synchronized” | **MEDIUM** |
| 26 | CD1 RETURN_STATUS when UNKNOWN | Sync sends `01 01 00` | `enqueue_status_request` exists but not auto-sync | Proactive | Passive / not in default loop | Indirect | Gated auto RETURN_STATUS during sync only | **MEDIUM** |
| 27 | Active authorize/reset/price in controller loop | Built into `main.py` event loop | Safety: LISTEN_ONLY; actives via gated lab CLIs / outbound queue | Test master always active when not passive | Production correctly gated | No (safety) | Do **not** auto-enable; transplant confirm/seq/poll mechanics only behind flags | **LOW** (safety keep) |
| 28 | Sale / FILLING_COMPLETED publishing | Treats FILLING_COMPLETED as sale + reset | Persistence bridge / transaction service with evidence keys | Over-fires (anti-pattern) | More guarded SM, but must not import working’s sale latch | No for comm | See anti-patterns; require valid sale evidence | **LOW** (do not copy) |

---

## 4. Runtime gap narrative (highest impact)

### 4.1 Communication foundation

The working master succeeds because RX is continuous, timestamped, and **demultiplexed per address**, while poll/command waits use a **software deadline (~120 ms)** that continues through empty queue pops and **ignores pre-TX stale frames**. The main `ControllerLoop` still performs RX inside the poll coroutine against a **shared assembler**, with a **25 ms** default response timeout—below the observed 10–50 ms first-byte band—and treats the **first** assembled frame as the entire response.

### 4.2 ACK / sequence session rules

Working ACKs CRC-valid pump DATA using the **frame’s own sequence nibble**, and only advances **controller** TX sequence after a matching short ACK. Retries **reuse** the reserved TX sequence. Main withholds ACK when pump DATA seq ≠ `expected_rx_sequence`, which can desynchronize a real dispenser after restart or missed frames. Lab helpers already implement drain + `not_before` ACK matching; production loop does not.

### 4.3 Application layer

Main’s decoder/mapper stack is richer and mostly correct for NOZIO/DC2/CRC when direction is resolved. The missing pieces for “working-like” control are **application confirmation**, **sync requiring DC1+NOZIO**, and **multi-frame poll sessions**—not raw CRC math.

---

## 5. Anti-patterns in the working controller (do not transplant)

Documented here so migration does **not** copy sale bugs:

| Anti-pattern | Working behavior | Correct rule |
|---|---|---|
| Every `FILLING_COMPLETED` as real sale | `main.py` latches sale + reset on status | Require valid sale evidence (below) |
| Sale when volume/amount zero | Logs completed with `0.0` L | Zero delivery → abort / no sale |
| `ABORTED` → `COMPLETED` | Lifecycle may still see FILLING_COMPLETED after abort path | Abort stays non-sale; FILLING_COMPLETED without FILLING ≠ sale |
| Reset when already RESET | May re-issue | Skip if already RESET and confirmed |
| `LINK_ACKNOWLEDGED` as `APPLICATION_CONFIRMED` | Only if confirm loop skipped/failed soft | Never equate wire ACK with app confirm |
| Auto authorize without feature flag | Default active master | Require explicit enable + physical enable |
| Auto price change at startup without flag | `auto_startup_commands` | Feature-flag only |

**Valid sale evidence (minimum):**

1. Authorization application-confirmed  
2. FILLING observed in lifecycle  
3. DC2 volume **and** amount increased  
4. FILLING_COMPLETED observed  
5. Not already ABORTED / aborted-no-delivery  

Zero-volume lift-return → **`ABORTED_NO_DELIVERY`**, not completed sale.

---

## 6. What main already has (do not rip out)

- Layered DART CRC/escaping/frame build-parse (`protocol/dart/line/*`)
- Legacy iGEM wire map 1→`0x50`, 2→`0x51`
- `read_serial_chunk` latency fix
- Pump state machine, safety LISTEN_ONLY, outbound eligibility
- Event broker, persistence, MQTT, API, health/liveness
- Bench / real-Wayne proven helpers to **mine patterns from** (not replace architecture wholesale)

---

## 7. Sources consulted

**Working:**  
`wayne_dart_controller/src/driver/serial_transport.py`, `core/master_controller.py`, `core/pump_state.py`, `protocol/{parser,crc,bcd}.py`, `main.py`, `config/settings.json`, `tests/test_crc_and_codecs.py`

**Main:**  
`protocol/dart/transport/serial.py`, `line/{legacy_stream,frame_parser,frame_builder,control,sequence,addressing}.py`, `application/{decoder,nozio}.py`, `controller/{controller_loop,pump_session,poll_scheduler,outbound,session_models}.py`, `state_machine/wayne_mapper.py`, `bench_poll/{serial_reader,poll_io}.py`, `real_wayne_price/session_helpers.py`, `docs/architecture/polling-engine.md`
