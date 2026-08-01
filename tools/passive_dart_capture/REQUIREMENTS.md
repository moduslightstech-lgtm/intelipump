# Passive DART Capture — Phase 0 Requirements & Plan

**Workspace path used:** `/Users/babatundealaraje/Documents/moduslights/intelipump-fdc`  
(`/home/intelipump/intelipump/intelipump-fdc` was not present on this machine.)

**Mode:** strictly passive / LISTEN_ONLY. No transmit path.

---

## 1. Safe-to-reuse modules (read-only)

These modules decode, classify, or read bytes only. Thin wrappers preferred over copies.

| Module | Use |
|--------|-----|
| `intelipump_fdc.protocol.dart.line.crc` | Canonical CRC-16 (`dart_crc16`) |
| `intelipump_fdc.protocol.dart.line.frame_parser` | `parse_frame` for complete wire frames |
| `intelipump_fdc.protocol.dart.line.stream` | `FrameStreamAssembler` for chunk reassembly |
| `intelipump_fdc.protocol.dart.line.captured_classify` | POLL / short ACK / DATA classification + shape-based direction inference |
| `intelipump_fdc.protocol.dart.line.control` / `constants` | Control types, SF/DLE/ETX, POLL/ACK bases |
| `intelipump_fdc.protocol.dart.line.models` | `DartLineFrame`, `ParseError` |
| `intelipump_fdc.protocol.dart.application.splitter` | `split_transactions` (TRANS+LNG+DATA) |
| `intelipump_fdc.protocol.dart.application.decoder` | Read-only DC1/DC2/DC3/DC5 (+ unknown preserve) |
| `intelipump_fdc.protocol.dart.application.nozio` | Documented NOZIO masks (`decode_nozio`) |
| `intelipump_fdc.protocol.dart.application.status` | Wayne DC1 status codes |
| `intelipump_fdc.protocol.dart.application.bcd` / `fields` | BCD helpers used by decoder |
| `intelipump_fdc.protocol.dart.transport.serial.read_serial_chunk` | Drain-friendly read helper (no write) |
| `intelipump_fdc.protocol.dart.transport.serial.SerialParity` / `_parity_constant` | Odd-parity open kwargs only |

Existing fixtures (reference for tests, not imported into the tool package):

- `tests/fixtures/dart/` and `tests/fixtures/dart/legacy_igem_epump/`
- Protocol unit tests under `tests/protocol/dart/`

Existing production capture (`intelipump_fdc.capture.*`) is **not** depended on for this tool’s schema (different record types). Patterns for exclusive open / TX-inhibit confirmation may be mirrored locally if needed; this tool keeps a simpler sync reader under `serial_reader.py`.

---

## 2. Must NOT import / use

| Banned | Reason |
|--------|--------|
| `intelipump_fdc.protocol.dart.transport.serial.SerialTransport` | Has `write()` / drain |
| `intelipump_fdc.protocol.dart.transport.base.ByteTransport.write` usage | Transmit API |
| `intelipump_fdc.protocol.dart.line.frame_builder` (in tool package) | TX frame construction (tests may use for fixtures) |
| `intelipump_fdc.real_wayne_price.*` (`reset_session`, `authorize_session`, write CLIs, CD1/CD2 write paths) | Active controller commands |
| `intelipump_fdc.protocol.cd1_*`, `cd2_reset`, price-write builders used for TX | Active command encoding |
| `intelipump_fdc.bench_poll.session` / continuous poll write paths | Transmit polls/commands |
| `intelipump_fdc.controller.*` session writers | Active controller |
| Any `serial.write` / `Serial.write` / `setRTS` / DE assert for TX enable | Bus drive |
| Command replay / RESET / AUTHORIZE / price-setting | Safety |

### Static safety banned patterns (exact)

Scanned on `tools/passive_dart_capture/**/*.py` **excluding** `tests/`:

1. `serial.write` / `Serial.write`
2. `.write(` only when attribute receiver is clearly serial (`ser.write`, `self._ser.write`, `transport.write` of byte transports) — **file** `.write(` for JSONL is allowed
3. `setRTS` / `rts = True` / `RTS` assert / `driver_enable` / `de_assert` / `assert_de` for TX
4. Imports matching: `real_wayne_price`, `reset_session`, `authorize_session`, `cd2_reset`, `frame_builder`, `SerialTransport`, `BenchPollSerialTransport`
5. Function defs named `transmit` / `replay` that send bytes (any `def transmit` / `def replay` in package)

Allowed: `Path.write_text`, evidence file `fh.write(...)`, JSON dumps.

---

## 3. Implementation plan

1. **serial_reader.py** — sync open 9600 8O1, short timeout, raw `read` only; timestamp after each OS read; never RTS/DE/write.
2. **frame_assembler.py** — wrap `FrameStreamAssembler` + classify; track first/last byte timestamps; emit frame dicts (polls, short ACKs, data, malformed/partial/CRC-invalid).
3. **dart_parser.py** — split/decode via existing decoder; DC1 label map (incl. `LIMIT_REACHED` for 0x06); DC2 raw preserve + verified BCD; DC3 price + local documented-mask NOZIO (OUT if `&0x10` else IN); DC5/unknown preserve; no invented fields.
4. **evidence_writer.py** — append JSONL under default `tools/passive_dart_capture/evidence/<sessionId>.jsonl`.
5. **markers.py** — operator marker set + JSONL records.
6. **capture.py** — loop: read → `serial_chunk` → assemble → `frame` (+ parse); SIGINT clean stop; direction `MERGED_BUS` on chunks, `UNKNOWN`/`INFERRED` on frames.
7. **analyzer.py** — offline JSONL → `reports/` CSVs/MD/JSON; dedupe by `sessionId+frameSequence`; complete frames only for frame analytics.
8. **cli.py** — `capture` / `marker` / `analyze` / `compare`.
9. **tests/** — framing, CRC, DC*, markers, timestamps, dedupe, SIGINT, static safety.
10. **README.md** — usage; no hardware auto-run.

---

## 4. Proposed file structure

```
tools/passive_dart_capture/
├── README.md
├── REQUIREMENTS.md
├── __init__.py
├── __main__.py
├── capture.py
├── serial_reader.py
├── frame_assembler.py
├── dart_parser.py
├── evidence_writer.py
├── markers.py
├── analyzer.py
├── cli.py
├── evidence/          # default capture output (gitignored contents)
├── reports/           # default analyzer output
└── tests/
    ├── conftest.py
    ├── test_frame_assembler.py
    ├── test_dart_parser.py
    ├── test_evidence_and_markers.py
    ├── test_analyzer.py
    ├── test_capture_shutdown.py
    └── test_static_safety.py
```

---

## 5. Defaults

| Setting | Value |
|---------|-------|
| Device | `/dev/ttyUSB0` |
| Baud | 9600 |
| Framing | 8 data, odd parity, 1 stop |
| Read timeout | 0.05 s |
| Evidence dir | `tools/passive_dart_capture/evidence/` |
| Reports dir | `tools/passive_dart_capture/reports/` |
| Chunk direction | `MERGED_BUS` |
| Source tag | `EPUMP_PASSIVE_CAPTURE` |
