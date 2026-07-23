# Phase 2 Capture Validation Report

Date: 2026-07-21  
Scope: Validate pure DART line utilities against real Wayne traffic.  
No serial I/O, simulator, application commands, or active control were added.

## 1. Capture inventory

| File | Location | Format | Notes |
|---|---|---|---|
| `capture_2026-07-19_merged_a.jsonl` | `captures/raw/private/` | JSONL | Copied from `docs/reference/private/Pasted text(28).txt` |
| `capture_2026-07-19_merged_b.jsonl` | `captures/raw/private/` | JSONL | Copied from `docs/reference/private/Pasted text(29).txt` |
| `capture_2026-07-19_merged_c.jsonl` | `captures/raw/private/` | JSONL | Copied from `docs/reference/private/Pasted text(30).txt` |
| `Pasted text(27).txt` | `docs/reference/private/` | Console log | Not machine-parseable JSONL; ignored for fixtures |
| `captures/labeled/` | empty | — | No labeled captures available |

Capture metadata markers show:

```text
CAPTURE_STARTED mode=ONE_PORT_MERGED baud=9600 format=8O1 burst_gap_ms=4.0
```

Every DATA record uses `direction=MERGED_BUS`. Therefore every fixture marks
`direction: "UNKNOWN"`. Control type alone is never used to infer direction.

## 2. Assembly results

From `uv run python scripts/analyze_dart_captures.py`:

| Metric | Count |
|---|---|
| JSONL records | 7599 |
| Assembled complete frames (SF-terminated) | 7379 |
| Control frames | 5281 |
| DATA candidates | 2098 |
| Parse errors | 0 |
| Incomplete trailing streams | 0 |

### Control-type histogram

| Type | Count |
|---|---|
| POLL | 2067 |
| ACK | 2068 |
| EOT | 1103 |
| DATA | 2098 |
| NAK | 43 |
| ACKPOLL | 0 |
| IAP | 0 |

Confirmed required examples present:

- `50 20 FA` — POLL addr 0x50
- `51 20 FA` — POLL addr 0x51
- `50 70 FA` — EOT addr 0x50
- `51 70 FA` — EOT addr 0x51
- ACK values in `C0–CF`
- NAK values in `50–5F` (43 observations)

ACKPOLL (`E0–EF`) and IAP (`40`) were **not** observed in these captures.

## 3. CRC analysis

### Spec claims

DART Serial Communication / Line-Level Specification (pages 2–3) and
`docs/protocol-notes/dart-line-summary.md` state:

- CRC-16, named CCITT in vendor text
- Initial value `0000h`
- Coverage: ADR through last unescaped data byte
- Inserted DLE excluded
- CRC-1 = low byte, CRC-2 = high byte

### Candidate results over ADR\|\|CTRL\|\|payload

| Candidate | Matches / 2098 DATA frames |
|---|---|
| `dart_ibm_ansi_init_0000` | **2098** |
| `ccitt_false_init_0000` | 0 |
| `ccitt_false_init_0000_xor_ffff` | 0 |
| `ccitt_true_init_0000` | 0 |
| `ccitt_true_init_0000_xor_ffff` | 0 |

### Boundary experiments (research)

For sampled DATA frames, these did **not** produce matches:

- exclude ADR
- payload only
- include ETX in CRC input
- treat received CRC as big-endian target for CCITT candidates

### Detailed example

Wire:

```text
51 30 65 01 01 22 83 03 FA
```

| Field | Value |
|---|---|
| ADR | `0x51` |
| CTRL | `0x30` (DATA, seq 0) |
| Payload | `65 01 01` |
| CRC-1 / CRC-2 | `22` / `83` → word `0x8322` |
| ETX / SF | `03` / `FA` |
| CRC input | `51 30 65 01 01` |
| `dart_ibm_ansi_init_0000` | `0x8322` (match) |
| CCITT candidates | no match |

A second independent frame (`51 3A ... 60 27 03 FA`) also matches only
`dart_ibm_ansi_init_0000`.

### Canonical CRC decision

**Proven:** `crc16_dart_ibm_ansi_init_0000` / alias `dart_crc16`

- Algorithm: reflected poly `0xA001`, init `0x0000`, xorout `0x0000`
  (IBM/ANSI/Modbus-style processing)
- Evidence: **2098 / 2098** complete DATA candidates across three captures
- CCITT candidates retained for diagnostics only
- Default builder/parser candidate set to the canonical function

Unresolved naming conflict: vendor text says “CCITT”, but traffic does not
match the implemented CCITT-family candidates. The canonical name documents
the proven algorithm rather than the vendor label.

## 4. Sequence behavior

Observed DATA sequence transitions **per address on the merged bus**:

| Kind | Count |
|---|---|
| GAP_OR_RESET | 1710 |
| INCREMENT | 271 |
| DUPLICATE | 98 |
| F→0 | 10 |
| F→1 | 7 |

Interpretation:

- Master and slave each maintain an independent TX# (spec page 3).
- Merged one-port captures interleave both directions under the same ADR.
- Therefore per-address transitions on this stream are **not** a clean
  single TX# timeline.
- Both `F→0` and `F→1` appear; neither can be proven as the sole live rule
  from these captures alone.

**Action:** `next_tx_sequence()` remains the documented helper (`F → 1`).
No code change until direction-separated captures exist.

## 5. DLE / escaping

| Observation | Count |
|---|---|
| Wire `10 FA` sequences | 44 |
| Literal `0x10` not followed by `FA` | 522 |
| ADR == `FA` | 0 |
| CTRL == `FA` | 0 |

Conclusions:

- Escaping of SF in payload/CRC is present in real traffic.
- Literal `0x10` occurs frequently and must not be treated as an escape by itself.
- No evidence of ADR/CTRL equal to `FA` in these captures.
- Current implementation (escape entire unescaped buffer before trailing SF;
  unescape `DLE+SF` → SF; literal DLE otherwise) is consistent with the
  observed frames and with the Tx/Rx interrupt description.

## 6. Capture quality classification

| Class | Assessment |
|---|---|
| Confirmed complete frames | 7379 SF-terminated assemblies that parsed |
| Likely complete frames | Same set; burst records sometimes fragment then reassemble cleanly |
| Incomplete fragments | No trailing incomplete stream remained after full-file concat |
| Merged-direction traffic | **All** captures — direction UNKNOWN |
| CRC unvalidated frames | None among DATA candidates once canonical CRC applied |

## 7. Safety / scope

- `LISTEN_ONLY` defaults unchanged
- No serial transport
- No active dispenser commands
- No FastAPI / MQTT / persistence changes

## 8. Unresolved questions

1. Why vendor documentation names CCITT while traffic matches IBM/ANSI-style CRC.
2. True TX# wrap rule on a single direction (`F→1` vs `F→0`) — needs
   direction-separated captures.
3. ACKPOLL and IAP behavior — not present in this corpus.
4. Whether dual-adapter captures would change any boundary conclusions.

## 9. Artifacts

- `tests/fixtures/dart/captured_frames.json`
- `tests/fixtures/dart/control_frames.json`
- `tests/fixtures/dart/data_frames.json`
- `tests/fixtures/dart/crc_vectors.json`
- `scripts/analyze_dart_captures.py`
- `tests/protocol/dart/line/test_captured_frames.py`
- Canonical CRC in `src/intelipump_fdc/protocol/dart/line/crc.py`
