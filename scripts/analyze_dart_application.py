#!/usr/bin/env python3
"""Analyze application transactions inside proven DART DATA frames.

Read-only. Does not modify captures. Emits fixture samples + JSON summary.
"""

from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload  # noqa: E402
from intelipump_fdc.protocol.dart.line.constants import DLE, SF  # noqa: E402
from intelipump_fdc.protocol.dart.line.control import ControlType  # noqa: E402
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame  # noqa: E402
from intelipump_fdc.protocol.dart.line.models import DartLineFrame  # noqa: E402

CAPTURE_DIR = ROOT / "captures" / "raw" / "private"
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "dart"
REPORT_PATH = ROOT / "docs" / "protocol-notes" / "application-capture-analysis.md"


def parse_hex(text: str) -> bytes:
    return bytes(int(part, 16) for part in text.split())


def iter_data_frames() -> list[tuple[str, DartLineFrame]]:
    frames: list[tuple[str, DartLineFrame]] = []
    for path in sorted(CAPTURE_DIR.glob("*.jsonl")):
        stream = bytearray()
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if obj.get("event_type") != "DATA":
                continue
            hex_data = obj.get("hex_data") or ""
            if hex_data.strip():
                stream.extend(parse_hex(hex_data))
        buf = bytearray()
        for byte in stream:
            buf.append(byte)
            if byte == SF and not (len(buf) >= 2 and buf[-2] == DLE):
                raw = bytes(buf)
                buf.clear()
                parsed = parse_frame(raw)
                if isinstance(parsed, DartLineFrame) and parsed.control_type is ControlType.DATA:
                    frames.append((path.name, parsed))
    return frames


def main() -> int:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    frames = iter_data_frames()
    type_counts: Counter[str] = Counter()
    wire_id_counts: Counter[int] = Counter()
    lengths: dict[str, Counter[int]] = defaultdict(Counter)
    samples: dict[str, list[dict[str, object]]] = defaultdict(list)
    trailing_total = 0
    malformed = 0
    fixture_rows: list[dict[str, object]] = []

    for source, frame in frames:
        bundle = decode_data_payload(
            frame.payload,
            pump_address=frame.address,
            line_sequence=frame.sequence,
            source_frame_raw_hex=frame.raw_frame.hex(" ").upper(),
        )
        if bundle.trailing_bytes:
            trailing_total += 1
        for tx in bundle.transactions:
            type_counts[tx.transaction_type.value] += 1
            wire_id_counts[tx.transaction_id] += 1
            lengths[tx.transaction_type.value][tx.length] += 1
            if tx.decode_status.value == "MALFORMED":
                malformed += 1
            if len(samples[tx.transaction_type.value]) < 3:
                samples[tx.transaction_type.value].append(
                    {
                        "sourceFile": source,
                        "pumpAddress": tx.pump_address,
                        "lineSequence": tx.line_sequence,
                        "direction": tx.direction.value,
                        "decodeStatus": tx.decode_status.value,
                        "rawTransactionHex": tx.raw_transaction.hex(" ").upper(),
                        "decodedBody": tx.decoded_body,
                        "warnings": list(tx.warnings),
                        "sourceFrameRawHex": tx.source_frame_raw_hex,
                    }
                )

    # Curated fixture set for tests
    wanted = [
        "AMBIGUOUS_CD1_OR_DC1",
        "DC2_FILLED_VOLUME_AMOUNT",
        "DC3_NOZZLE_STATUS_PRICE",
        "CD5_PRICE_UPDATE",
        "CD101_REQUEST_TOTALS",
        "DC101_TOTAL_COUNTERS",
        "UNKNOWN",
    ]
    for key in wanted:
        for sample in samples.get(key, []):
            fixture_rows.append(
                {
                    "name": f"{key.lower()}-{len(fixture_rows)}",
                    "sourceFile": sample["sourceFile"],
                    "direction": sample["direction"],
                    "expectedTransactionType": key,
                    "expectedDecodeStatus": sample["decodeStatus"],
                    "rawTransactionHex": sample["rawTransactionHex"],
                    "sourceFrameRawHex": sample["sourceFrameRawHex"],
                    "pumpAddress": sample["pumpAddress"],
                    "lineSequence": sample["lineSequence"],
                    "notes": "; ".join(sample["warnings"])
                    if sample["warnings"]
                    else "Decoded from merged-bus capture.",
                }
            )
            if len([r for r in fixture_rows if r["expectedTransactionType"] == key]) >= 2:
                break

    # Multi-transaction payload sample from fixtures/data_frames if present
    data_fx = json.loads((FIXTURE_DIR / "data_frames.json").read_text(encoding="utf-8"))
    multi = next((item for item in data_fx if item["payloadHex"].count(" ") > 20), None)
    if multi is not None:
        payload = parse_hex(str(multi["payloadHex"]))
        bundle = decode_data_payload(
            payload,
            pump_address=int(multi["expectedAddress"]),  # type: ignore[arg-type]
            line_sequence=int(multi["expectedSequence"]),  # type: ignore[arg-type]
            source_frame_raw_hex=str(multi["rawHex"]),
        )
        fixture_rows.append(
            {
                "name": "multi-transaction-payload",
                "sourceFile": multi["sourceFile"],
                "direction": "UNKNOWN",
                "expectedTransactionType": "MULTI",
                "expectedDecodeStatus": "MIXED",
                "payloadHex": multi["payloadHex"],
                "sourceFrameRawHex": multi["rawHex"],
                "expectedTransactionCount": len(bundle.transactions),
                "transactionTypes": [t.transaction_type.value for t in bundle.transactions],
                "notes": "Single DATA payload containing multiple application transactions.",
            }
        )

    (FIXTURE_DIR / "application_transactions.json").write_text(
        json.dumps(fixture_rows, indent=2) + "\n", encoding="utf-8"
    )

    summary = {
        "dataFramesAnalyzed": len(frames),
        "transactionTypeCounts": dict(type_counts),
        "wireIdCounts": {f"0x{k:02X}": v for k, v in sorted(wire_id_counts.items())},
        "lengthsByType": {k: dict(v) for k, v in lengths.items()},
        "payloadsWithTrailingBytes": trailing_total,
        "malformedTransactions": malformed,
        "sampleTypes": {k: v for k, v in samples.items()},
    }
    print(json.dumps(summary, indent=2))

    # Write markdown report
    lines = [
        "# Phase 3 Application Capture Analysis",
        "",
        "Source of truth: WAYNE EUROPE - Protocol Specification Dart Pump Interface",
        "Revision 2.11 (WM041550 Rev 02).",
        "",
        "Captures are `ONE_PORT_MERGED` / `MERGED_BUS`; direction is UNKNOWN unless",
        "payload structure uniquely selects a CD or DC layout.",
        "",
        "## Inventory",
        "",
        f"- DATA frames analyzed: **{len(frames)}**",
        f"- Payloads with trailing undecoded bytes: **{trailing_total}**",
        f"- Malformed known transactions: **{malformed}**",
        "",
        "## Transaction ID frequency (after TRANS+LNG split)",
        "",
        "| Wire TRANS | Count |",
        "|---|---|",
    ]
    for key, value in sorted(wire_id_counts.items()):
        lines.append(f"| `0x{key:02X}` | {value} |")

    lines.extend(
        [
            "",
            "## Logical type counts (decoder dispatch)",
            "",
            "| Type | Count | Typical LNG |",
            "|---|---|---|",
        ]
    )
    for key, value in type_counts.most_common():
        lngs = ", ".join(f"{lng}:{cnt}" for lng, cnt in lengths[key].most_common())
        lines.append(f"| `{key}` | {value} | {lngs} |")

    lines.extend(
        [
            "",
            "## Boundary rule",
            "",
            "Each application transaction is `TRANS (1) + LNG (1) + DATA (LNG)`.",
            "Multiple transactions may appear in one DATA payload (Pump Interface, page 10).",
            "",
            "## Representative samples",
            "",
        ]
    )
    for key, rows in samples.items():
        lines.append(f"### {key}")
        lines.append("")
        for row in rows[:2]:
            lines.append(f"- raw: `{row['rawTransactionHex']}`")
            lines.append(f"  - direction: `{row['direction']}` status: `{row['decodeStatus']}`")
            if row["decodedBody"] is not None:
                lines.append(f"  - body: `{json.dumps(row['decodedBody'], sort_keys=True)}`")
            if row["warnings"]:
                lines.append(f"  - warnings: {row['warnings']}")
        lines.append("")

    lines.extend(
        [
            "## Likely request/response pairs (inference only)",
            "",
            "Merged-bus ordering is not authoritative. Observed co-occurrence patterns:",
            "",
            "- `CD101` (`65 01 ..`) often near `DC101` (`65 10 ..`) — totals request/response.",
            "- `CD1/DC1` (`01 01 ..`) often near `DC3` (`03 04 ..`) — status / nozzle+price.",
            "- `CD5` (`05 03 ..`) price updates appear as standalone controller→pump frames.",
            "",
            "These pairs are **inference**, not proven direction-separated evidence.",
            "",
            "## Scaling / decimals",
            "",
            "DC7 defines `DPVOL` / `DPAMO` / `DPUNP`, but DC7 was **not observed** in these",
            "captures. Decoders therefore expose `raw_scaled` integers and leave",
            "`Decimal` values unset unless the caller supplies decimals.",
            "",
            "## Unresolved",
            "",
            "1. CD1 vs DC1 on TRANS `0x01` (same LNG=1) without direction.",
            "2. CD3 vs DC3 on TRANS `0x03` LNG=4 — ambiguous until direction/"
            "session context resolves (never prefer DC3 from TRANS alone).",
            "3. True money/volume decimal places until DC7 parameters are captured.",
            "",
        ]
    )
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
