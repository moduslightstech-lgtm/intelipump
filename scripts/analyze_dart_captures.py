#!/usr/bin/env python3
"""Analyze Wayne DART captures and emit Phase 2 fixtures + summary.

Reads JSONL under captures/raw/private/. Does not perform serial I/O.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from intelipump_fdc.protocol.dart.line.constants import DLE, ETX, SF  # noqa: E402
from intelipump_fdc.protocol.dart.line.control import ControlType  # noqa: E402
from intelipump_fdc.protocol.dart.line.crc import (  # noqa: E402
    CANONICAL_CRC_CANDIDATE,
    compute_all_candidates,
    crc_from_le_bytes,
    dart_crc16,
)
from intelipump_fdc.protocol.dart.line.escaping import DleEscapeError, unescape_dle  # noqa: E402
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame  # noqa: E402
from intelipump_fdc.protocol.dart.line.models import DartLineFrame, ParseError  # noqa: E402

CAPTURE_DIR = ROOT / "captures" / "raw" / "private"
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "dart"


def _research_ibm_ansi_init_0000(data: bytes) -> int:
    """Isolated research duplicate of the canonical algorithm (cross-check only)."""
    return dart_crc16(data)


RESEARCH_CRC_CANDIDATES: dict[str, Callable[[bytes], int]] = {
    "research_ibm_ansi_init_0000": _research_ibm_ansi_init_0000,
}


@dataclass
class AssembledFrame:
    source_file: str
    raw: bytes
    quality: str
    notes: str = ""


@dataclass
class AnalysisStats:
    capture_files: list[str] = field(default_factory=list)
    records: int = 0
    assembled_frames: int = 0
    control_frames: int = 0
    data_candidates: int = 0
    parse_errors: int = 0
    incomplete_trailing: int = 0
    dle_10_fa_count: int = 0
    literal_10_count: int = 0
    adr_fa_count: int = 0
    ctrl_fa_count: int = 0
    control_type_counts: Counter[str] = field(default_factory=Counter)
    crc_candidate_matches: Counter[str] = field(default_factory=Counter)
    research_crc_matches: Counter[str] = field(default_factory=Counter)
    sequence_transitions: list[dict[str, object]] = field(default_factory=list)
    wrap_f_to_0: int = 0
    wrap_f_to_1: int = 0
    direction_note: str = (
        "All JSONL DATA records use direction=MERGED_BUS; "
        "frame direction is UNKNOWN (do not infer from CTRL)."
    )


def parse_hex(hex_data: str) -> bytes:
    return bytes(int(part, 16) for part in hex_data.split() if part)


def load_capture_bytes(path: Path) -> tuple[bytes, int]:
    chunks: list[bytes] = []
    records = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        records += 1
        if obj.get("event_type") != "DATA":
            continue
        hex_data = obj.get("hex_data") or ""
        if not hex_data.strip():
            continue
        chunks.append(parse_hex(hex_data))
    return b"".join(chunks), records


def split_frames(stream: bytes, source_file: str) -> tuple[list[AssembledFrame], bytes]:
    frames: list[AssembledFrame] = []
    buf = bytearray()
    for byte in stream:
        buf.append(byte)
        if byte == SF and not (len(buf) >= 2 and buf[-2] == DLE):
            raw = bytes(buf)
            quality = "confirmed_complete" if len(raw) >= 3 else "incomplete_fragment"
            frames.append(AssembledFrame(source_file=source_file, raw=raw, quality=quality))
            buf.clear()
    return frames, bytes(buf)


def analyze_data_crc(buffer: bytes) -> dict[str, object]:
    if len(buffer) < 5 or buffer[-1] != ETX:
        return {"ok": False, "reason": "not_data_structure"}
    payload = buffer[2:-3]
    received = crc_from_le_bytes(buffer[-3], buffer[-2])
    crc_input = buffer[: 2 + len(payload)]
    production = compute_all_candidates(crc_input)
    research = {name: fn(crc_input) for name, fn in RESEARCH_CRC_CANDIDATES.items()}
    prod_matches = [name for name, value in production.items() if value == received]
    research_matches = [name for name, value in research.items() if value == received]

    experiments: dict[str, dict[str, bool]] = {}
    for label, data in {
        "excl_adr": buffer[1 : 2 + len(payload)],
        "payload_only": payload,
        "incl_etx": buffer[: 2 + len(payload)] + bytes((ETX,)),
    }.items():
        vals = compute_all_candidates(data)
        vals.update({n: f(data) for n, f in RESEARCH_CRC_CANDIDATES.items()})
        swapped = ((received & 0xFF) << 8) | ((received >> 8) & 0xFF)
        experiments[label] = {
            "matches_le": any(v == received for v in vals.values()),
            "matches_be": any(v == swapped for v in vals.values()),
        }

    return {
        "ok": True,
        "crc_input_hex": crc_input.hex(" ").upper(),
        "payload_hex": payload.hex(" ").upper(),
        "received_crc": received,
        "received_crc_low": buffer[-3],
        "received_crc_high": buffer[-2],
        "production_matches": prod_matches,
        "research_matches": research_matches,
        "production_values": production,
        "research_values": research,
        "experiments": experiments,
    }


def fixture_base(
    *,
    name: str,
    source: str,
    raw: bytes,
    frame: DartLineFrame,
    notes: str,
) -> dict[str, object]:
    return {
        "name": name,
        "sourceFile": source,
        "rawHex": raw.hex(" ").upper(),
        "direction": "UNKNOWN",
        "expectedAddress": frame.address,
        "expectedControlType": frame.control_type.value,
        "expectedSequence": frame.sequence,
        "expectedCrcValid": frame.crc_valid,
        "notes": notes,
    }


def main() -> int:
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    stats = AnalysisStats()
    control_fixtures: list[dict[str, object]] = []
    data_fixtures: list[dict[str, object]] = []
    captured_controls: list[dict[str, object]] = []
    captured_data: list[dict[str, object]] = []
    detailed_data: list[dict[str, object]] = []

    last_data_seq: dict[int, int] = {}
    seen_control_keys: set[tuple[int, int]] = set()
    seen_data_raw: set[bytes] = set()
    # Track DATA sequences separately would require direction — unavailable.

    jsonl_files = sorted(CAPTURE_DIR.glob("*.jsonl"))
    if not jsonl_files:
        print("ERROR: no JSONL captures in", CAPTURE_DIR, file=sys.stderr)
        return 1

    for path in jsonl_files:
        stats.capture_files.append(path.name)
        stream, records = load_capture_bytes(path)
        stats.records += records
        frames, trailing = split_frames(stream, path.name)
        if trailing:
            stats.incomplete_trailing += 1
        stats.assembled_frames += len(frames)

        for assembled in frames:
            raw = assembled.raw
            stats.dle_10_fa_count += sum(
                1
                for i in range(len(raw) - 1)
                if raw[i] == DLE and raw[i + 1] == SF
            )
            i = 0
            while i < len(raw):
                if raw[i] == DLE:
                    if i + 1 < len(raw) and raw[i + 1] == SF:
                        i += 2
                        continue
                    stats.literal_10_count += 1
                i += 1

            parsed = parse_frame(raw)
            if isinstance(parsed, ParseError):
                stats.parse_errors += 1
                continue

            frame = parsed
            if frame.address == SF:
                stats.adr_fa_count += 1
            if frame.control == SF:
                stats.ctrl_fa_count += 1
            stats.control_type_counts[frame.control_type.value] += 1

            is_data = frame.control_type is ControlType.DATA or frame.received_crc is not None
            if is_data and frame.received_crc is not None:
                try:
                    buffer = unescape_dle(raw[:-1])
                except DleEscapeError:
                    stats.parse_errors += 1
                    continue
                crc_info = analyze_data_crc(buffer)
                if not crc_info.get("ok"):
                    continue
                stats.data_candidates += 1
                for name in crc_info["production_matches"]:  # type: ignore[index]
                    stats.crc_candidate_matches[str(name)] += 1
                for name in crc_info["research_matches"]:  # type: ignore[index]
                    stats.research_crc_matches[str(name)] += 1

                seq = frame.sequence
                addr = frame.address
                if addr in last_data_seq:
                    prev = last_data_seq[addr]
                    if prev == 0xF and seq == 0x0:
                        stats.wrap_f_to_0 += 1
                        kind = "F_TO_0"
                    elif prev == 0xF and seq == 0x1:
                        stats.wrap_f_to_1 += 1
                        kind = "F_TO_1"
                    elif seq == prev:
                        kind = "DUPLICATE"
                    elif ((prev + 1) & 0xF) == seq:
                        kind = "INCREMENT"
                    else:
                        kind = "GAP_OR_RESET"
                    stats.sequence_transitions.append(
                        {"address": addr, "from": prev, "to": seq, "kind": kind}
                    )
                last_data_seq[addr] = seq

                prod_matches = list(crc_info["production_matches"])  # type: ignore[arg-type]
                matching = (
                    CANONICAL_CRC_CANDIDATE.value
                    if CANONICAL_CRC_CANDIDATE.value in prod_matches
                    else (prod_matches[0] if prod_matches else None)
                )
                note = (
                    "Merged-bus capture; direction UNKNOWN. "
                    f"CRC matches={prod_matches}; "
                    f"research cross-check={crc_info['research_matches']}."
                )
                item = fixture_base(
                    name=f"data-{path.stem}-addr{addr:02X}-seq{seq:X}-{len(data_fixtures)}",
                    source=path.name,
                    raw=raw,
                    frame=frame,
                    notes=note,
                )
                # crc_valid under canonical default should be True when matched
                item["expectedCrcValid"] = matching == CANONICAL_CRC_CANDIDATE.value
                item.update(
                    {
                        "unescapedHex": buffer.hex(" ").upper(),
                        "payloadHex": crc_info["payload_hex"],
                        "receivedCrcLow": crc_info["received_crc_low"],
                        "receivedCrcHigh": crc_info["received_crc_high"],
                        "matchingCrcCandidate": matching,
                        "productionCrcMatches": prod_matches,
                        "researchCrcMatches": crc_info["research_matches"],
                        "quality": assembled.quality,
                    }
                )
                if raw not in seen_data_raw:
                    seen_data_raw.add(raw)
                    data_fixtures.append(item)
                    detailed_data.append(
                        {
                            "rawHex": item["rawHex"],
                            "crcInputHex": crc_info["crc_input_hex"],
                            "receivedCrc": crc_info["received_crc"],
                            "productionValues": crc_info["production_values"],
                            "researchValues": crc_info["research_values"],
                            "experiments": crc_info["experiments"],
                        }
                    )
                captured_data.append(item)
            else:
                stats.control_frames += 1
                key = (frame.address, frame.control)
                note = (
                    "Complete control frame from MERGED_BUS capture; "
                    "direction UNKNOWN (metadata does not separate master/slave)."
                )
                item = fixture_base(
                    name=(
                        f"ctrl-{frame.control_type.value}-"
                        f"addr{frame.address:02X}-ctrl{frame.control:02X}"
                    ),
                    source=path.name,
                    raw=raw,
                    frame=frame,
                    notes=note,
                )
                item["expectedCrcValid"] = None
                if frame.control_type in {ControlType.POLL, ControlType.IAP}:
                    item["expectedSequence"] = None
                if key not in seen_control_keys:
                    seen_control_keys.add(key)
                    control_fixtures.append(item)
                captured_controls.append(item)

    proven_candidate: str | None = None
    for name, count in stats.crc_candidate_matches.most_common():
        if count >= 2:
            proven_candidate = name
            break

    crc_vectors: list[dict[str, object]] = []
    if proven_candidate is not None:
        for item in data_fixtures:
            matches = list(item.get("productionCrcMatches") or [])
            if proven_candidate not in matches:
                continue
            detail = next(d for d in detailed_data if d["rawHex"] == item["rawHex"])
            crc_vectors.append(
                {
                    "name": item["name"],
                    "sourceFile": item["sourceFile"],
                    "rawHex": item["rawHex"],
                    "direction": "UNKNOWN",
                    "crcInputHex": detail["crcInputHex"],
                    "receivedCrcLow": item["receivedCrcLow"],
                    "receivedCrcHigh": item["receivedCrcHigh"],
                    "matchingCrcCandidate": proven_candidate,
                    "notes": (
                        f"Independent DATA frame matching {proven_candidate}. "
                        "Merged-bus capture; direction UNKNOWN."
                    ),
                }
            )
            if len(crc_vectors) >= 8:
                break

    preferred_controls = [
        bytes.fromhex("50 20 FA"),
        bytes.fromhex("51 20 FA"),
        bytes.fromhex("50 70 FA"),
        bytes.fromhex("51 70 FA"),
    ]
    curated_controls: list[dict[str, object]] = []
    by_raw = {parse_hex(str(i["rawHex"])): i for i in control_fixtures}
    for raw in preferred_controls:
        if raw in by_raw:
            curated_controls.append(by_raw[raw])
        else:
            parsed = parse_frame(raw)
            if isinstance(parsed, DartLineFrame):
                curated_controls.append(
                    fixture_base(
                        name=f"ctrl-preferred-{raw.hex()}",
                        source=stats.capture_files[0],
                        raw=raw,
                        frame=parsed,
                        notes="Required example pattern; direction UNKNOWN.",
                    )
                )
    for ctype in ("ACK", "NAK", "ACKPOLL", "IAP"):
        for item in control_fixtures:
            if item["expectedControlType"] == ctype:
                curated_controls.append(item)
                break

    seen_raw: set[str] = set()
    unique_controls: list[dict[str, object]] = []
    for item in curated_controls + control_fixtures:
        rh = str(item["rawHex"])
        if rh in seen_raw:
            continue
        seen_raw.add(rh)
        unique_controls.append(item)
        if len(unique_controls) >= 40:
            break

    # Prefer a diverse DATA subset that includes at least one DLE-escaped frame.
    selected_data = []
    seen_sel: set[str] = set()
    dle_first = next(
        (item for item in data_fixtures if "10 FA" in str(item["rawHex"])),
        None,
    )
    if dle_first is not None:
        selected_data.append(dle_first)
        seen_sel.add(str(dle_first["rawHex"]))
    for item in data_fixtures:
        rh = str(item["rawHex"])
        if rh in seen_sel:
            continue
        selected_data.append(item)
        seen_sel.add(rh)
        if len(selected_data) >= 20:
            break

    (FIXTURE_DIR / "control_frames.json").write_text(
        json.dumps(unique_controls, indent=2) + "\n", encoding="utf-8"
    )
    (FIXTURE_DIR / "data_frames.json").write_text(
        json.dumps(selected_data, indent=2) + "\n", encoding="utf-8"
    )
    (FIXTURE_DIR / "crc_vectors.json").write_text(
        json.dumps(crc_vectors, indent=2) + "\n", encoding="utf-8"
    )
    (FIXTURE_DIR / "captured_frames.json").write_text(
        json.dumps(
            {
                "meta": {
                    "directionPolicy": stats.direction_note,
                    "captureFiles": stats.capture_files,
                    "provenCrcCandidate": proven_candidate,
                    "canonicalCrcCandidate": CANONICAL_CRC_CANDIDATE.value,
                },
                "controlFrames": unique_controls,
                "dataFrames": selected_data,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    summary = {
        "captureFiles": stats.capture_files,
        "records": stats.records,
        "assembledFrames": stats.assembled_frames,
        "controlFrames": stats.control_frames,
        "uniqueControlFixtures": len(unique_controls),
        "dataCandidates": stats.data_candidates,
        "uniqueDataFixtures": len(selected_data),
        "parseErrors": stats.parse_errors,
        "incompleteTrailingStreams": stats.incomplete_trailing,
        "controlTypeCounts": dict(stats.control_type_counts),
        "productionCrcMatches": dict(stats.crc_candidate_matches),
        "researchCrcMatches": dict(stats.research_crc_matches),
        "provenCrcCandidate": proven_candidate,
        "canonicalCrcCandidate": CANONICAL_CRC_CANDIDATE.value,
        "wrapFto0": stats.wrap_f_to_0,
        "wrapFto1": stats.wrap_f_to_1,
        "dle10FaCount": stats.dle_10_fa_count,
        "literal10Count": stats.literal_10_count,
        "adrFaCount": stats.adr_fa_count,
        "ctrlFaCount": stats.ctrl_fa_count,
        "directionNote": stats.direction_note,
        "sequenceTransitionKinds": dict(
            Counter(str(t["kind"]) for t in stats.sequence_transitions)
        ),
        "ackpollObserved": stats.control_type_counts.get("ACKPOLL", 0),
        "iapObserved": stats.control_type_counts.get("IAP", 0),
    }
    print(json.dumps(summary, indent=2))
    (FIXTURE_DIR / "_analysis_summary.json").write_text(
        json.dumps(
            {
                "summary": summary,
                "sampleDetailedData": detailed_data[:5],
                "sampleTransitions": stats.sequence_transitions[:40],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
