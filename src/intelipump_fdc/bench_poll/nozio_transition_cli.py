"""Poll-only offline analyzer: compare three NOZIO evidence captures.

Compares nozzle-in / nozzle-held-out / nozzle-returned JSONL evidence.
Does not open serial ports or transmit any commands.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from intelipump_fdc.protocol.dart.application.nozio import (
    DC3_NOZIO_SPEC_REF,
    decode_nozio,
)

CLASS_NO_CHANGE = "NO_NOZZLE_POSITION_CHANGE_OBSERVED"
CLASS_EXPECTED_N1 = "EXPECTED_NOZZLE1_IN_OUT_IN_OBSERVED"
CLASS_OTHER_TRANSITION = "NOZZLE_POSITION_TRANSITION_OBSERVED_OTHER"
CLASS_INSUFFICIENT = "INSUFFICIENT_DC3_NOZIO_SAMPLES"


@dataclass(frozen=True, slots=True)
class CaptureNozioSummary:
    path: Path
    samples: int
    nozio_counts: dict[str, int]
    dominant_nozio: int | None
    dominant_hex: str | None
    any_out: bool
    parse_errors: int


def _extract_nozio_bytes(raw_hex: str) -> list[int]:
    """Pull NOZIO bytes from a DATA frame application payload."""
    raw = bytes.fromhex(raw_hex.replace(" ", ""))
    if len(raw) < 8 or (raw[1] & 0xF0) != 0x30:
        return []
    app = raw[2:-4]
    found: list[int] = []
    i = 0
    while i + 1 < len(app):
        trans, lng = app[i], app[i + 1]
        end = i + 2 + lng
        if end > len(app):
            break
        body = app[i + 2 : end]
        if trans == 0x03 and lng >= 4 and len(body) >= 4:
            found.append(body[3])
        i = end
    return found


def summarize_evidence_jsonl(path: Path) -> CaptureNozioSummary:
    counts: Counter[int] = Counter()
    samples = 0
    parse_errors = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            parse_errors += 1
            continue
        raw_hex = obj.get("rawHex") or obj.get("hex_data")
        direction = obj.get("direction")
        if direction is not None and str(direction).upper() == "TX":
            continue
        if not raw_hex:
            continue
        try:
            values = _extract_nozio_bytes(str(raw_hex))
        except ValueError:
            parse_errors += 1
            continue
        for value in values:
            counts[value] += 1
            samples += 1
    dominant: int | None = None
    if counts:
        dominant = counts.most_common(1)[0][0]
    any_out = any(bool(v & 0x10) for v in counts)
    return CaptureNozioSummary(
        path=path,
        samples=samples,
        nozio_counts={f"{v:02X}": n for v, n in sorted(counts.items())},
        dominant_nozio=dominant,
        dominant_hex=None if dominant is None else f"{dominant:02X}",
        any_out=any_out,
        parse_errors=parse_errors,
    )


def classify_transition(
    nozzle_in: CaptureNozioSummary,
    held_out: CaptureNozioSummary,
    returned: CaptureNozioSummary,
) -> dict[str, Any]:
    """Classify physical IN→OUT→IN for nozzle 1 without blaming the parser."""
    if min(nozzle_in.samples, held_out.samples, returned.samples) < 1:
        classification = CLASS_INSUFFICIENT
    else:
        seq = (
            nozzle_in.dominant_nozio,
            held_out.dominant_nozio,
            returned.dominant_nozio,
        )
        if seq == (0x01, 0x11, 0x01):
            classification = CLASS_EXPECTED_N1
        elif (
            nozzle_in.dominant_nozio == held_out.dominant_nozio == returned.dominant_nozio
            and nozzle_in.dominant_nozio is not None
            and not held_out.any_out
        ):
            classification = CLASS_NO_CHANGE
        elif held_out.any_out and (
            nozzle_in.dominant_nozio != held_out.dominant_nozio
            or returned.dominant_nozio != held_out.dominant_nozio
        ):
            classification = CLASS_OTHER_TRANSITION
        else:
            classification = CLASS_NO_CHANGE

    def _side(summary: CaptureNozioSummary) -> dict[str, Any]:
        evidence = None
        if summary.dominant_nozio is not None:
            evidence = decode_nozio(summary.dominant_nozio).to_evidence_dict()
        return {
            "path": str(summary.path),
            "samples": summary.samples,
            "nozioCounts": summary.nozio_counts,
            "dominantNozioHex": summary.dominant_hex,
            "anyOutObserved": summary.any_out,
            "parseErrors": summary.parse_errors,
            "dominantDecode": evidence,
        }

    return {
        "schemaVersion": 1,
        "analyzer": "nozio-transition-offline",
        "documentationSource": DC3_NOZIO_SPEC_REF,
        "expectedNozzle1Sequence": ["01", "11", "01"],
        "classification": classification,
        "notes": [
            "Unchanged NOZIO across captures is classified as "
            f"{CLASS_NO_CHANGE}; that is not evidence the parser failed.",
            "Decoder uses documented masks: logical=0x0F, position=0x10, "
            "reserved=0xE0 (warn if reserved nonzero).",
        ],
        "captures": {
            "nozzleIn": _side(nozzle_in),
            "nozzleHeldOut": _side(held_out),
            "nozzleReturned": _side(returned),
        },
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="intelipump-nozio-transition-analyze",
        description=(
            "Offline poll-only analyzer: compare three evidence JSONL captures "
            "(nozzle-in, nozzle-held-out, nozzle-returned) using documented "
            "DC3 NOZIO masks. No serial I/O; no CD5/RESET/AUTHORIZE/raw TX."
        ),
    )
    p.add_argument("--nozzle-in", type=Path, required=True)
    p.add_argument("--nozzle-held-out", type=Path, required=True)
    p.add_argument("--nozzle-returned", type=Path, required=True)
    p.add_argument("--json", action="store_true")
    return p


def run(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    for path in (args.nozzle_in, args.nozzle_held_out, args.nozzle_returned):
        if not path.is_file():
            print(f"NOZIO_ANALYZE_REFUSED: missing file {path}", file=sys.stderr)
            return 2
    report = classify_transition(
        summarize_evidence_jsonl(args.nozzle_in),
        summarize_evidence_jsonl(args.nozzle_held_out),
        summarize_evidence_jsonl(args.nozzle_returned),
    )
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"classification: {report['classification']}")
        print(f"documentationSource: {report['documentationSource']}")
        for name, side in report["captures"].items():
            print(
                f"{name}: dominant={side['dominantNozioHex']} "
                f"samples={side['samples']} counts={side['nozioCounts']} "
                f"anyOut={side['anyOutObserved']}"
            )
    return 0 if report["classification"] != CLASS_INSUFFICIENT else 1


if __name__ == "__main__":
    raise SystemExit(run())
