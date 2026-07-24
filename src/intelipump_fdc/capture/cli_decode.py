"""CLI: intelipump-decode-capture — offline decode of passive JSONL captures."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from intelipump_fdc.capture.decode_offline import decode_capture_file
from intelipump_fdc.capture.report import (
    OperatorObservations,
    PassiveConclusion,
    suggest_conclusion,
    write_validation_report,
)
from intelipump_fdc.core.config import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="intelipump-decode-capture",
        description=(
            "Offline decode of a passive capture JSONL file. "
            "Never opens a serial port and never transmits."
        ),
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to wayne-passive-*.jsonl capture",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full decode result as JSON",
    )
    parser.add_argument(
        "--write-report",
        action="store_true",
        help="Write validation-report.md beside the capture (or --report-dir)",
    )
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Directory for validation-report.md (default: <capture-id>/ beside file)",
    )
    parser.add_argument(
        "--conclusion",
        choices=[c.value for c in PassiveConclusion],
        default=None,
        help="Optional operator conclusion (PASS never auto-selected)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Optional capture duration (seconds) for the report",
    )
    parser.add_argument(
        "--operator-interference",
        action="store_true",
        help="Mark that dispenser operation changed / interference occurred",
    )
    return parser


def run(argv: list[str] | None = None) -> None:
    settings = get_settings()
    if settings.environment.upper() != "LAB":
        raise SystemExit("intelipump-decode-capture is LAB-only")

    parser = build_parser()
    args = parser.parse_args(argv)
    path = Path(args.input)
    if not path.is_file():
        raise SystemExit(f"capture file not found: {path}")

    # Offline only: decode_capture_file never opens a serial port.
    result = decode_capture_file(path)
    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print(f"captureId={result.capture_id}")
        print(f"totalRxBytes={result.total_rx_bytes}")
        print(f"candidateFrames={len(result.candidate_frames)}")
        print(f"crcValid={result.crc_valid_count} crcInvalid={result.crc_invalid_count}")
        print(f"possibleAddresses={result.possible_addresses}")
        print(f"undecodedTrailingHex={result.undecoded_trailing_hex!r}")
        print("certainty_note=candidate interpretations only; not confirmed field truth")

    if args.write_report:
        capture_id = result.capture_id or path.stem
        report_dir = (
            Path(args.report_dir) if args.report_dir else path.parent / capture_id
        )
        report_path = report_dir / "validation-report.md"
        conclusion = (
            PassiveConclusion(args.conclusion) if args.conclusion else None
        )
        final = suggest_conclusion(
            result,
            operator_interference=args.operator_interference or None,
            operator_force=conclusion,
        )
        write_validation_report(
            path,
            report_path,
            observations=OperatorObservations(),
            conclusion=final,
            duration_s=args.duration,
        )
        print(f"report={report_path}")


if __name__ == "__main__":
    run()
