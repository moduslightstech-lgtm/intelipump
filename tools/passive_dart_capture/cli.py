"""CLI for strictly passive Wayne DART capture and offline analysis.

Commands never transmit. Capture opens the serial port in receive-only mode
(behaviorally: no write/RTS/DE calls). Do not run against hardware unless the
operator intentionally starts ``capture``. Prefer ``dry-run`` for pipeline checks.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from tools.passive_dart_capture.analyzer import (
    analyze_path,
    compare_sessions,
    resolve_evidence_path,
)
from tools.passive_dart_capture.capture import run_capture
from tools.passive_dart_capture.dry_run import DEFAULT_FIXTURE, run_dry_run
from tools.passive_dart_capture.evidence_writer import DEFAULT_EVIDENCE_DIR, DEFAULT_REPORTS_DIR
from tools.passive_dart_capture.markers import ALLOWED_MARKERS, append_marker
from tools.passive_dart_capture.serial_reader import PASSIVE_SERIAL_OPEN_LIMITATIONS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="passive_dart_capture",
        description=(
            "Strictly passive Wayne DART RS-485 capture/analysis. "
            "LISTEN_ONLY: never transmits, never asserts RTS/DE. "
            "Does not auto-run against hardware — choose a subcommand."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    cap = sub.add_parser("capture", help="Start passive serial capture (stop with Ctrl+C / SIGINT)")
    cap.add_argument(
        "--device",
        default="/dev/ttyUSB0",
        help="Serial device (default /dev/ttyUSB0)",
    )
    cap.add_argument("--baud", type=int, default=9600, help="Baud rate (default 9600)")
    cap.add_argument("--session-id", default=None, help="Optional session id")
    cap.add_argument(
        "--evidence-dir",
        type=Path,
        default=DEFAULT_EVIDENCE_DIR,
        help=f"Evidence directory (default: {DEFAULT_EVIDENCE_DIR})",
    )

    dry = sub.add_parser(
        "dry-run",
        help=(
            "Hardware-free pipeline check: fixture → capture evidence → analyze "
            "(writes under a temp directory; never opens a serial device)"
        ),
    )
    dry.add_argument(
        "--fixture",
        type=Path,
        default=DEFAULT_FIXTURE,
        help=f"Hex fixture path (default: {DEFAULT_FIXTURE})",
    )
    dry.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="Output directory (default: tempfile.mkdtemp)",
    )
    dry.add_argument(
        "--session-id",
        default="dry-run",
        help="Session id for evidence/reports (default: dry-run)",
    )
    dry.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep the temp work directory (default: delete when --work-dir omitted)",
    )

    mark = sub.add_parser("marker", help="Append an operator marker to a session JSONL")
    mark.add_argument("session_id", help="Session id")
    mark.add_argument(
        "marker",
        help="Marker name: " + ", ".join(sorted(ALLOWED_MARKERS)),
    )
    mark.add_argument("--note", default=None, help="Optional note")
    mark.add_argument(
        "--evidence-dir",
        type=Path,
        default=DEFAULT_EVIDENCE_DIR,
        help=f"Evidence directory (default: {DEFAULT_EVIDENCE_DIR})",
    )

    an = sub.add_parser("analyze", help="Offline analysis of a capture JSONL")
    an.add_argument("session_id", help="Session id (looks up evidence/<id>.jsonl)")
    an.add_argument(
        "--evidence-dir",
        type=Path,
        default=DEFAULT_EVIDENCE_DIR,
        help=f"Evidence directory (default: {DEFAULT_EVIDENCE_DIR})",
    )
    an.add_argument(
        "--evidence-file",
        type=Path,
        default=None,
        help="Explicit JSONL path (overrides session_id lookup)",
    )
    an.add_argument(
        "--reports-dir",
        type=Path,
        default=DEFAULT_REPORTS_DIR,
        help=f"Reports directory (default: {DEFAULT_REPORTS_DIR})",
    )

    cmp_ = sub.add_parser("compare", help="Compare two capture sessions")
    cmp_.add_argument("session_a", help="First session id")
    cmp_.add_argument("session_b", help="Second session id")
    cmp_.add_argument(
        "--evidence-dir",
        type=Path,
        default=DEFAULT_EVIDENCE_DIR,
        help=f"Evidence directory (default: {DEFAULT_EVIDENCE_DIR})",
    )
    cmp_.add_argument(
        "--reports-dir",
        type=Path,
        default=DEFAULT_REPORTS_DIR,
        help=f"Reports directory (default: {DEFAULT_REPORTS_DIR})",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "capture":
        print(
            "PASSIVE CAPTURE starting (LISTEN_ONLY). "
            "No transmit. Stop with Ctrl+C / SIGINT.",
            file=sys.stderr,
        )
        print(PASSIVE_SERIAL_OPEN_LIMITATIONS, file=sys.stderr)
        path = run_capture(
            device=args.device,
            baud=args.baud,
            session_id=args.session_id,
            evidence_dir=args.evidence_dir,
        )
        print(f"Evidence written: {path}")
        return 0

    if args.command == "dry-run":
        created_temp = args.work_dir is None
        result = run_dry_run(
            fixture_path=args.fixture,
            work_dir=args.work_dir,
            session_id=args.session_id,
        )
        summary = result.analysis.summary
        counts = summary["recordCounts"]
        print("DRY-RUN complete (no serial / no hardware access).")
        print(f"  work_dir: {result.work_dir}")
        print(f"  evidence: {result.evidence_path}")
        print(f"  chunks: {counts['serialChunks']}  frames: {counts['frameRecords']}")
        print(f"  completeFramesAnalyzed: {counts['completeFramesAnalyzed']}")
        print(f"  crcInvalid: {summary['errors'].get('crcInvalidFrames', 0)}")
        print(f"  incompleteFrames: {summary['errors'].get('incompleteFrames', 0)}")
        for name, p in result.analysis.report_paths.items():
            print(f"  {name}: {p}")
        if created_temp and not args.keep_temp:
            shutil.rmtree(result.work_dir, ignore_errors=True)
            print("  (temp work_dir removed; pass --keep-temp to retain)")
        elif created_temp and args.keep_temp:
            print("  (temp work_dir kept)")
        return 0

    if args.command == "marker":
        record = append_marker(
            args.session_id,
            args.marker,
            evidence_dir=args.evidence_dir,
            note=args.note,
        )
        print(f"Marker {record['marker']} appended for session {args.session_id}")
        return 0

    if args.command == "analyze":
        path = resolve_evidence_path(
            args.session_id,
            evidence_dir=args.evidence_dir,
            evidence_file=args.evidence_file,
        )
        if not path.is_file():
            print(f"Evidence not found: {path}", file=sys.stderr)
            return 1
        result = analyze_path(
            path,
            reports_dir=args.reports_dir,
            session_id=args.session_id,
        )
        print(f"Analyzed session {result.session_id}")
        for name, p in result.report_paths.items():
            print(f"  {name}: {p}")
        return 0

    if args.command == "compare":
        path_a = resolve_evidence_path(args.session_a, evidence_dir=args.evidence_dir)
        path_b = resolve_evidence_path(args.session_b, evidence_dir=args.evidence_dir)
        if not path_a.is_file() or not path_b.is_file():
            print(f"Evidence missing: {path_a} or {path_b}", file=sys.stderr)
            return 1
        comparison = compare_sessions(path_a, path_b, reports_dir=args.reports_dir)
        print(json_dumps(comparison))
        return 0

    parser.error(f"unknown command {args.command}")
    return 2


def json_dumps(obj: object) -> str:
    import json

    return json.dumps(obj, indent=2, ensure_ascii=True)


if __name__ == "__main__":
    raise SystemExit(main())
