"""CLI: RS-485 office bench discovery, validation, and HIL run."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from intelipump_fdc.core.config import get_settings
from intelipump_fdc.hardware.adapter_validation import (
    expected_serial_config,
    validate_adapter_open,
)
from intelipump_fdc.hardware.bench_runner import run_rs485_bench
from intelipump_fdc.hardware.errors import BenchConfigError, HardwareError
from intelipump_fdc.hardware.models import BenchConfig
from intelipump_fdc.hardware.serial_discovery import list_serial_devices


def run(argv: list[str] | None = None) -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        prog="intelipump-rs485-bench",
        description=(
            "Phase 10 LAB RS-485 office bench: discover adapters, validate "
            "9600/8O1, run controller↔simulator HIL. No real Wayne dispenser."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    list_p = sub.add_parser("list-adapters", help="Enumerate USB serial adapters")
    list_p.add_argument("--json", action="store_true")

    val_p = sub.add_parser("validate", help="Validate adapter opens with odd parity")
    val_p.add_argument("--port", required=True)
    val_p.add_argument("--json", action="store_true")

    run_p = sub.add_parser("run", help="Run two-adapter controller↔simulator bench")
    run_p.add_argument("--controller-port", default=settings.bench.controller_port)
    run_p.add_argument("--simulator-port", default=settings.bench.simulator_port)
    run_p.add_argument(
        "--controller-stable-id",
        default=settings.bench.controller_adapter_stable_id,
    )
    run_p.add_argument(
        "--simulator-stable-id",
        default=settings.bench.simulator_adapter_stable_id,
    )
    run_p.add_argument("--bench-name", default=settings.bench.name)
    run_p.add_argument("--duration", type=float, default=settings.bench.duration_s)
    run_p.add_argument("--baud", type=int, default=9600)
    run_p.add_argument(
        "--response-timeout-ms",
        type=int,
        default=settings.bench.response_timeout_ms,
    )
    run_p.add_argument("--addresses", default="1,2")
    run_p.add_argument(
        "--evidence",
        default="data/bench/rs485-bench-evidence.jsonl",
        help="Capture JSONL path (summary JSON written alongside)",
    )
    run_p.add_argument(
        "--report",
        default=None,
        help="Markdown report path (default: evidence stem .md)",
    )
    run_p.add_argument("--log-frames", action="store_true")
    run_p.add_argument("--physical", action="store_true", help="Mark run as physical HIL")
    run_p.add_argument("--skip-open-validation", action="store_true")
    run_p.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)

    if settings.environment.upper() != "LAB":
        raise SystemExit("intelipump-rs485-bench is LAB-only")

    try:
        if args.command == "list-adapters":
            devices = list_serial_devices()
            payload = [d.to_dict() for d in devices]
            if args.json:
                print(json.dumps(payload, indent=2))
            else:
                if not payload:
                    print("No USB serial adapters found.")
                for d in devices:
                    print(
                        f"{d.device_path}\tstable={d.stable_id}\t"
                        f"vid={d.vid}\tpid={d.pid}\t{d.product or ''}"
                    )
            return

        if args.command == "validate":
            result = asyncio.run(
                validate_adapter_open(expected_serial_config(args.port))
            )
            if args.json:
                print(json.dumps(result.to_dict(), indent=2))
            else:
                status = "OK" if result.ok else "FAIL"
                print(f"{status} {result.device_path} parity={result.parity}")
                for err in result.errors:
                    print(f"  error: {err}")
                for warn in result.warnings:
                    print(f"  warning: {warn}")
            if not result.ok:
                raise SystemExit(1)
            return

        if args.command == "run":
            addresses = tuple(
                int(x.strip()) for x in args.addresses.split(",") if x.strip()
            )
            if args.baud != 9600:
                raise BenchConfigError("Phase 10 bench requires --baud 9600")
            config = BenchConfig(
                name=args.bench_name,
                environment="LAB",
                controller_port=args.controller_port
                or settings.bench.controller_port,
                simulator_port=args.simulator_port or settings.bench.simulator_port,
                controller_adapter_stable_id=args.controller_stable_id
                or settings.bench.controller_adapter_stable_id,
                simulator_adapter_stable_id=args.simulator_stable_id
                or settings.bench.simulator_adapter_stable_id,
                baud_rate=args.baud,
                response_timeout_ms=args.response_timeout_ms,
                protocol_target_ms=settings.bench.protocol_target_ms,
                turnaround_delay_ms=settings.bench.turnaround_delay_ms,
                inter_frame_delay_ms=settings.bench.inter_frame_delay_ms,
                automatic_direction_control=settings.bench.automatic_direction_control,
                termination_enabled=settings.bench.termination_enabled,
                bias_enabled=settings.bench.bias_enabled,
                ground_reference_connected=settings.bench.ground_reference_connected,
                addresses=addresses,
                duration_s=args.duration,
                exclusive_open=settings.bench.exclusive_open,
            )
            if not config.controller_port or not config.simulator_port:
                raise BenchConfigError(
                    "set --controller-port and --simulator-port "
                    "(prefer /dev/serial/by-id/...)"
                )
            evidence_path = Path(args.evidence)
            report_path = Path(args.report) if args.report else None
            evidence = asyncio.run(
                run_rs485_bench(
                    config,
                    evidence_path=evidence_path,
                    report_path=report_path,
                    capture_path=evidence_path
                    if evidence_path.suffix == ".jsonl"
                    else evidence_path.with_suffix(".jsonl"),
                    skip_open_validation=args.skip_open_validation,
                    log_frames=args.log_frames,
                    physical_run=bool(args.physical),
                )
            )
            if args.json:
                print(json.dumps(evidence.to_dict(), indent=2))
            else:
                print(
                    f"bench={evidence.bench.name} "
                    f"physical_hil_status={evidence.physical_hil_status} "
                    f"software_ok={evidence.success} "
                    f"polls={evidence.poll_count} data={evidence.data_count} "
                    f"timeouts={evidence.timeout_count}"
                )
                lat = (
                    evidence.latency.to_dict()
                    if evidence.latency is not None and hasattr(evidence.latency, "to_dict")
                    else {}
                )
                intervals = lat.get("intervals") or {}
                print(
                    "latency (write_complete→complete_response) "
                    f"mean_ms={lat.get('mean_ms')} "
                    f"p95_ms={lat.get('p95_ms')} p99_ms={lat.get('p99_ms')} "
                    f"jitter_ms={lat.get('jitter_ms')}"
                )
                for key in (
                    "poll_write_complete_to_first_response_byte",
                    "poll_write_complete_to_complete_response",
                    "data_receive_complete_to_ack_write_start",
                    "ack_write_complete_to_next_poll_write_start",
                ):
                    series = intervals.get(key) or {}
                    print(
                        f"  {key}: mean_ms={series.get('mean_ms')} "
                        f"count={series.get('count')}"
                    )
                print(f"capture={evidence.capture_path}")
                print(f"evidence_json={evidence.evidence_json_path}")
                print(f"report={evidence.report_markdown_path}")
            # Physical PASS required for exit 0 on --physical; otherwise software ok.
            if args.physical and evidence.physical_hil_status != "PASS":
                raise SystemExit(2)
            if not evidence.success:
                raise SystemExit(2)
            return
    except HardwareError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    run()
