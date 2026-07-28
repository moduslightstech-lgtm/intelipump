"""CLI: intelipump-continuous-poll-bench (CONTINUOUS_POLL_BENCH)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

from intelipump_fdc.bench_poll.guards import PollBenchRefusedError
from intelipump_fdc.bench_poll.transport import (
    BenchPollSerialConfig,
    BenchPollSerialTransport,
)
from intelipump_fdc.capture.port_guards import DEFAULT_CONTROLLER_SERVICE, DEFAULT_LOCK_DIR
from intelipump_fdc.continuous_poll_bench.guards import (
    ContinuousPollBenchParams,
    ContinuousPollConfirmations,
    ContinuousPollRefusedError,
    run_continuous_poll_preflight,
)
from intelipump_fdc.continuous_poll_bench.session import (
    ContinuousPollSession,
    ContinuousPollSessionConfig,
)
from intelipump_fdc.core.config import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="intelipump-continuous-poll-bench",
        description=(
            "CONTINUOUS_POLL_BENCH: bounded status-only DART polls against "
            "one owned lab pump address. Default short path (real-Wayne max 5s). "
            "Optional extended POLL-only watch (max 300s) with "
            "--confirm-extended-watch; Ctrl+C stops early. "
            "No authorize/preset/price/reset/RETURN_STATUS/daemon. "
            "Stop intelipump.service first."
        ),
    )
    parser.add_argument("--port", required=True)
    parser.add_argument(
        "--address",
        type=int,
        required=True,
        help="Exactly one pump address (1-255)",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=3,
        help=(
            "Bounded duration (default 3; real-Wayne short max 5; "
            "simulator short max 30; with --confirm-extended-watch max 300). "
            "SIGINT/SIGTERM stops early."
        ),
    )
    parser.add_argument(
        "--poll-interval-ms",
        type=int,
        default=300,
        help=(
            "Monotonic poll interval (50-1000, default 300; "
            "real-Wayne minimum 300)"
        ),
    )
    parser.add_argument(
        "--response-timeout-ms",
        type=int,
        default=250,
        help="Must be < poll-interval-ms (default 250)",
    )
    parser.add_argument(
        "--evidence-dir",
        required=True,
        help="Directory for JSONL + Markdown evidence",
    )
    parser.add_argument("--baud", type=int, default=9600)
    parser.add_argument(
        "--confirm-owned-lab-pump",
        action="store_true",
        help="Confirm privately owned lab dispenser (not production/customer)",
    )
    parser.add_argument(
        "--confirm-technician-present",
        action="store_true",
    )
    parser.add_argument(
        "--confirm-emergency-isolation-ready",
        action="store_true",
    )
    parser.add_argument(
        "--confirm-no-fuel-test",
        action="store_true",
        help="Confirm this session does not require fuel dispensing",
    )
    parser.add_argument(
        "--confirm-authorization-disabled",
        action="store_true",
        help="Confirm authorization remains disabled for this session",
    )
    parser.add_argument(
        "--confirm-status-poll-only",
        action="store_true",
        help="Confirm only verified build_poll status polls will be sent",
    )
    parser.add_argument(
        "--confirm-bounded-duration",
        action="store_true",
        help="Confirm this session is time-bounded (no daemon/indefinite mode)",
    )
    parser.add_argument(
        "--confirm-extended-watch",
        action="store_true",
        help=(
            "Confirm extended POLL-only watch: raise duration cap to 300s "
            "(still bounded; Ctrl+C / SIGTERM stops early). Required when "
            "duration exceeds the short-path max (5s real-Wayne / 30s simulator)."
        ),
    )
    parser.add_argument(
        "--controller-service",
        default=DEFAULT_CONTROLLER_SERVICE,
    )
    parser.add_argument("--lock-dir", default=str(DEFAULT_LOCK_DIR))
    parser.add_argument(
        "--simulator-port",
        action="append",
        default=[],
        help="Additional simulator adapter path to check for ownership (repeatable)",
    )
    parser.add_argument(
        "--simulator-validation",
        action="store_true",
        help=(
            "LAB simulator mode: allow known simulator to own only "
            "/dev/intelipump-simulator; controller adapter must remain free. "
            "Default (flag absent) refuses any running simulator (real Wayne)."
        ),
    )
    parser.add_argument(
        "--skip-service-check",
        action="store_true",
        help="LAB/test only",
    )
    parser.add_argument(
        "--skip-port-check",
        action="store_true",
        help="LAB/test only",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    # Explicitly do NOT accept raw hex / payloads / command types / daemon.
    return parser


def _evidence_paths(evidence_dir: Path) -> tuple[Path, Path]:
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    base = evidence_dir / f"continuous-poll-bench-{stamp}"
    return Path(str(base) + ".jsonl"), Path(str(base) + ".md")


async def _async_main(args: argparse.Namespace) -> int:
    settings = get_settings()
    params = ContinuousPollBenchParams(
        port=args.port,
        address=args.address,
        duration_seconds=float(args.duration_seconds),
        poll_interval_ms=args.poll_interval_ms,
        response_timeout_ms=args.response_timeout_ms,
        evidence_dir=Path(args.evidence_dir),
        baud=args.baud,
        confirmations=ContinuousPollConfirmations(
            owned_lab_pump=args.confirm_owned_lab_pump,
            technician_present=args.confirm_technician_present,
            emergency_isolation_ready=args.confirm_emergency_isolation_ready,
            no_fuel_test=args.confirm_no_fuel_test,
            authorization_disabled=args.confirm_authorization_disabled,
            status_poll_only=args.confirm_status_poll_only,
            bounded_duration=args.confirm_bounded_duration,
            extended_watch=args.confirm_extended_watch,
        ),
        controller_service=args.controller_service,
        lock_dir=Path(args.lock_dir),
        simulator_ports=tuple(args.simulator_port or ()),
        simulator_validation=bool(args.simulator_validation),
        skip_service_check=args.skip_service_check,
        skip_port_check=args.skip_port_check,
    )

    app_lock = None
    try:
        canonical, app_lock = run_continuous_poll_preflight(params, settings)
    except (ContinuousPollRefusedError, PollBenchRefusedError) as exc:
        msg = (
            exc.exit_message()
            if isinstance(exc, ContinuousPollRefusedError)
            else f"CONTINUOUS_POLL_BENCH_REFUSED: {exc}"
        )
        print(msg, file=sys.stderr)
        return 2

    jsonl_path, md_path = _evidence_paths(params.evidence_dir)
    transport = BenchPollSerialTransport(
        BenchPollSerialConfig(
            device=canonical,
            baud_rate=params.baud,
            requested_path=params.port,
            read_timeout_s=0.015,
        )
    )
    session = ContinuousPollSession(
        transport,
        ContinuousPollSessionConfig(
            port=params.port,
            address=params.address,
            baud=params.baud,
            duration_seconds=params.duration_seconds,
            poll_interval_ms=params.poll_interval_ms,
            response_timeout_ms=params.response_timeout_ms,
            evidence_jsonl=jsonl_path,
            evidence_md=md_path,
            max_writes=params.max_writes,
            target_type=params.target_type,
            simulator_validation=params.simulator_validation,
            canonical=canonical,
        ),
    )

    loop = asyncio.get_running_loop()

    def _stop() -> None:
        session.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, _stop)

    try:
        summary = await session.run()
    except Exception as exc:
        print(f"CONTINUOUS_POLL_BENCH_REFUSED: {exc}", file=sys.stderr)
        return 2
    finally:
        if app_lock is not None:
            app_lock.release()

    print(f"CONTINUOUS_POLL_BENCH complete: {summary}")
    result = summary.get("result")
    if result == "CONTINUOUS_POLL_BENCH_FAIL":
        return 1
    return 0


def run(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        code = asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        print("CONTINUOUS_POLL_BENCH interrupted", file=sys.stderr)
        raise SystemExit(1) from None
    raise SystemExit(code)


if __name__ == "__main__":
    run()
