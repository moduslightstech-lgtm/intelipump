"""CLI: intelipump-poll-bench — bounded real-pump status poll (POLL_ONLY_BENCH)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

from intelipump_fdc.bench_poll.guards import (
    PollBenchConfirmations,
    PollBenchParams,
    PollBenchRefusedError,
    run_poll_bench_preflight,
)
from intelipump_fdc.bench_poll.session import PollBenchSession, PollBenchSessionConfig
from intelipump_fdc.bench_poll.transport import (
    BenchPollSerialConfig,
    BenchPollSerialTransport,
)
from intelipump_fdc.capture.port_guards import DEFAULT_CONTROLLER_SERVICE, DEFAULT_LOCK_DIR
from intelipump_fdc.core.config import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="intelipump-poll-bench",
        description=(
            "POLL_ONLY_BENCH: bounded verified DART status poll against an owned "
            "lab pump. No authorize/preset/price/reset. Stop intelipump.service first."
        ),
    )
    parser.add_argument("--port", required=True)
    parser.add_argument("--address", type=int, required=True)
    parser.add_argument("--baud", type=int, default=9600)
    parser.add_argument("--max-polls", type=int, default=1)
    parser.add_argument("--response-timeout-ms", type=int, default=100)
    parser.add_argument(
        "--evidence-dir",
        default="data/bench/real-wayne",
        help="Directory for JSONL + Markdown evidence",
    )
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
            "LAB simulator one-poll mode: allow known simulator to own only "
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
    # Explicitly do NOT accept raw hex / payloads / command types.
    return parser


def _evidence_paths(evidence_dir: Path) -> tuple[Path, Path]:
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    base = evidence_dir / f"poll-bench-{stamp}"
    return Path(str(base) + ".jsonl"), Path(str(base) + ".md")


async def _async_main(args: argparse.Namespace) -> int:
    settings = get_settings()
    params = PollBenchParams(
        port=args.port,
        address=args.address,
        baud=args.baud,
        max_polls=args.max_polls,
        response_timeout_ms=args.response_timeout_ms,
        evidence_dir=Path(args.evidence_dir),
        confirmations=PollBenchConfirmations(
            owned_lab_pump=args.confirm_owned_lab_pump,
            technician_present=args.confirm_technician_present,
            emergency_isolation_ready=args.confirm_emergency_isolation_ready,
            no_fuel_test=args.confirm_no_fuel_test,
            authorization_disabled=args.confirm_authorization_disabled,
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
        canonical, app_lock = run_poll_bench_preflight(params, settings)
    except PollBenchRefusedError as exc:
        print(exc.exit_message(), file=sys.stderr)
        return 2

    jsonl_path, md_path = _evidence_paths(params.evidence_dir)
    transport = BenchPollSerialTransport(
        BenchPollSerialConfig(
            device=canonical,
            baud_rate=params.baud,
            requested_path=params.port,
            read_timeout_s=max(0.01, params.response_timeout_ms / 1000.0),
        )
    )
    session = PollBenchSession(
        transport,
        PollBenchSessionConfig(
            port=canonical,
            address=params.address,
            baud=params.baud,
            max_polls=params.max_polls,
            response_timeout_ms=params.response_timeout_ms,
            evidence_jsonl=jsonl_path,
            evidence_md=md_path,
            target_type=params.target_type,
            simulator_validation=params.simulator_validation,
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
        print(f"POLL_BENCH_REFUSED: {exc}", file=sys.stderr)
        return 2
    finally:
        if app_lock is not None:
            app_lock.release()

    print(f"POLL_ONLY_BENCH complete: {summary}")
    return 0 if summary.get("result") != "POLL_BENCH_FAIL" else 1


def run(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Reject any attempt to pass forbidden kwargs via unknown args — argparse
    # already rejects unknown options (no --raw-hex / --payload / --command).
    try:
        raise SystemExit(asyncio.run(_async_main(args)))
    except PollBenchRefusedError as exc:
        print(exc.exit_message(), file=sys.stderr)
        raise SystemExit(2) from exc
    except KeyboardInterrupt:
        print("POLL_ONLY_BENCH interrupted", file=sys.stderr)
        raise SystemExit(130) from None


if __name__ == "__main__":
    run()
