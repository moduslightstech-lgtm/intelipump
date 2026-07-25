"""CLI: intelipump-capture-passive — receive-only Wayne lab capture."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

from intelipump_fdc.capture.port_guards import (
    DEFAULT_CONTROLLER_SERVICE,
    DEFAULT_LOCK_DIR,
    PassiveCaptureRefusedError,
    run_preflight_guards,
)
from intelipump_fdc.capture.receive_only import (
    ReceiveOnlySerialConfig,
    ReceiveOnlySerialSource,
    TxInhibitNotConfirmedError,
)
from intelipump_fdc.capture.session import PassiveCaptureConfig, PassiveCaptureSession
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.protocol.dart.transport.serial import SerialParity


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="intelipump-capture-passive",
        description=(
            "PASSIVE_CAPTURE_ONLY: receive-only serial capture. "
            "Never writes, polls, ACKs, or authorizes. "
            "Stop intelipump.service before use so the port is free."
        ),
    )
    parser.add_argument("--port", required=True, help="Serial device path")
    parser.add_argument("--baud", type=int, default=9600)
    parser.add_argument(
        "--output",
        default=None,
        help="JSONL output path (default: data/captures/wayne-passive-<UTC>.jsonl)",
    )
    parser.add_argument("--duration", type=float, required=True, help="Seconds")
    parser.add_argument(
        "--format",
        default="jsonl",
        choices=["jsonl"],
        help="Capture format (jsonl only)",
    )
    parser.add_argument("--read-size", type=int, default=256)
    parser.add_argument(
        "--idle-gap-ms",
        type=float,
        default=50.0,
        help="Record idleGapMs when gap since previous RX exceeds this",
    )
    parser.add_argument(
        "--reconnect-delay-s",
        type=float,
        default=0.5,
        help="Delay between reconnect attempts when the adapter disappears",
    )
    parser.add_argument(
        "--confirm-tx-physically-inhibited",
        action="store_true",
        help=(
            "Required. Software cannot prove TX is disabled; confirm physical "
            "TX inhibit/disconnect before capture."
        ),
    )
    parser.add_argument(
        "--confirm-controller-stopped",
        action="store_true",
        help=(
            "Required for physical ports. Confirms intent; service inactivity "
            "is still verified via systemctl."
        ),
    )
    parser.add_argument(
        "--controller-service",
        default=DEFAULT_CONTROLLER_SERVICE,
        help="systemd unit verified inactive when --confirm-controller-stopped",
    )
    parser.add_argument(
        "--lock-dir",
        default=str(DEFAULT_LOCK_DIR),
        help="Directory for application device lock files",
    )
    parser.add_argument(
        "--skip-port-in-use-check",
        action="store_true",
        help="LAB/test only: skip holder/lock probes (not for Wayne sessions)",
    )
    parser.add_argument(
        "--skip-service-check",
        action="store_true",
        help="LAB/test only: skip systemctl is-active check",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser


def default_output_path() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return Path(f"data/captures/wayne-passive-{stamp}.jsonl")


async def _async_main(args: argparse.Namespace) -> int:
    output = Path(args.output) if args.output else default_output_path()
    app_lock = None
    try:
        _requested, canonical, app_lock = run_preflight_guards(
            port=args.port,
            confirm_tx_physically_inhibited=args.confirm_tx_physically_inhibited,
            confirm_controller_stopped=args.confirm_controller_stopped,
            controller_service=args.controller_service,
            lock_dir=Path(args.lock_dir),
            skip_port_in_use_check=args.skip_port_in_use_check,
            skip_service_check=args.skip_service_check,
        )
    except PassiveCaptureRefusedError as exc:
        print(exc.exit_message(), file=sys.stderr)
        return 2

    source = ReceiveOnlySerialSource(
        ReceiveOnlySerialConfig(
            device=canonical,
            baud_rate=args.baud,
            parity=SerialParity.ODD,
            read_chunk_size=args.read_size,
            exclusive_open=True,
            apply_tiocexcl=True,
            requested_path=args.port,
        )
    )
    session = PassiveCaptureSession(
        source,
        PassiveCaptureConfig(
            port=canonical,
            baud=args.baud,
            output=output,
            duration_s=args.duration,
            read_size=args.read_size,
            idle_gap_ms=args.idle_gap_ms,
            reconnect_delay_s=args.reconnect_delay_s,
            format=args.format,
        ),
    )

    loop = asyncio.get_running_loop()

    def _stop() -> None:
        session.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, _stop)

    try:
        # Open happens inside session.run() before any capture file is created.
        summary = await session.run()
    except PassiveCaptureRefusedError as exc:
        print(exc.exit_message(), file=sys.stderr)
        return 2
    except OSError as exc:
        print(
            f"PASSIVE_CAPTURE_REFUSED: open failed: {exc}",
            file=sys.stderr,
        )
        return 2
    finally:
        if app_lock is not None:
            app_lock.release()

    print(f"PASSIVE_CAPTURE_ONLY complete: {summary}")
    return 0


def run(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = get_settings()
    if settings.environment.upper() != "LAB":
        raise SystemExit("intelipump-capture-passive is LAB-only")

    parser = build_parser()
    args = parser.parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        raise SystemExit(asyncio.run(_async_main(args)))
    except TxInhibitNotConfirmedError as exc:
        print(f"PASSIVE_CAPTURE_REFUSED: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    except PassiveCaptureRefusedError as exc:
        print(exc.exit_message(), file=sys.stderr)
        raise SystemExit(2) from exc
    except KeyboardInterrupt:
        print("PASSIVE_CAPTURE_ONLY interrupted", file=sys.stderr)
        raise SystemExit(130) from None


if __name__ == "__main__":
    run()
