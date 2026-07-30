"""CLI: intelipump-real-wayne-reset-write — single-shot CD1 RESET."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from intelipump_fdc.bench_poll.guards import (
    PollBenchConfirmations,
    PollBenchParams,
    PollBenchRefusedError,
    run_poll_bench_preflight,
)
from intelipump_fdc.bench_poll.transport import (
    BenchPollSerialConfig,
    BenchPollSerialTransport,
)
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.real_wayne_price.guards import (
    ResetWriteConfirmations,
    ResetWriteParams,
    validate_reset_write_params,
    validate_reset_write_settings,
)
from intelipump_fdc.real_wayne_price.reset_session import ResetWriteSession
from intelipump_fdc.real_wayne_price.states import ResetWriteState


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="intelipump-real-wayne-reset-write",
        description=(
            "Technician-supervised real-Wayne CD1 RESET attempt. "
            "Requires DC1 FILLING_COMPLETE (CLOSED after price), transmits "
            "exactly one RESET DATA frame, then observes/verifies status. "
            "Documented nozzle OUT (NOZIO bit 0x10) allows the normal path; "
            "owned-lab heads that never assert 0x10 may use the gated "
            "--allow-nozio-unknown-for-reset override with extra confirms. "
            "Does not send CD5, CD2, or AUTHORIZE."
        ),
    )
    p.add_argument("--port", required=True)
    p.add_argument("--address", type=int, required=True, choices=[1, 2])
    p.add_argument("--evidence-dir", required=True)
    p.add_argument("--baud", type=int, default=9600)
    p.add_argument("--response-timeout-ms", type=int, default=500)
    p.add_argument("--ack-timeout-ms", type=int, default=500)
    p.add_argument("--post-write-settle-ms", type=int, default=1000)
    p.add_argument("--post-write-max-status-polls", type=int, default=16)
    p.add_argument(
        "--post-reset-observation-seconds",
        type=float,
        default=3.0,
        help="Bounded fresh status observation window after RESET TX (default 3)",
    )
    p.add_argument("--sequence", type=int, default=0)
    p.add_argument("--confirm-owned-lab-pump", action="store_true")
    p.add_argument("--confirm-technician-present", action="store_true")
    p.add_argument("--confirm-no-product-connected", action="store_true")
    p.add_argument(
        "--confirm-no-fuel-test",
        action="store_true",
        help="Alias of --confirm-no-product-connected (no fuel / no product)",
    )
    p.add_argument("--confirm-motor-isolated", action="store_true")
    p.add_argument("--confirm-valves-isolated", action="store_true")
    p.add_argument("--confirm-emergency-isolation-ready", action="store_true")
    p.add_argument("--confirm-authorization-disabled", action="store_true")
    p.add_argument("--confirm-single-write-plan-reviewed", action="store_true")
    p.add_argument("--confirm-execute-cd1-reset", action="store_true")
    p.add_argument(
        "--confirm-post-write-status-verification-required", action="store_true"
    )
    p.add_argument(
        "--i-understand-this-transmits-to-owned-lab-pump", action="store_true"
    )
    p.add_argument(
        "--confirm-physical-nozzle-out",
        action="store_true",
        help="Operator confirms nozzle is physically OUT (override path)",
    )
    p.add_argument(
        "--confirm-price-visible",
        action="store_true",
        help="Operator confirms price is visible on the head (override path)",
    )
    p.add_argument(
        "--allow-nozio-unknown-for-reset",
        action="store_true",
        help=(
            "Owned-lab only: treat missing NOZIO 0x10 as UNKNOWN and allow one "
            "RESET diagnostic when all override confirms are present"
        ),
    )
    p.add_argument(
        "--confirm-reset-only",
        action="store_true",
        help="Operator confirms this session sends RESET only (override path)",
    )
    p.add_argument(
        "--confirm-no-authorize",
        action="store_true",
        help="Operator confirms AUTHORIZE is prohibited (override path)",
    )
    p.add_argument("--skip-service-check", action="store_true")
    p.add_argument("--skip-port-check", action="store_true")
    p.add_argument("--json", action="store_true")
    forbidden = {
        "--raw-hex",
        "--payload",
        "--command",
        "--authorize",
        "--replay",
        "--send",
        "--price-nozzle-1",
    }
    for action in p._actions:
        for opt in action.option_strings or []:
            if opt in forbidden:
                raise RuntimeError(f"forbidden option registered: {opt}")
    return p


def _params_from_args(args: argparse.Namespace) -> ResetWriteParams:
    no_product = bool(args.confirm_no_product_connected) or bool(
        args.confirm_no_fuel_test
    )
    confirms = ResetWriteConfirmations(
        owned_lab_pump=bool(args.confirm_owned_lab_pump),
        technician_present=bool(args.confirm_technician_present),
        no_product_connected=no_product,
        motor_isolated=bool(args.confirm_motor_isolated),
        valves_isolated=bool(args.confirm_valves_isolated),
        emergency_isolation_ready=bool(args.confirm_emergency_isolation_ready),
        authorization_disabled=bool(args.confirm_authorization_disabled),
        single_write_plan_reviewed=bool(args.confirm_single_write_plan_reviewed),
        execute_cd1_reset=bool(args.confirm_execute_cd1_reset),
        post_write_status_verification_required=bool(
            args.confirm_post_write_status_verification_required
        ),
        understand_transmits_to_owned_lab_pump=bool(
            args.i_understand_this_transmits_to_owned_lab_pump
        ),
        confirm_physical_nozzle_out=bool(args.confirm_physical_nozzle_out),
        confirm_price_visible=bool(args.confirm_price_visible),
        allow_nozio_unknown_for_reset=bool(args.allow_nozio_unknown_for_reset),
        confirm_reset_only=bool(args.confirm_reset_only),
        confirm_no_authorize=bool(args.confirm_no_authorize),
    )
    return ResetWriteParams(
        port=args.port,
        address=args.address,
        evidence_dir=Path(args.evidence_dir),
        confirmations=confirms,
        baud=args.baud,
        response_timeout_ms=args.response_timeout_ms,
        ack_timeout_ms=args.ack_timeout_ms,
        post_write_settle_ms=args.post_write_settle_ms,
        post_write_max_status_polls=args.post_write_max_status_polls,
        post_reset_observation_seconds=float(args.post_reset_observation_seconds),
        sequence=args.sequence,
        skip_service_check=bool(args.skip_service_check),
        skip_port_check=bool(args.skip_port_check),
    )


async def _async_main(argv: list[str] | None) -> int:
    args = build_parser().parse_args(argv)
    params = _params_from_args(args)
    settings = get_settings()
    try:
        validate_reset_write_params(params)
        validate_reset_write_settings(settings)
    except PollBenchRefusedError as exc:
        print(f"RESET_WRITE_REFUSED: {exc}", file=sys.stderr)
        return 2

    poll_params = PollBenchParams(
        port=params.port,
        address=params.address,
        baud=params.baud,
        max_polls=1,
        response_timeout_ms=params.response_timeout_ms,
        evidence_dir=params.evidence_dir,
        confirmations=PollBenchConfirmations(
            owned_lab_pump=params.confirmations.owned_lab_pump,
            technician_present=params.confirmations.technician_present,
            emergency_isolation_ready=params.confirmations.emergency_isolation_ready,
            no_fuel_test=params.confirmations.no_product_connected,
            authorization_disabled=params.confirmations.authorization_disabled,
        ),
        skip_service_check=params.skip_service_check,
        skip_port_check=params.skip_port_check,
    )
    app_lock = None
    try:
        canonical, app_lock = run_poll_bench_preflight(
            poll_params,
            settings,
            enforce_poll_only_bench_mode=False,
        )
    except PollBenchRefusedError as exc:
        print(f"RESET_WRITE_REFUSED: {exc}", file=sys.stderr)
        return 2

    transport = BenchPollSerialTransport(
        BenchPollSerialConfig(
            device=canonical,
            baud_rate=params.baud,
            requested_path=params.port,
            read_timeout_s=0.015,
        )
    )
    session = ResetWriteSession(transport, params, canonical_port=canonical)
    try:
        result = await session.run()
    finally:
        if app_lock is not None:
            app_lock.release()

    if args.json:
        print(json.dumps(result.summary, indent=2))
    else:
        print(f"state: {result.state.value}")
        print(f"diagnosticResult: {result.summary.get('diagnosticResult')}")
        print(f"transmitted: {result.transmitted}")
        print(f"candidateFrameHex: {result.summary.get('candidateFrameHex')}")
        print(f"ackOutcome: {result.summary.get('ackOutcome')}")
        print(f"evidence: {result.summary.get('evidence')}")
        if result.summary.get("warnings"):
            print(f"warnings: {result.summary['warnings']}")
        if result.summary.get("refusalReasons"):
            print(f"refusalReasons: {result.summary['refusalReasons']}")

    if result.state is ResetWriteState.RESET_VERIFIED:
        return 0
    if result.transmitted:
        return 1
    return 2


def run(argv: list[str] | None = None) -> int:
    return asyncio.run(_async_main(argv))


if __name__ == "__main__":
    raise SystemExit(run())
