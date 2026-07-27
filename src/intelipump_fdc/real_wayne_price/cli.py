"""CLI: intelipump-real-wayne-price-dry-run — never transmits CD5."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from intelipump_fdc.bench_poll.guards import (
    PollBenchRefusedError,
    run_poll_bench_preflight,
)
from intelipump_fdc.bench_poll.transport import (
    BenchPollSerialConfig,
    BenchPollSerialTransport,
)
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.real_wayne_price.dry_run import PriceDryRunSession
from intelipump_fdc.real_wayne_price.guards import (
    PriceDryRunConfirmations,
    PriceDryRunParams,
    validate_price_dry_run_params,
    validate_price_dry_run_settings,
)
from intelipump_fdc.real_wayne_price.states import PriceDryRunState


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="intelipump-real-wayne-price-dry-run",
        description=(
            "Technician-supervised real-Wayne CD5 price programming dry-run. "
            "Polls status only; builds CD5 candidate in memory; never transmits "
            "a non-poll frame."
        ),
    )
    p.add_argument("--port", required=True)
    p.add_argument("--address", type=int, required=True, choices=[1, 2])
    p.add_argument("--logical-nozzle-count", type=int, required=True, choices=[1, 2])
    p.add_argument("--price-nozzle-1", type=int, required=True)
    p.add_argument("--price-nozzle-2", type=int, default=None)
    p.add_argument("--evidence-dir", required=True)
    p.add_argument("--baud", type=int, default=9600)
    p.add_argument("--response-timeout-ms", type=int, default=250)
    p.add_argument("--sequence", type=int, default=0)
    p.add_argument("--price-scale-confirmed-by-technician", action="store_true")
    p.add_argument(
        "--logical-nozzle-mapping-confirmed-by-technician", action="store_true"
    )
    p.add_argument("--confirm-owned-lab-pump", action="store_true")
    p.add_argument("--confirm-technician-present", action="store_true")
    p.add_argument("--confirm-no-product-connected", action="store_true")
    p.add_argument("--confirm-motor-isolated", action="store_true")
    p.add_argument("--confirm-valves-isolated", action="store_true")
    p.add_argument("--confirm-emergency-isolation-ready", action="store_true")
    p.add_argument("--confirm-authorization-disabled", action="store_true")
    p.add_argument("--confirm-single-write-plan-reviewed", action="store_true")
    p.add_argument("--skip-service-check", action="store_true")
    p.add_argument("--skip-port-check", action="store_true")
    p.add_argument(
        "--json",
        action="store_true",
        help="Print summary JSON to stdout",
    )
    # Explicitly reject dangerous options if someone adds them later.
    forbidden = {
        "--raw-hex",
        "--payload",
        "--command",
        "--authorize",
        "--replay",
        "--transmit",
        "--write",
        "--send",
    }
    for action in p._actions:
        for opt in action.option_strings or []:
            if opt in forbidden:
                raise RuntimeError(f"forbidden option registered: {opt}")
    return p


def _params_from_args(args: argparse.Namespace) -> PriceDryRunParams:
    confirms = PriceDryRunConfirmations(
        owned_lab_pump=bool(args.confirm_owned_lab_pump),
        technician_present=bool(args.confirm_technician_present),
        no_product_connected=bool(args.confirm_no_product_connected),
        motor_isolated=bool(args.confirm_motor_isolated),
        valves_isolated=bool(args.confirm_valves_isolated),
        emergency_isolation_ready=bool(args.confirm_emergency_isolation_ready),
        authorization_disabled=bool(args.confirm_authorization_disabled),
        single_write_plan_reviewed=bool(args.confirm_single_write_plan_reviewed),
        price_scale_confirmed=bool(args.price_scale_confirmed_by_technician),
        logical_nozzle_mapping_confirmed=bool(
            args.logical_nozzle_mapping_confirmed_by_technician
        ),
    )
    return PriceDryRunParams(
        port=args.port,
        address=args.address,
        logical_nozzle_count=args.logical_nozzle_count,
        price_nozzle_1=args.price_nozzle_1,
        price_nozzle_2=args.price_nozzle_2,
        evidence_dir=Path(args.evidence_dir),
        confirmations=confirms,
        baud=args.baud,
        response_timeout_ms=args.response_timeout_ms,
        sequence=args.sequence,
        skip_service_check=bool(args.skip_service_check),
        skip_port_check=bool(args.skip_port_check),
    )


async def _async_main(argv: list[str] | None) -> int:
    args = build_parser().parse_args(argv)
    params = _params_from_args(args)
    settings = get_settings()
    try:
        validate_price_dry_run_params(params)
        validate_price_dry_run_settings(settings)
    except PollBenchRefusedError as exc:
        print(f"PRICE_DRY_RUN_REFUSED: {exc}", file=sys.stderr)
        return 2

    # Reuse poll-bench preflight (port lock / service inactive) via thin adapter.
    from intelipump_fdc.bench_poll.guards import (
        PollBenchConfirmations,
        PollBenchParams,
    )

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
        print(f"PRICE_DRY_RUN_REFUSED: {exc}", file=sys.stderr)
        return 2

    transport = BenchPollSerialTransport(
        BenchPollSerialConfig(
            device=canonical,
            baud_rate=params.baud,
            requested_path=params.port,
            read_timeout_s=0.015,
        )
    )
    session = PriceDryRunSession(transport, params, canonical_port=canonical)
    try:
        result = await session.run()
    finally:
        if app_lock is not None:
            app_lock.release()

    if args.json:
        print(json.dumps(result.summary, indent=2))
    else:
        print(f"state: {result.state.value}")
        print(f"transmitted: {result.transmitted}")
        print(
            "serialWriteCalledForCandidate: "
            f"{result.serial_write_called_for_candidate}"
        )
        print(f"cd5PayloadHex: {result.summary.get('cd5PayloadHex')}")
        print(f"candidateFrameHex: {result.summary.get('candidateFrameHex')}")
        print(f"crc: {result.summary.get('crc')}")
        print(f"evidence: {result.summary.get('evidence')}")
        if result.summary.get("refusalReasons"):
            print(f"refusalReasons: {result.summary['refusalReasons']}")

    if result.state in {
        PriceDryRunState.FILLING_COMPLETE_EXPECTED,
        PriceDryRunState.PRICE_BLOCK_APPROVED_NOT_TRANSMITTED,
        PriceDryRunState.PRICE_BLOCK_REVIEW_PENDING,
        PriceDryRunState.PRICE_BLOCK_BUILT,
    }:
        return 0
    return 1


def run(argv: list[str] | None = None) -> int:
    return asyncio.run(_async_main(argv))


if __name__ == "__main__":
    raise SystemExit(run())
