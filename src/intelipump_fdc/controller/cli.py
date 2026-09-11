"""LAB controller CLI (LISTEN_ONLY polling over virtual serial)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal

from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.feature_flags import WayneFeatureFlags
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.core.config import ControllerMode, get_settings
from intelipump_fdc.core.liveness import LivenessTracker
from intelipump_fdc.core.systemd_notify import SystemdNotifier
from intelipump_fdc.persistence.database import create_engine, dispose_engine
from intelipump_fdc.persistence.errors import SchemaError
from intelipump_fdc.persistence.migrations import reset_lab_database
from intelipump_fdc.protocol.dart.transport.serial import (
    SerialConfig,
    SerialParity,
    SerialTransport,
)
from intelipump_fdc.services.lab_persistence import (
    apply_recovered_contexts,
    start_persistence,
)
from intelipump_fdc.services.recovery_service import format_recovery_report


def build_parser() -> argparse.ArgumentParser:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        prog="intelipump-controller",
        description="LAB DART controller (LISTEN_ONLY by default; no field commands).",
    )
    parser.add_argument("--port", default="/tmp/dart-controller")
    parser.add_argument("--addresses", default="1,2")
    parser.add_argument("--baud", type=int, default=9600)
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Optional run duration in seconds. Omit to run continuously.",
    )
    parser.add_argument(
        "--log-frames",
        action="store_true",
        help=(
            "Deprecated: kept for compatibility. Idle POLL/EOT/raw bytes are "
            "not printed. Use --log-all-frames for a full bus dump."
        ),
    )
    parser.add_argument(
        "--log-all-frames",
        action="store_true",
        help="Print every POLL, EOT, RX raw chunk, and TX/RX timing.",
    )
    parser.add_argument(
        "--log-dart-timing",
        action="store_true",
        help="With --log-all-frames, also print TX complete timestamps.",
    )
    parser.add_argument(
        "--mode",
        default=ControllerMode.LISTEN_ONLY.value,
        choices=[m.value for m in ControllerMode],
    )
    parser.add_argument(
        "--response-timeout-ms",
        type=int,
        default=120,
        help="Software poll/command response deadline in ms (default 120)",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--database-url",
        default=settings.database.url,
        help="SQLAlchemy async SQLite URL",
    )
    parser.add_argument(
        "--reset-lab-database",
        action="store_true",
        help="Drop and recreate LAB database (requires --yes)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm destructive LAB database reset",
    )
    parser.add_argument(
        "--show-recovery-report",
        action="store_true",
        help="Print recovery report after startup (and after run)",
    )
    parser.add_argument(
        "--no-persistence",
        action="store_true",
        help="Disable SQLite persistence (Phase 6-compatible)",
    )
    parser.add_argument(
        "--confirm-owned-lab-dispense-session",
        action="store_true",
        help=(
            "OWNED LAB ONLY: enable CD5 price and RESET. "
            "AUTHORIZE-on-lift is OFF by default (phantom-flow experiment); "
            "pass --authorize-on-nozzle-lift to restore the old behavior. "
            "Requires --mode BENCH_CONTROL, --price, and the other confirm flags. "
            "Default remains poll-and-observe / LISTEN_ONLY."
        ),
    )
    parser.add_argument(
        "--authorize-on-nozzle-lift",
        action="store_true",
        help=(
            "EXPERIMENT REVERT: with --confirm-owned-lab-dispense-session, "
            "auto-AUTHORIZE when the nozzle is lifted (pre-2026-09-11 behavior). "
            "Without this flag, lift alone does not enable delivery; touch "
            "/var/lib/intelipump/authorize-<addr> while nozzle is OUT to AUTHORIZE."
        ),
    )
    parser.add_argument(
        "--confirm-physical-control-enable",
        action="store_true",
        help="Operator confirms the physical control-enable signal is present.",
    )
    parser.add_argument(
        "--enable-active-commands",
        action="store_true",
        help="Allow CD5/RESET/AUTHORIZE for this process only (not persisted).",
    )
    parser.add_argument(
        "--price",
        type=int,
        default=None,
        help="Startup unit price as raw BCD digits (e.g. 1175 for 00 11 75).",
    )
    parser.add_argument(
        "--logical-nozzle-count",
        type=int,
        default=1,
        choices=(1, 2),
        help="CD5 nozzle count (default 1).",
    )
    parser.add_argument(
        "--confirm-logical-nozzle-mapping",
        action="store_true",
        help="Technician confirms PRI1 is logical nozzle 1.",
    )
    parser.add_argument(
        "--confirm-price-scale-raw-bcd",
        action="store_true",
        help="Confirm --price is raw BCD digits (not inferred decimals).",
    )
    return parser


def resolve_duration(duration: float | None) -> float | None:
    """Map CLI ``--duration`` to controller loop seconds.

    ``None`` means run continuously until SIGTERM/SIGINT / ``request_stop``.
    Positive values are finite run times. ``0`` and negatives are rejected.
    """
    if duration is None:
        return None
    if duration < 0:
        raise ValueError("--duration must not be negative")
    if duration == 0:
        raise ValueError(
            "--duration 0 is not allowed; omit --duration to run continuously"
        )
    return duration


def _owned_lab_dispense_or_exit(args: argparse.Namespace, settings) -> None:
    """Refuse incomplete owned-lab active sessions; keep LISTEN_ONLY default."""
    if not args.confirm_owned_lab_dispense_session:
        if args.enable_active_commands or args.confirm_physical_control_enable:
            raise SystemExit(
                "active-command flags require --confirm-owned-lab-dispense-session"
            )
        return
    missing: list[str] = []
    if ControllerMode(args.mode) is not ControllerMode.BENCH_CONTROL:
        missing.append("--mode BENCH_CONTROL")
    if settings.environment.upper() != "LAB":
        missing.append("INTELIPUMP_ENVIRONMENT=LAB")
    if not args.enable_active_commands:
        missing.append("--enable-active-commands")
    if not args.confirm_physical_control_enable:
        missing.append("--confirm-physical-control-enable")
    if args.price is None:
        missing.append("--price N")
    if not args.confirm_logical_nozzle_mapping:
        missing.append("--confirm-logical-nozzle-mapping")
    if not args.confirm_price_scale_raw_bcd:
        missing.append("--confirm-price-scale-raw-bcd")
    if missing:
        raise SystemExit(
            "owned-lab dispense session refused; missing: " + ", ".join(missing)
        )


def run(argv: list[str] | None = None) -> None:
    settings = get_settings()
    parser = build_parser()
    args = parser.parse_args(argv)

    addresses = tuple(int(x.strip()) for x in args.addresses.split(",") if x.strip())
    if not addresses:
        raise SystemExit("at least one address required")

    try:
        duration_s = resolve_duration(args.duration)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    mode = ControllerMode(args.mode)
    if mode is ControllerMode.FIELD_CONTROL:
        raise SystemExit("FIELD_CONTROL is not permitted")
    _owned_lab_dispense_or_exit(args, settings)

    if args.reset_lab_database:
        if settings.environment.upper() != "LAB":
            raise SystemExit("--reset-lab-database rejected: environment is not LAB")
        if not args.yes:
            raise SystemExit("--reset-lab-database requires --yes")

        async def _reset() -> None:
            engine = create_engine(args.database_url)
            try:
                await reset_lab_database(engine, environment=settings.environment)
            except SchemaError as exc:
                raise SystemExit(str(exc)) from exc
            finally:
                await dispose_engine(engine)

        asyncio.run(_reset())
        print(f"LAB database reset: {args.database_url}")

    async def _main() -> None:
        owned = bool(args.confirm_owned_lab_dispense_session)
        safety = ControllerSafetyContext(
            environment=settings.environment,
            mode=mode,
            active_commands_enabled=bool(args.enable_active_commands) and owned,
            require_physical_control_enable=True,
            physical_enable_present=bool(args.confirm_physical_control_enable)
            and owned,
            allow_virtual_polling=True,
            owned_lab_active_session=owned,
        )
        # Keep default LAB helper available for tests; CLI always overrides.
        _ = default_lab_safety
        notifier = SystemdNotifier.from_env(enabled=settings.watchdog.enabled)
        liveness = LivenessTracker(
            controller_mode=mode.value,
            watchdog_enabled=notifier.enabled,
            notify_socket_present=notifier.notify_socket_present,
            database_health="disabled" if args.no_persistence else "unknown",
            serial_device_status="closed",
        )
        transport = SerialTransport(
            SerialConfig(
                device=args.port,
                baud_rate=args.baud,
                parity=SerialParity.ODD,
            )
        )
        runtime = ControllerRuntime(
            transport=transport,
            safety=safety,
            config=PollSchedulerConfig(
                addresses=addresses,
                response_timeout_ms=args.response_timeout_ms,
                awaiting_filling_complete_timeout_s=(
                    settings.controller.awaiting_filling_complete_timeout_s
                ),
                dc2_stability_window_s=settings.controller.dc2_stability_window_s,
            ),
            log_frames=args.log_frames,
            log_dart_timing=args.log_dart_timing,
            log_all_frames=args.log_all_frames,
            # EXPERIMENT 2026-09-11 (phantom flow): do NOT auto-AUTHORIZE on
            # nozzle lift. Lift alone was enabling Wayne delivery and the face
            # climbed without intentional squeeze.
            # REVERT: pass --authorize-on-nozzle-lift (or set
            # automatic_authorization=owned below again).
            feature_flags=WayneFeatureFlags(
                poll_and_observe=not owned,
                automatic_startup_price_programming=owned,
                automatic_reset=owned,
                automatic_authorization=bool(
                    owned and args.authorize_on_nozzle_lift
                ),
                automatic_transaction_publishing=False,
            ),
            liveness=liveness,
            notifier=notifier,
            status_interval_s=settings.watchdog.status_interval_s,
            startup_unit_price=args.price if owned else None,
            logical_nozzle_count=args.logical_nozzle_count,
        )
        loop_ctrl = ControllerLoop(runtime)
        persistence = None
        if not args.no_persistence:
            persistence = await start_persistence(
                database_url=args.database_url,
                station_id=settings.controller.station_id,
                environment=settings.environment,
                addresses=addresses,
                events=runtime.events,
                simulated=True,
            )
            apply_recovered_contexts(loop_ctrl, persistence.recovery.pump_contexts)
            liveness.database_health = "ok"
            if args.show_recovery_report:
                print("--- recovery report ---")
                if args.json:
                    print(json.dumps(persistence.recovery.to_dict(), indent=2))
                else:
                    print(format_recovery_report(persistence.recovery))
        else:
            liveness.database_health = "disabled"

        # Serial runtime initialized before READY (open is idempotent in run()).
        try:
            await transport.open()
            liveness.serial_device_status = "open"
        except Exception as exc:
            liveness.serial_device_status = "error"
            print(f"serial open deferred: {type(exc).__name__}:{exc}")

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, loop_ctrl.request_stop)

        duration_label = "continuous" if duration_s is None else f"{duration_s}s"
        print(
            f"controller port={args.port} addresses={addresses} "
            f"mode={mode.value} duration={duration_label} "
            f"db={args.database_url if not args.no_persistence else 'disabled'} "
            f"watchdog={notifier.enabled}"
        )
        if owned:
            print(
                "[OWNED-LAB] dispense session ON: CD5 "
                f"price={args.price} hold-display-until-lift=yes "
                "RESET-on-next-lift=yes AUTHORIZE-on-lift=yes "
                "(this process only; not persisted)"
            )
        # READY only after config, safety, DB recovery, and serial runtime init.
        notifier.ready(
            status=(
                f"mode={mode.value} serial={liveness.serial_device_status} "
                f"db={liveness.database_health}"
            )
        )
        try:
            await loop_ctrl.run(duration_s=duration_s)
        finally:
            notifier.stopping()
            # Graceful shutdown: flush persistence; transport closed by run().
            if persistence is not None:
                await persistence.shutdown()

        summary = loop_ctrl.summary()
        if persistence is not None:
            summary["persistence"] = {
                "database_url": args.database_url,
                "schema_version": persistence.recovery.schema_version,
                "queue_depth": persistence.worker.depth,
                "queue_processed": persistence.worker.processed,
                "queue_dropped_normal": persistence.worker.dropped_normal,
                "unresolved_transactions": list(
                    persistence.recovery.unresolved_transactions
                ),
                "recovery_warnings": list(persistence.recovery.warnings),
            }
        if args.json:
            print(json.dumps(summary, indent=2))
        else:
            t = summary["totals"]
            assert isinstance(t, dict)
            print(
                "stats: "
                f"polls={t['poll_count']} eot={t['eot_count']} data={t['data_count']} "
                f"crc={t['crc_errors']} timeouts={t['timeouts']} "
                f"nak={t['nak_count']} dup={t['duplicate_count']}"
            )
            pumps = summary["pumps"]
            assert isinstance(pumps, dict)
            for addr, info in pumps.items():
                assert isinstance(info, dict)
                print(
                    f"  pump {addr}: comm={info.get('communication')} "
                    f"sync={info.get('state_synchronized')} "
                    f"dc1={info.get('observed_status')} "
                    f"nozio={info.get('nozzle_position')} "
                    f"sale={info.get('sale_lifecycle')} "
                    f"vol={info.get('filled_volume_raw')} "
                    f"amt={info.get('filled_amount_raw')} "
                    f"price={info.get('unit_price_raw')} "
                    f"completed={info.get('last_completed_sale')} "
                    f"err={info.get('last_error')}"
                )
            if persistence is not None and args.show_recovery_report:
                print(
                    f"persistence: schema={persistence.recovery.schema_version} "
                    f"processed={persistence.worker.processed} "
                    f"dropped_normal={persistence.worker.dropped_normal}"
                )

    asyncio.run(_main())


if __name__ == "__main__":
    run()
