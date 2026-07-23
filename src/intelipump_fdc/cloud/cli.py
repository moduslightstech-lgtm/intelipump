"""LAB cloud sync CLI (MQTT heartbeat + durable sync_queue delivery)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
from datetime import UTC, datetime

from intelipump_fdc.cloud.mqtt.fake import FakeMqttClient
from intelipump_fdc.cloud.runtime import CloudRuntime
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.migrations import init_schema
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.recovery_service import RecoveryService


def run(argv: list[str] | None = None) -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        prog="intelipump-cloud-sync",
        description=(
            "LAB MQTT cloud sync: heartbeats, durable sync_queue delivery, "
            "optional command intake (no production execution)."
        ),
    )
    parser.add_argument("--mqtt-host", default=settings.mqtt.host)
    parser.add_argument("--mqtt-port", type=int, default=settings.mqtt.port)
    parser.add_argument("--device-id", default=settings.controller.device_id)
    parser.add_argument("--station-id", default=settings.controller.station_id)
    parser.add_argument("--database-url", default=settings.database.url)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--log-messages", action="store_true")
    parser.add_argument("--commands-enabled", action="store_true")
    parser.add_argument("--tls", action="store_true")
    parser.add_argument(
        "--fake-mqtt",
        action="store_true",
        help="Use in-process fake MQTT (tests / offline demo)",
    )
    args = parser.parse_args(argv)
    asyncio.run(_async_main(args))


async def _async_main(args: argparse.Namespace) -> None:
    get_settings.cache_clear()
    settings = get_settings()
    # Apply CLI overrides (mutable nested settings models).
    settings.mqtt.enabled = True
    settings.mqtt.host = args.mqtt_host
    settings.mqtt.port = args.mqtt_port
    settings.mqtt.tls_enabled = bool(args.tls)
    settings.mqtt.command_subscription_enabled = bool(args.commands_enabled)
    settings.controller.device_id = args.device_id
    settings.controller.station_id = args.station_id
    settings.database.url = args.database_url

    engine = create_engine(args.database_url)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    recovery = RecoveryService(
        engine,
        factory,
        station_id=args.station_id,
        environment=settings.environment,
    )
    await recovery.ensure_pumps((1, 2))
    report = await recovery.recover()

    started = datetime.now(UTC)

    async def payload_provider() -> dict[str, object]:
        pending = 0
        unresolved = 0
        async with unit_of_work(factory) as uow:
            pending = await uow.sync_queue.pending_count()
            unresolved = len(
                await uow.transactions.list_unresolved(station_id=args.station_id)
            )
        return {
            "controllerMode": settings.controller.mode.value,
            "status": "ONLINE",
            "uptimeSeconds": (datetime.now(UTC) - started).total_seconds(),
            "databaseStatus": "OK",
            "controllerLoopRunning": False,
            "transportKind": None,
            "transportOpen": None,
            "configuredPumpCount": 2,
            "healthyPumpCount": 0,
            "degradedPumpCount": 0,
            "disconnectedPumpCount": 2,
            "pendingSyncCount": pending,
            "unresolvedTransactionCount": unresolved,
        }

    mqtt = (
        FakeMqttClient(host=f"cli-{args.mqtt_host}", client_id=args.device_id)
        if args.fake_mqtt
        else None
    )
    cloud = CloudRuntime.create(
        settings=settings,
        session_factory=factory,
        mqtt=mqtt,
        payload_provider=payload_provider,
    )
    stop = asyncio.Event()

    def _signal(*_args: object) -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal)

    await cloud.start()
    if args.log_messages and isinstance(mqtt, FakeMqttClient):
        print(json.dumps({"fake_mqtt": True, "host": args.mqtt_host}))

    try:
        await asyncio.wait_for(stop.wait(), timeout=args.duration)
    except TimeoutError:
        pass
    finally:
        await cloud.stop()
        pending, age, delivered, failed = await cloud.pending_sync_snapshot()
        stats = {
            "delivered": cloud.sync_stats.delivered,
            "failed": cloud.sync_stats.failed,
            "pending": pending,
            "oldest_pending_age_seconds": age,
            "db_delivered": delivered,
            "db_failed": failed,
            "online_published": cloud.online_published,
            "heartbeat_last_published_at": (
                cloud.heartbeat.last_published_at.isoformat()
                if cloud.heartbeat and cloud.heartbeat.last_published_at
                else None
            ),
            "recovery_warnings": list(report.warnings)[:10],
            "mqtt_connected": cloud.mqtt.is_connected,
        }
        print(json.dumps(stats, indent=2))
        await dispose_engine(engine)
        get_settings.cache_clear()
