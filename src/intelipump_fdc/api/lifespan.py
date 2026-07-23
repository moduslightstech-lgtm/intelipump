"""Application lifespan: shared DB, recovery, broker, optional controller/MQTT."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import structlog
from fastapi import FastAPI

from intelipump_fdc.api.state import AppState
from intelipump_fdc.cloud.runtime import CloudRuntime
from intelipump_fdc.core.config import Settings, get_settings
from intelipump_fdc.events.broker import EventBroker
from intelipump_fdc.events.models import LiveEventType
from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.migrations import init_schema
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_worker import PersistenceWorker
from intelipump_fdc.services.recovery_service import RecoveryService

logger = structlog.get_logger(__name__)


@asynccontextmanager
async def app_lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = get_settings()
    engine = create_engine(settings.database.url)
    await configure_sqlite_pragmas(engine)
    schema = await init_schema(engine)
    factory = create_session_factory(engine)
    broker = EventBroker(
        max_sse_subscribers=settings.api.max_sse_subscribers,
        max_ws_subscribers=settings.api.max_ws_subscribers,
        queue_size=settings.api.event_queue_size,
    )
    worker = PersistenceWorker(maxsize=256)
    worker.start()

    addresses = tuple(
        int(x.strip())
        for x in settings.api.controller_addresses.split(",")
        if x.strip()
    )
    recovery_svc = RecoveryService(
        engine,
        factory,
        station_id=settings.controller.station_id,
        environment=settings.environment,
    )
    pump_map = await recovery_svc.ensure_pumps(addresses or (1, 2))
    report = await recovery_svc.recover()

    async def _heartbeat_payload() -> dict[str, object]:
        pending = 0
        unresolved = 0
        healthy = degraded = disconnected = 0
        try:
            async with unit_of_work(factory) as uow:
                pending = await uow.sync_queue.pending_count()
                unresolved = len(
                    await uow.transactions.list_unresolved(
                        station_id=settings.controller.station_id
                    )
                )
                pumps = await uow.pumps.list_for_station(
                    settings.controller.station_id
                )
                for pump in pumps:
                    snap = await uow.states.latest(pump.id)
                    if snap is None or not snap.communication_healthy:
                        disconnected += 1
                    elif snap.normalized_state in {"FAULTED", "DISCOVERING"}:
                        degraded += 1
                    else:
                        healthy += 1
        except Exception:
            pass
        loop_running = False
        transport_kind = None
        transport_open = None
        app_state: AppState | None = getattr(app.state, "app_state", None)
        if app_state is not None:
            loop_running = (
                app_state.controller_task is not None
                and not app_state.controller_task.done()
            )
            if app_state.controller_loop is not None:
                meta = app_state.controller_loop.runtime.transport.metadata
                transport_kind = meta.kind
                transport_open = app_state.controller_loop.runtime.transport.is_open
        started_at = app_state.started_at if app_state else datetime.now(UTC)
        return {
            "controllerMode": settings.controller.mode.value,
            "status": "ONLINE",
            "uptimeSeconds": (datetime.now(UTC) - started_at).total_seconds(),
            "databaseStatus": "OK",
            "controllerLoopRunning": loop_running,
            "transportKind": transport_kind,
            "transportOpen": transport_open,
            "configuredPumpCount": len(pump_map),
            "healthyPumpCount": healthy,
            "degradedPumpCount": degraded,
            "disconnectedPumpCount": disconnected,
            "pendingSyncCount": pending,
            "unresolvedTransactionCount": unresolved,
        }

    cloud = CloudRuntime.create(
        settings=settings,
        session_factory=factory,
        payload_provider=_heartbeat_payload,
    )

    state = AppState(
        settings=settings,
        engine=engine,
        session_factory=factory,
        broker=broker,
        started_at=datetime.now(UTC),
        recovery_report=report,
        worker=worker,
        pump_id_by_address=pump_map,
        cloud=None,
    )
    app.state.app_state = state

    if settings.mqtt.enabled:
        await cloud.start()
        state.cloud = cloud

    broker.publish_typed(
        LiveEventType.CONTROLLER_RECOVERED,
        station_id=settings.controller.station_id,
        environment=settings.environment,
        simulated=settings.api.simulated,
        payload={"schema_version": schema, "pumps": list(report.pumps_restored)},
    )

    logger.info(
        "api_started",
        schema_version=schema,
        pumps=len(pump_map),
        start_controller_loop=settings.api.start_controller_loop,
        mqtt_enabled=settings.mqtt.enabled,
    )

    try:
        yield
    finally:
        state.shutting_down = True
        state.metrics.accepting_commands = False
        if state.cloud is not None:
            await state.cloud.stop()
        if state.controller_loop is not None:
            state.controller_loop.request_stop()
        if state.controller_task is not None:
            state.controller_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await state.controller_task
        if state.worker is not None:
            await state.worker.stop(flush=True, timeout_s=5.0)
        await broker.close_all()
        await dispose_engine(engine)
        logger.info("api_shutdown_complete")
