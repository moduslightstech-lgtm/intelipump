"""Health and metrics routes."""

from __future__ import annotations

import time
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Request

from intelipump_fdc.api.dependencies import get_app_state
from intelipump_fdc.api.middleware import get_correlation_id
from intelipump_fdc.api.models.responses import (
    ControllerHealthResponse,
    ControllerMetricsResponse,
    RecoverySummary,
)
from intelipump_fdc.api.state import AppState
from intelipump_fdc.persistence.dto import PumpRecord
from intelipump_fdc.persistence.unit_of_work import unit_of_work

router = APIRouter(tags=["health"])


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


@router.get("/controller/health", response_model=ControllerHealthResponse)
async def controller_health(
    request: Request,
    state: AppState = Depends(get_app_state),
) -> ControllerHealthResponse:
    _ = get_correlation_id(request)
    warnings: list[str] = []
    database_status = "OK"
    schema_version: int | None = None
    unresolved = 0
    pending_sync = 0
    sync_age: float | None = None
    sync_delivered = 0
    sync_failed = 0
    pumps: list[PumpRecord] = []
    start = time.perf_counter()
    try:
        async with unit_of_work(state.session_factory) as uow:
            from intelipump_fdc.persistence.migrations import get_schema_version

            schema_version = await get_schema_version(uow.session)
            unresolved = len(
                await uow.transactions.list_unresolved(
                    station_id=state.settings.controller.station_id
                )
            )
            pending_sync = await uow.sync_queue.pending_count()
            sync_age = await uow.sync_queue.oldest_pending_age_seconds()
            sync_delivered = await uow.sync_queue.delivered_count()
            sync_failed = await uow.sync_queue.failed_count()
            pumps = list(
                await uow.pumps.list_for_station(state.settings.controller.station_id)
            )
    except Exception as exc:
        database_status = "ERROR"
        warnings.append(f"database: {type(exc).__name__}: {exc}")
    state.metrics.observe_db_latency((time.perf_counter() - start) * 1000)

    healthy = degraded = disconnected = 0
    async with unit_of_work(state.session_factory) as uow2:
        for pump in pumps:
            snap = await uow2.states.latest(pump.id)
            if snap is None or not snap.communication_healthy:
                disconnected += 1
            elif snap.normalized_state in {"FAULTED", "DISCOVERING"}:
                degraded += 1
            else:
                healthy += 1

    loop_running = (
        state.controller_task is not None and not state.controller_task.done()
    )
    transport_open: bool | None = None
    if state.controller_loop is not None:
        transport_open = state.controller_loop.runtime.transport.is_open

    status = "ONLINE"
    if state.shutting_down:
        status = "OFFLINE"
    elif database_status != "OK" or (disconnected and not healthy):
        status = "DEGRADED"
    if state.recovery_report and state.recovery_report.warnings:
        warnings.extend(state.recovery_report.warnings[:5])

    recovery = None
    summary = state.recovery_summary()
    if summary is not None:
        recovery = RecoverySummary(**summary)

    cloud = state.cloud.health_dict() if state.cloud is not None else {}
    if state.cloud is not None:
        sync_delivered = max(sync_delivered, int(cloud.get("syncDeliveredCount", 0)))
        sync_failed = max(sync_failed, int(cloud.get("syncFailedCount", 0)))

    return ControllerHealthResponse(
        status=status,
        environment=state.settings.environment,
        mode=state.settings.controller.mode,
        device_id=state.settings.controller.device_id,
        station_id=state.settings.controller.station_id,
        active_commands_enabled=state.settings.safety.active_commands_enabled,
        physical_enable_detected=False,
        controller_loop_running=loop_running,
        transport_open=transport_open,
        database_status=database_status,
        schema_version=schema_version,
        persistence_queue_depth=state.worker.depth if state.worker else 0,
        pending_sync_count=pending_sync,
        unresolved_transaction_count=unresolved,
        configured_pump_count=len(pumps),
        healthy_pump_count=healthy,
        degraded_pump_count=degraded,
        disconnected_pump_count=disconnected,
        last_recovery_report=recovery,
        uptime_seconds=state.uptime_seconds,
        timestamp=datetime.now(UTC),
        warnings=warnings,
        mqtt_enabled=bool(cloud.get("mqttEnabled", False)),
        mqtt_connected=bool(cloud.get("mqttConnected", False)),
        mqtt_host=cloud.get("mqttHost"),
        mqtt_last_connected_at=_parse_iso(cloud.get("mqttLastConnectedAt")),
        mqtt_last_disconnected_at=_parse_iso(cloud.get("mqttLastDisconnectedAt")),
        mqtt_reconnect_count=int(cloud.get("mqttReconnectCount", 0)),
        mqtt_last_error=cloud.get("mqttLastError"),
        sync_oldest_pending_age_seconds=sync_age,
        sync_delivered_count=sync_delivered,
        sync_failed_count=sync_failed,
        heartbeat_last_published_at=_parse_iso(cloud.get("heartbeatLastPublishedAt")),
        cloud_command_subscription_active=bool(
            cloud.get("cloudCommandSubscriptionActive", False)
        ),
    )


@router.get("/controller/metrics", response_model=ControllerMetricsResponse)
async def controller_metrics(
    state: AppState = Depends(get_app_state),
) -> ControllerMetricsResponse:
    m = state.metrics
    if state.controller_loop is not None:
        summary = state.controller_loop.summary()
        totals = summary.get("totals", {})
        if isinstance(totals, dict):
            m.poll_count = int(totals.get("poll_count", 0))
            m.data_count = int(totals.get("data_count", 0))
            m.eot_count = int(totals.get("eot_count", 0))
            m.crc_error_count = int(totals.get("crc_errors", 0))
            m.timeout_count = int(totals.get("timeouts", 0))
            m.nak_count = int(totals.get("nak_count", 0))
            m.duplicate_count = int(totals.get("duplicate_count", 0))
    cloud = state.cloud.health_dict() if state.cloud is not None else {}
    pending_sync = None
    if state.cloud is not None:
        try:
            pending_sync, _, _, _ = await state.cloud.pending_sync_snapshot()
        except Exception:
            pending_sync = None
    else:
        try:
            async with unit_of_work(state.session_factory) as uow:
                pending_sync = await uow.sync_queue.pending_count()
        except Exception:
            pending_sync = None
    return ControllerMetricsResponse(
        poll_count=m.poll_count,
        data_count=m.data_count,
        eot_count=m.eot_count,
        crc_error_count=m.crc_error_count,
        timeout_count=m.timeout_count,
        nak_count=m.nak_count,
        duplicate_count=m.duplicate_count,
        persistence_queue_depth=state.worker.depth if state.worker else 0,
        dropped_live_event_count=state.broker.dropped_noncritical,
        active_sse_subscribers=state.broker.active_sse,
        active_websocket_subscribers=state.broker.active_ws,
        database_latency_ms_last=m.database_latency_ms_last,
        database_latency_ms_max=m.database_latency_ms_max,
        uptime_seconds=state.uptime_seconds,
        mqtt_enabled=bool(cloud.get("mqttEnabled", False)),
        mqtt_connected=bool(cloud.get("mqttConnected", False)),
        mqtt_host=cloud.get("mqttHost"),
        mqtt_reconnect_count=int(cloud.get("mqttReconnectCount", 0)),
        pending_sync_count=pending_sync,
        sync_delivered_count=int(cloud.get("syncDeliveredCount", 0)),
        sync_failed_count=int(cloud.get("syncFailedCount", 0)),
        heartbeat_last_published_at=_parse_iso(cloud.get("heartbeatLastPublishedAt")),
        cloud_command_subscription_active=bool(
            cloud.get("cloudCommandSubscriptionActive", False)
        ),
    )
