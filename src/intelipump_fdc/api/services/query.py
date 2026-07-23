"""Query helpers and DTO→API mapping."""

from __future__ import annotations

import math
import time
from datetime import datetime

from intelipump_fdc.api.models.responses import (
    AlarmResponse,
    AuditResponse,
    PageMeta,
    PumpStatusResponse,
    TransactionEventResponse,
    TransactionResponse,
    format_scaled,
)
from intelipump_fdc.api.state import AppState
from intelipump_fdc.persistence.dto import (
    AlarmRecord,
    AuditRecord,
    PumpRecord,
    StateSnapshotRecord,
    TransactionEventRecord,
    TransactionRecord,
)
from intelipump_fdc.persistence.unit_of_work import unit_of_work


def page_meta(*, page: int, page_size: int, total: int) -> PageMeta:
    total_pages = max(1, math.ceil(total / page_size)) if page_size else 1
    return PageMeta(
        page=page, page_size=page_size, total=total, total_pages=total_pages
    )


def clamp_page(page: int, page_size: int, max_page_size: int) -> tuple[int, int, int]:
    page = max(1, page)
    page_size = min(max(1, page_size), max_page_size)
    offset = (page - 1) * page_size
    return page, page_size, offset


def to_transaction(rec: TransactionRecord, *, event_count: int = 0) -> TransactionResponse:
    return TransactionResponse(
        id=rec.id,
        transaction_uuid=rec.transaction_uuid,
        station_id=rec.station_id,
        pump_id=rec.pump_id,
        nozzle_id=rec.nozzle_id,
        status=rec.status,
        raw_price=rec.raw_price,
        price_decimals=rec.price_decimals,
        price_formatted=format_scaled(rec.raw_price, rec.price_decimals),
        raw_volume=rec.raw_volume,
        volume_decimals=rec.volume_decimals,
        volume_formatted=format_scaled(rec.raw_volume, rec.volume_decimals),
        raw_amount=rec.raw_amount,
        amount_decimals=rec.amount_decimals,
        amount_formatted=format_scaled(rec.raw_amount, rec.amount_decimals),
        started_at=rec.started_at,
        completed_at=rec.completed_at,
        closed_at=rec.closed_at,
        source_completion_key=rec.source_completion_key,
        environment=rec.environment,
        simulated=rec.simulated,
        event_count=event_count,
    )


def to_tx_event(rec: TransactionEventRecord) -> TransactionEventResponse:
    return TransactionEventResponse(
        id=rec.id,
        transaction_id=rec.transaction_id,
        event_type=rec.event_type,
        event_key=rec.event_key,
        raw_payload=rec.raw_payload,
        source_frame_ref=rec.source_frame_ref,
        observed_at=rec.observed_at,
        created_at=rec.created_at,
    )


def to_alarm(rec: AlarmRecord) -> AlarmResponse:
    return AlarmResponse(
        id=rec.id,
        station_id=rec.station_id,
        pump_id=rec.pump_id,
        severity=rec.severity,
        alarm_type=rec.alarm_type,
        message=rec.message,
        active=rec.active,
        first_seen_at=rec.first_seen_at,
        last_seen_at=rec.last_seen_at,
        cleared_at=rec.cleared_at,
        source_key=rec.source_key,
    )


def to_audit(rec: AuditRecord) -> AuditResponse:
    return AuditResponse(
        id=rec.id,
        correlation_id=rec.correlation_id,
        actor=rec.actor,
        source=rec.source,
        action=rec.action,
        station_id=rec.station_id,
        pump_id=rec.pump_id,
        previous_state=rec.previous_state,
        resulting_state=rec.resulting_state,
        result=rec.result,
        created_at=rec.created_at,
        previous_hash=rec.previous_hash,
        record_hash=rec.record_hash,
    )


def to_pump_status(
    pump: PumpRecord,
    snap: StateSnapshotRecord | None,
    *,
    environment: str,
    simulated: bool,
    session_timeouts: int = 0,
    session_retries: int = 0,
    last_error: str | None = None,
    communication_health: str | None = None,
    price_verified: bool = False,
    last_transition_at: datetime | None = None,
) -> PumpStatusResponse:
    return PumpStatusResponse(
        logical_pump_id=pump.logical_pump_id,
        pump_db_id=pump.id,
        dart_address=pump.dart_address,
        enabled=pump.enabled,
        normalized_state=snap.normalized_state if snap else "DISCONNECTED",
        previous_state=snap.previous_state if snap else None,
        communication_health=communication_health
        or (
            "HEALTHY"
            if snap and snap.communication_healthy
            else "DISCONNECTED"
        ),
        selected_nozzle=snap.selected_nozzle if snap else None,
        active_transaction_id=snap.active_transaction_id if snap else None,
        price_verified=price_verified,
        last_wayne_status=snap.raw_wayne_status if snap else None,
        state_version=snap.state_version if snap else 0,
        last_observed_at=snap.observed_at if snap else None,
        last_transition_at=last_transition_at or (snap.persisted_at if snap else None),
        timeout_count=session_timeouts,
        retry_count=session_retries,
        last_protocol_error=last_error,
        simulated=simulated,
        environment=environment,
    )


async def timed_uow(state: AppState):  # type: ignore[no-untyped-def]
    start = time.perf_counter()
    async with unit_of_work(state.session_factory) as uow:
        yield uow
    state.metrics.observe_db_latency((time.perf_counter() - start) * 1000)
