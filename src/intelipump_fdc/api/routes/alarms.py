"""Alarm and audit read routes."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query

from intelipump_fdc.api.dependencies import get_app_state
from intelipump_fdc.api.errors import ApiError
from intelipump_fdc.api.models.responses import (
    AlarmListResponse,
    AlarmResponse,
    AuditListResponse,
    AuditVerifyResponse,
)
from intelipump_fdc.api.services.query import clamp_page, page_meta, to_alarm, to_audit
from intelipump_fdc.api.state import AppState
from intelipump_fdc.persistence.unit_of_work import unit_of_work

alarms_router = APIRouter(tags=["alarms"])
audit_router = APIRouter(tags=["audit"])


@alarms_router.get("/alarms", response_model=AlarmListResponse)
async def list_alarms(
    state: AppState = Depends(get_app_state),
    pump_id: str | None = None,
    severity: str | None = None,
    active: bool | None = None,
    alarm_type: str | None = None,
    first_seen_from: datetime | None = None,
    first_seen_to: datetime | None = None,
    page: int = Query(1, ge=1),
    page_size: int | None = None,
) -> AlarmListResponse:
    size = page_size or state.settings.api.default_page_size
    page, size, offset = clamp_page(page, size, state.settings.api.max_page_size)
    async with unit_of_work(state.session_factory) as uow:
        items, total = await uow.alarms.list_filtered(
            station_id=state.settings.controller.station_id,
            pump_id=pump_id,
            severity=severity,
            active=active,
            alarm_type=alarm_type,
            first_seen_from=first_seen_from,
            first_seen_to=first_seen_to,
            offset=offset,
            limit=size,
        )
    return AlarmListResponse(
        items=[to_alarm(a) for a in items],
        page=page_meta(page=page, page_size=size, total=total),
    )


@alarms_router.get("/alarms/{alarm_id}", response_model=AlarmResponse)
async def get_alarm(
    alarm_id: str, state: AppState = Depends(get_app_state)
) -> AlarmResponse:
    async with unit_of_work(state.session_factory) as uow:
        alarm = await uow.alarms.get(alarm_id)
        if alarm is None:
            raise ApiError(
                code="ALARM_NOT_FOUND",
                message=f"Alarm was not found: {alarm_id}",
                status_code=404,
            )
        return to_alarm(alarm)


@audit_router.get("/audit", response_model=AuditListResponse)
async def list_audit(
    state: AppState = Depends(get_app_state),
    correlation_id: str | None = None,
    pump_id: str | None = None,
    action: str | None = None,
    result: str | None = None,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
    page: int = Query(1, ge=1),
    page_size: int | None = None,
) -> AuditListResponse:
    size = page_size or state.settings.api.default_page_size
    page, size, offset = clamp_page(page, size, state.settings.api.max_page_size)
    async with unit_of_work(state.session_factory) as uow:
        items, total = await uow.audit.list_filtered(
            station_id=state.settings.controller.station_id,
            correlation_id=correlation_id,
            pump_id=pump_id,
            action=action,
            result_value=result,
            created_from=created_from,
            created_to=created_to,
            offset=offset,
            limit=size,
        )
    return AuditListResponse(
        items=[to_audit(a) for a in items],
        page=page_meta(page=page, page_size=size, total=total),
    )


@audit_router.get("/audit/verify", response_model=AuditVerifyResponse)
async def verify_audit(
    state: AppState = Depends(get_app_state),
) -> AuditVerifyResponse:
    async with unit_of_work(state.session_factory) as uow:
        report = await uow.audit.verify_chain_report()
    invalid = report.get("first_invalid_record_id")
    reason = report.get("reason")
    checked = report["records_checked"]
    return AuditVerifyResponse(
        valid=bool(report["valid"]),
        records_checked=checked if isinstance(checked, int) else int(str(checked)),
        first_invalid_record_id=str(invalid) if invalid else None,
        genesis=str(report["genesis"]),
        reason=str(reason) if reason else None,
    )
