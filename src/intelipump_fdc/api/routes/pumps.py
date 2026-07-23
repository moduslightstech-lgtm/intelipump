"""Pump query routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from intelipump_fdc.api.dependencies import get_app_state
from intelipump_fdc.api.errors import ApiError
from intelipump_fdc.api.models.responses import PumpStatusResponse, PumpTotalsResponse
from intelipump_fdc.api.services.query import to_pump_status
from intelipump_fdc.api.state import AppState
from intelipump_fdc.persistence.dto import PumpRecord, StateSnapshotRecord
from intelipump_fdc.persistence.unit_of_work import unit_of_work

router = APIRouter(tags=["pumps"])


async def _resolve_pump(
    state: AppState, pump_id: str
) -> tuple[PumpRecord, StateSnapshotRecord | None]:
    async with unit_of_work(state.session_factory) as uow:
        pump = await uow.pumps.get_by_id(pump_id)
        if pump is None:
            # allow logical id
            for p in await uow.pumps.list_for_station(state.settings.controller.station_id):
                if p.logical_pump_id == pump_id or str(p.dart_address) == pump_id:
                    pump = p
                    break
        if pump is None:
            raise ApiError(
                code="PUMP_NOT_FOUND",
                message=f"Pump was not found: {pump_id}",
                status_code=404,
            )
        snap = await uow.states.latest(pump.id)
        return pump, snap


@router.get("/pumps", response_model=list[PumpStatusResponse])
async def list_pumps(state: AppState = Depends(get_app_state)) -> list[PumpStatusResponse]:
    out: list[PumpStatusResponse] = []
    async with unit_of_work(state.session_factory) as uow:
        pumps = await uow.pumps.list_for_station(state.settings.controller.station_id)
        for pump in pumps:
            snap = await uow.states.latest(pump.id)
            timeouts = retries = 0
            last_error = None
            comm = None
            price_verified = False
            if state.controller_loop is not None:
                session = state.controller_loop.sessions.get(pump.dart_address)
                if session is not None:
                    timeouts = session.state.stats.timeout_count
                    retries = session.state.consecutive_timeouts
                    last_error = session.state.last_error
                    comm = session.state.communication.value
                    price_verified = session.machine.context.price_verified
            out.append(
                to_pump_status(
                    pump,
                    snap,
                    environment=state.settings.environment,
                    simulated=state.settings.api.simulated,
                    session_timeouts=timeouts,
                    session_retries=retries,
                    last_error=last_error,
                    communication_health=comm,
                    price_verified=price_verified,
                )
            )
    return out


@router.get("/pumps/{pump_id}", response_model=PumpStatusResponse)
@router.get("/pumps/{pump_id}/status", response_model=PumpStatusResponse)
async def get_pump(
    pump_id: str, state: AppState = Depends(get_app_state)
) -> PumpStatusResponse:
    pump, snap = await _resolve_pump(state, pump_id)
    timeouts = retries = 0
    last_error = None
    comm = None
    price_verified = False
    if state.controller_loop is not None:
        session = state.controller_loop.sessions.get(pump.dart_address)
        if session is not None:
            timeouts = session.state.stats.timeout_count
            retries = session.state.consecutive_timeouts
            last_error = session.state.last_error
            comm = session.state.communication.value
            price_verified = session.machine.context.price_verified
    return to_pump_status(
        pump,
        snap,
        environment=state.settings.environment,
        simulated=state.settings.api.simulated,
        session_timeouts=timeouts,
        session_retries=retries,
        last_error=last_error,
        communication_health=comm,
        price_verified=price_verified,
    )


@router.get("/pumps/{pump_id}/totals", response_model=PumpTotalsResponse)
async def get_pump_totals(
    pump_id: str, state: AppState = Depends(get_app_state)
) -> PumpTotalsResponse:
    pump, _snap = await _resolve_pump(state, pump_id)
    # Totals from latest completed/active transaction when present.
    async with unit_of_work(state.session_factory) as uow:
        items, _ = await uow.transactions.list_filtered(
            station_id=state.settings.controller.station_id,
            pump_id=pump.id,
            limit=1,
            offset=0,
        )
    notes = [
        "Totals reflect the most recent transaction raw scaled values.",
        "Decimal formatting omitted when pump decimal metadata is unknown.",
    ]
    if not items:
        return PumpTotalsResponse(logical_pump_id=pump.logical_pump_id, notes=notes)
    tx = items[0]
    from intelipump_fdc.api.models.responses import format_scaled

    return PumpTotalsResponse(
        logical_pump_id=pump.logical_pump_id,
        raw_volume=tx.raw_volume,
        volume_decimals=tx.volume_decimals,
        volume_formatted=format_scaled(tx.raw_volume, tx.volume_decimals),
        raw_amount=tx.raw_amount,
        amount_decimals=tx.amount_decimals,
        amount_formatted=format_scaled(tx.raw_amount, tx.amount_decimals),
        notes=notes,
    )


@router.get("/pumps/{pump_id}/events")
async def get_pump_events(
    pump_id: str,
    state: AppState = Depends(get_app_state),
    limit: int = 50,
) -> list[dict[str, object]]:
    pump, _ = await _resolve_pump(state, pump_id)
    # Historical: recent transaction events for this pump.
    async with unit_of_work(state.session_factory) as uow:
        txs, _ = await uow.transactions.list_filtered(
            station_id=state.settings.controller.station_id,
            pump_id=pump.id,
            limit=min(limit, state.settings.api.max_page_size),
            offset=0,
        )
        events: list[dict[str, object]] = []
        for tx in txs:
            for ev in await uow.transactions.list_events(tx.id):
                events.append(
                    {
                        "transaction_uuid": tx.transaction_uuid,
                        "event_type": ev.event_type,
                        "event_key": ev.event_key,
                        "created_at": ev.created_at.isoformat(),
                    }
                )
                if len(events) >= limit:
                    return events
    return events
