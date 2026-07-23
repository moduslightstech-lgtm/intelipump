"""Transaction query routes."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Query

from intelipump_fdc.api.dependencies import get_app_state
from intelipump_fdc.api.errors import ApiError
from intelipump_fdc.api.models.responses import (
    TransactionEventResponse,
    TransactionListResponse,
    TransactionResponse,
)
from intelipump_fdc.api.services.query import (
    clamp_page,
    page_meta,
    to_transaction,
    to_tx_event,
)
from intelipump_fdc.api.state import AppState
from intelipump_fdc.persistence.unit_of_work import unit_of_work

router = APIRouter(tags=["transactions"])


@router.get("/transactions", response_model=TransactionListResponse)
async def list_transactions(
    state: AppState = Depends(get_app_state),
    station_id: str | None = None,
    pump_id: str | None = None,
    nozzle_id: int | None = None,
    status: str | None = None,
    simulated: bool | None = None,
    environment: str | None = None,
    started_from: datetime | None = None,
    started_to: datetime | None = None,
    completed_from: datetime | None = None,
    completed_to: datetime | None = None,
    page: int = Query(1, ge=1),
    page_size: int | None = None,
) -> TransactionListResponse:
    size = page_size or state.settings.api.default_page_size
    page, size, offset = clamp_page(page, size, state.settings.api.max_page_size)
    async with unit_of_work(state.session_factory) as uow:
        items, total = await uow.transactions.list_filtered(
            station_id=station_id or state.settings.controller.station_id,
            pump_id=pump_id,
            nozzle_id=nozzle_id,
            status=status,
            simulated=simulated,
            environment=environment,
            started_from=started_from,
            started_to=started_to,
            completed_from=completed_from,
            completed_to=completed_to,
            offset=offset,
            limit=size,
        )
        responses: list[TransactionResponse] = []
        for tx in items:
            events = await uow.transactions.list_events(tx.id)
            responses.append(to_transaction(tx, event_count=len(events)))
    return TransactionListResponse(
        items=responses, page=page_meta(page=page, page_size=size, total=total)
    )


@router.get("/transactions/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(
    transaction_id: str, state: AppState = Depends(get_app_state)
) -> TransactionResponse:
    async with unit_of_work(state.session_factory) as uow:
        tx = await uow.transactions.get_by_id(transaction_id)
        if tx is None:
            raise ApiError(
                code="TRANSACTION_NOT_FOUND",
                message=f"Transaction was not found: {transaction_id}",
                status_code=404,
            )
        events = await uow.transactions.list_events(tx.id)
        return to_transaction(tx, event_count=len(events))


@router.get(
    "/transactions/{transaction_id}/events",
    response_model=list[TransactionEventResponse],
)
async def get_transaction_events(
    transaction_id: str, state: AppState = Depends(get_app_state)
) -> list[TransactionEventResponse]:
    async with unit_of_work(state.session_factory) as uow:
        tx = await uow.transactions.get_by_id(transaction_id)
        if tx is None:
            raise ApiError(
                code="TRANSACTION_NOT_FOUND",
                message=f"Transaction was not found: {transaction_id}",
                status_code=404,
            )
        events = await uow.transactions.list_events(tx.id)
        return [to_tx_event(e) for e in events]
