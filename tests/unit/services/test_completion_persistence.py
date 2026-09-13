"""Persistence / publish idempotency for completion lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.lab_persistence import start_persistence

STATION = "InteliPump-US-Lab"


async def _flush() -> None:
    await asyncio.sleep(0.4)


def _seed_verified_ready(bridge, *, address: int = 1, tx: str | None = None) -> None:
    """Lift + authorize + baseline so FILLING+volume can open a verified sale."""
    can_pump, can_nozzle, _ = bridge._channel_identity(address)
    bridge._verified.note_nozzle_lifted(
        pump_id=can_pump, nozzle_id=can_nozzle, dart_address=address
    )
    bridge._verified.note_authorized(
        pump_id=can_pump,
        nozzle_id=can_nozzle,
        dart_address=address,
        baseline_volume_raw=0,
    )
    if tx:
        state = bridge._verified.get_or_create(
            pump_id=can_pump, nozzle_id=can_nozzle, dart_address=address
        )
        state.transaction_id = tx


async def _open_verified_sale(
    events: EventBus,
    bridge,
    *,
    address: int = 1,
    tx: str,
) -> str:
    _seed_verified_ready(bridge, address=address, tx=tx)
    events.publish(
        ControllerEvent(
            type=ControllerEventType.STATE_CHANGED,
            address=address,
            detail="AUTHORIZED->FILLING",
            payload={
                "event": "FILLING_STARTED",
                "previous_state": "AUTHORIZED",
                "normalized_state": "FILLING",
                "state_version": 2,
                "selected_nozzle": 1,
                "active_transaction_id": tx,
                "communication_healthy": True,
            },
        )
    )
    await _flush()
    events.publish(
        ControllerEvent(
            type=ControllerEventType.APPLICATION_TRANSACTION_DECODED,
            address=address,
            detail="DC2",
            payload={
                "raw_volume": 25,
                "raw_amount": 30000,
                "volume_decimals": 2,
                "amount_decimals": 2,
                "raw_price": 1175,
                "price_decimals": 2,
                "selected_nozzle": 1,
            },
        )
    )
    await _flush()
    opened = bridge._tx_by_address.get(address) or tx
    return opened


@pytest.mark.asyncio
async def test_hangup_awaits_then_confirmed_completes_once(tmp_path: Path) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'comp.db'}"
    events = EventBus()
    persistence = await start_persistence(
        database_url=db,
        station_id=STATION,
        environment="LAB",
        addresses=(1,),
        events=events,
    )
    try:
        sale_id = await _open_verified_sale(
            events, persistence.bridge, address=1, tx="sale-await"
        )

        events.publish(
            ControllerEvent(
                type=ControllerEventType.STATE_CHANGED,
                address=1,
                detail="FILLING->FILLING_COMPLETE",
                payload={
                    "event": "NOZZLE_RETURNED",
                    "previous_state": "FILLING",
                    "normalized_state": "FILLING_COMPLETE",
                    "state_version": 3,
                    "selected_nozzle": 1,
                    "active_transaction_id": sale_id,
                    "communication_healthy": True,
                    "awaiting_filling_complete": True,
                    "completion_evidence_key": "hang:1",
                },
            )
        )
        await _flush()

        async with unit_of_work(persistence.session_factory) as uow:
            tx = await uow.transactions.get_by_uuid(sale_id)
            assert tx is not None
            assert tx.status != "COMPLETED"
            assert await uow.transactions.count_completed(station_id=STATION) == 0
            audits = await uow.audit.list_all()
            assert any(a.action == "AWAITING_FILLING_COMPLETE" for a in audits)

        events.publish(
            ControllerEvent(
                type=ControllerEventType.STATE_CHANGED,
                address=1,
                detail="FILLING_COMPLETE->FILLING_COMPLETE",
                payload={
                    "event": "FILLING_COMPLETED",
                    "previous_state": "FILLING_COMPLETE",
                    "normalized_state": "FILLING_COMPLETE",
                    "state_version": 4,
                    "active_transaction_id": sale_id,
                    "communication_healthy": True,
                    "awaiting_filling_complete": False,
                    "completion_inferred": False,
                    "completion_evidence_key": "complete:frame:5",
                },
            )
        )
        await _flush()

        events.publish(
            ControllerEvent(
                type=ControllerEventType.STATE_CHANGED,
                address=1,
                detail="FILLING_COMPLETE->FILLING_COMPLETE",
                payload={
                    "event": "FILLING_COMPLETED",
                    "previous_state": "FILLING_COMPLETE",
                    "normalized_state": "FILLING_COMPLETE",
                    "state_version": 5,
                    "active_transaction_id": sale_id,
                    "awaiting_filling_complete": False,
                    "completion_evidence_key": "complete:frame:5",
                },
            )
        )
        await _flush()

        async with unit_of_work(persistence.session_factory) as uow:
            assert await uow.transactions.count_completed(station_id=STATION) == 1
            tx = await uow.transactions.get_by_uuid(sale_id)
            assert tx is not None
            assert tx.source_completion_key == "complete:frame:5"
            evs = await uow.transactions.list_events(tx.id)
            completed = [e for e in evs if e.event_type == "COMPLETED"]
            assert len(completed) == 1
            assert completed[0].raw_payload is not None
            assert completed[0].raw_payload.get("completion_inferred") is False
            audits = await uow.audit.list_all()
            assert any(a.action == "TRANSACTION_COMPLETED" for a in audits)
            pending = await uow.sync_queue.pending_count()
            assert pending >= 1
            # Duplicate completion must not enqueue a second MQTT payload.
            claimed = await uow.sync_queue.claim_batch(limit=50)
            completed_sync = [
                s for s in claimed if s.event_type == "TRANSACTION_COMPLETED"
            ]
            assert len(completed_sync) == 1
            assert completed_sync[0].deduplication_key == (
                f"tx-completed:{STATION}:complete:frame:5"
            )
    finally:
        await persistence.shutdown()


@pytest.mark.asyncio
async def test_inferred_completion_distinguishable(tmp_path: Path) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'inf.db'}"
    events = EventBus()
    persistence = await start_persistence(
        database_url=db,
        station_id=STATION,
        environment="LAB",
        addresses=(1,),
        events=events,
    )
    try:
        sale_id = await _open_verified_sale(
            events, persistence.bridge, address=1, tx="sale-inf"
        )
        events.publish(
            ControllerEvent(
                type=ControllerEventType.STATE_CHANGED,
                address=1,
                detail="FILLING_COMPLETE->FILLING_COMPLETE",
                payload={
                    "event": "FILLING_COMPLETED",
                    "previous_state": "FILLING_COMPLETE",
                    "normalized_state": "FILLING_COMPLETE",
                    "state_version": 2,
                    "active_transaction_id": sale_id,
                    "awaiting_filling_complete": False,
                    "completion_inferred": True,
                    "completion_evidence_key": "inferred:sale-inf:hangup-timeout",
                    "warnings": [
                        "INFERRED: DC1 FILLING_COMPLETED missing after hang-up timeout"
                    ],
                },
            )
        )
        await _flush()
        async with unit_of_work(persistence.session_factory) as uow:
            tx = await uow.transactions.get_by_uuid(sale_id)
            assert tx is not None
            assert tx.status == "COMPLETED"
            evs = await uow.transactions.list_events(tx.id)
            completed = [e for e in evs if e.event_type == "COMPLETED"]
            assert len(completed) == 1
            assert completed[0].raw_payload is not None
            assert completed[0].raw_payload.get("completion_inferred") is True
            audits = await uow.audit.list_all()
            inferred = [
                a for a in audits if a.action == "TRANSACTION_COMPLETED_INFERRED"
            ]
            assert len(inferred) == 1
            assert inferred[0].details is not None
            assert inferred[0].details.get("completion_inferred") is True
            assert any(
                "INFERRED" in str(w)
                for w in (inferred[0].details.get("warnings") or [])
            )
    finally:
        await persistence.shutdown()


@pytest.mark.asyncio
async def test_restart_already_completed_does_not_republish(tmp_path: Path) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'done.db'}"
    events = EventBus()
    persistence = await start_persistence(
        database_url=db,
        station_id=STATION,
        environment="LAB",
        addresses=(1,),
        events=events,
    )
    try:
        sale_id = await _open_verified_sale(
            events, persistence.bridge, address=1, tx="sale-done"
        )
        payload = {
            "event": "FILLING_COMPLETED",
            "previous_state": "FILLING",
            "normalized_state": "FILLING_COMPLETE",
            "state_version": 2,
            "active_transaction_id": sale_id,
            "awaiting_filling_complete": False,
            "completion_evidence_key": "complete:sale-done",
        }
        events.publish(
            ControllerEvent(
                type=ControllerEventType.STATE_CHANGED,
                address=1,
                detail="FILLING->FILLING_COMPLETE",
                payload=payload,
            )
        )
        await _flush()
        payload2 = dict(payload)
        payload2["state_version"] = 3
        events.publish(
            ControllerEvent(
                type=ControllerEventType.STATE_CHANGED,
                address=1,
                detail="FILLING_COMPLETE->FILLING_COMPLETE",
                payload=payload2,
            )
        )
        await _flush()
        async with unit_of_work(persistence.session_factory) as uow:
            assert await uow.transactions.count_completed(station_id=STATION) == 1
            claimed = await uow.sync_queue.claim_batch(limit=50)
            completed_sync = [
                s for s in claimed if s.event_type == "TRANSACTION_COMPLETED"
            ]
            assert len(completed_sync) == 1
    finally:
        await persistence.shutdown()
