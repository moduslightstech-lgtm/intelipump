"""Suppress STARTED→COMPLETED with no FILLING_UPDATE (residual-face flash).

Regression for SAO pump-3 2026-10-20: 0.08 L / ₦105 and 0.07 L / ₦100 completes
in ~9 ms with no fill ticks (same code as pump-2; hose triggered the path).
"""

from __future__ import annotations

import pytest

from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
from intelipump_fdc.services.persistence_worker import PersistenceWorker
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    FillingUpdateRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService

STATION = "SAO-Redeemed-Station-1"


def _bridge(factory, pump_id: str) -> PersistenceBridge:
    return PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="PROD",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_id},
        logical_by_address={2: "pump-3"},
        events=EventBus(),
        mqtt_pump_by_address={2: "pump-3"},
        mqtt_nozzle_by_address={2: "nozzle-2"},
        mqtt_source_by_address={2: "pump-3-n2"},
        automatic_transaction_publishing=True,
    )


def _hangup_payload(*, filled_vol: int, filled_amt: int) -> dict:
    return {
        "event": "FILLING_COMPLETED",
        "normalized_state": PumpState.FILLING_COMPLETE.value,
        "previous_state": PumpState.AUTHORIZED.value,
        "active_transaction_id": None,
        "selected_nozzle": 1,
        "communication_healthy": True,
        "state_version": 12,
        "may_publish_sale": True,
        "sale_lifecycle": "FILLING",
        "filled_volume_raw": filled_vol,
        "filled_amount_raw": filled_amt,
        "filling_price_raw": 1375,
        "unit_price_raw": 1375,
        # Sticky boot flag — must NOT authorize a zero-fill flash.
        "filling_seen_this_boot": True,
        "filling_observed": False,
        "completion_evidence_key": "complete:frame:flash",
        "awaiting_filling_complete": False,
    }


@pytest.mark.asyncio
async def test_zero_fill_flash_cancelled_not_completed(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-3", dart_address=2
        )
        pump_id = pump.id
        await TransactionService(uow).begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-flash-08",
                nozzle_id=1,
                canonical_pump_id="pump-3",
                canonical_nozzle_id="nozzle-2",
                source_identifier="pump-3-n2",
                raw_price=1375,
                price_decimals=0,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="PROD",
            )
        )

    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-flash-08"

    await bridge._handle_state_changed(
        {
            "address": 2,
            "detail": "AUTHORIZED->FILLING_COMPLETE",
            "payload": _hangup_payload(filled_vol=8, filled_amt=10500),
        }
    )

    async with unit_of_work(factory) as uow:
        row = await uow.transactions.get_by_uuid("tx-flash-08")
        assert row is not None
        assert row.status == "CANCELLED"
        assert str(row.source_completion_key or "").startswith(
            "abandoned-no-delivery:"
        )
        events = await uow.transactions.list_events(row.id)
        types = [e.event_type for e in events]
        assert "COMPLETED" not in types
        assert "CANCELLED_NO_DELIVERY" in types
        claimed = await uow.sync_queue.claim_batch(limit=50)
        completed_pub = [
            s
            for s in claimed
            if s.event_type == "TRANSACTION_COMPLETED"
            and s.entity_id == "tx-flash-08"
        ]
        assert completed_pub == []


@pytest.mark.asyncio
async def test_short_fill_with_ticks_still_completes(engine_factory: tuple) -> None:
    """0.29 L style: real FILLING_UPDATE then hang-up must still COMPLETE."""
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-3", dart_address=2
        )
        pump_id = pump.id
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-short-real",
                nozzle_id=1,
                canonical_pump_id="pump-3",
                canonical_nozzle_id="nozzle-2",
                source_identifier="pump-3-n2",
                raw_price=1375,
                price_decimals=0,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="PROD",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-short-real",
                raw_volume=12,
                raw_amount=16500,
                event_key="fill:tx-short-real:12:16500",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-short-real",
                raw_volume=29,
                raw_amount=40000,
                event_key="fill:tx-short-real:29:40000",
            )
        )

    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-short-real"
    # Verified book: lift → auth → FILLING → volume so hang-up is authoritative.
    can_pump, can_noz = "pump-3", "nozzle-2"
    bridge._verified.note_nozzle_lifted(
        pump_id=can_pump, nozzle_id=can_noz, dart_address=2
    )
    bridge._verified.note_authorized(
        pump_id=can_pump,
        nozzle_id=can_noz,
        dart_address=2,
        baseline_volume_raw=0,
    )
    bridge._verified.note_dc1_state(
        pump_id=can_pump, nozzle_id=can_noz, dc1_state="FILLING", dart_address=2
    )
    bridge._verified.note_volume(
        pump_id=can_pump,
        nozzle_id=can_noz,
        volume_raw=29,
        amount_raw=40000,
        dart_address=2,
    )

    await bridge._handle_state_changed(
        {
            "address": 2,
            "detail": "FILLING->FILLING_COMPLETE",
            "payload": {
                **_hangup_payload(filled_vol=29, filled_amt=40000),
                "previous_state": PumpState.FILLING.value,
                "filling_observed": True,
                "completion_evidence_key": "complete:frame:short",
            },
        }
    )

    async with unit_of_work(factory) as uow:
        row = await uow.transactions.get_by_uuid("tx-short-real")
        assert row is not None
        assert row.status == "COMPLETED"
        assert row.raw_volume == 29
        assert row.raw_amount == 40000
