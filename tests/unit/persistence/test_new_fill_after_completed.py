"""A new fill after hang-up must not reuse a COMPLETED SQLite row."""

from __future__ import annotations

import pytest

from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
from intelipump_fdc.services.persistence_worker import PersistenceWorker
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService

STATION = "InteliPump-US-Lab"


@pytest.mark.asyncio
async def test_new_fill_mints_uuid_after_completed_sale(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
        await TransactionService(uow).begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-8000",
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=3,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await TransactionService(uow).complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-8000",
                source_completion_key="done:tx-8000",
                raw_volume=680,
                raw_amount=800000,
            )
        )

    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="LAB",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_id},
        logical_by_address={2: "pump-2"},
        events=EventBus(),
    )
    bridge._tx_by_address[2] = "tx-8000"

    await bridge._handle_state_changed(
        {
            "address": 2,
            "detail": "AUTHORIZED->FILLING",
            "payload": {
                "normalized_state": PumpState.FILLING.value,
                "previous_state": PumpState.AUTHORIZED.value,
                "active_transaction_id": "tx-8000",
                "selected_nozzle": 1,
                "communication_healthy": True,
                "state_version": 9,
            },
        }
    )

    async with unit_of_work(factory) as uow:
        old = await uow.transactions.get_by_uuid("tx-8000")
        assert old is not None
        assert old.status == "COMPLETED"
        assert old.raw_amount == 800000
        open_rows = await uow.transactions.list_unresolved(station_id=STATION)
        assert len(open_rows) == 1
        assert open_rows[0].transaction_uuid != "tx-8000"
        assert open_rows[0].status == "ACTIVE"
        assert bridge._tx_by_address[2] == open_rows[0].transaction_uuid
