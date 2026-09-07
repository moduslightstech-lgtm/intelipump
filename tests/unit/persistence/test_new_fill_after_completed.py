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


def _bridge(factory, pump_id: str) -> PersistenceBridge:
    return PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="LAB",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_id},
        logical_by_address={2: "pump-2"},
        events=EventBus(),
    )


async def _completed_sale(factory, pump_id: str, uuid: str = "tx-600") -> None:
    async with unit_of_work(factory) as uow:
        await TransactionService(uow).begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid=uuid,
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await TransactionService(uow).complete(
            CompleteTransactionRequest(
                transaction_uuid=uuid,
                source_completion_key=f"done:{uuid}",
                raw_volume=51,
                raw_amount=60000,
            )
        )


@pytest.mark.asyncio
async def test_dc2_ticks_open_new_sale_when_mapping_is_completed(
    engine_factory: tuple,
) -> None:
    """Lab 2026-09-07: ₦750 DC2 arrived while mapping still pointed at ₦600."""
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
    await _completed_sale(factory, pump_id)
    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-600"

    await bridge._handle_app_decoded(
        {
            "address": 2,
            "is_dc2": True,
            "payload": {
                "raw_volume": 63,
                "raw_amount": 75000,
                "volume_decimals": 2,
                "amount_decimals": 2,
                "raw_price": 1175,
                "price_decimals": 2,
            },
        }
    )

    async with unit_of_work(factory) as uow:
        old = await uow.transactions.get_by_uuid("tx-600")
        assert old is not None
        assert old.status == "COMPLETED"
        assert old.raw_amount == 60000
        open_rows = await uow.transactions.list_unresolved(station_id=STATION)
        assert len(open_rows) == 1
        assert open_rows[0].transaction_uuid != "tx-600"
        assert open_rows[0].raw_amount == 75000
        assert open_rows[0].raw_volume == 63
        assert bridge._tx_by_address[2] == open_rows[0].transaction_uuid


@pytest.mark.asyncio
async def test_later_state_event_does_not_rebind_completed_uuid(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
    await _completed_sale(factory, pump_id)
    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-600"

    await bridge._handle_state_changed(
        {
            "address": 2,
            "detail": "AUTHORIZED->FILLING",
            "payload": {
                "normalized_state": PumpState.FILLING.value,
                "previous_state": PumpState.AUTHORIZED.value,
                "active_transaction_id": "tx-600",
                "selected_nozzle": 1,
                "communication_healthy": True,
                "state_version": 9,
            },
        }
    )
    minted = bridge._tx_by_address[2]
    assert minted != "tx-600"

    await bridge._handle_state_changed(
        {
            "address": 2,
            "detail": "FILLING->FILLING",
            "payload": {
                "normalized_state": PumpState.FILLING.value,
                "previous_state": PumpState.FILLING.value,
                "active_transaction_id": "tx-600",
                "selected_nozzle": 1,
                "communication_healthy": True,
                "state_version": 10,
            },
        }
    )
    assert bridge._tx_by_address[2] == minted

    await bridge._handle_app_decoded(
        {
            "address": 2,
            "is_dc2": True,
            "payload": {"raw_volume": 63, "raw_amount": 75000},
        }
    )

    async with unit_of_work(factory) as uow:
        row = await uow.transactions.get_by_uuid(minted)
        assert row is not None
        assert row.status == "ACTIVE"
        assert row.raw_amount == 75000


@pytest.mark.asyncio
async def test_hangup_completes_open_sale_not_stale_controller_uuid(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
    await _completed_sale(factory, pump_id)
    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-600"

    await bridge._handle_app_decoded(
        {
            "address": 2,
            "is_dc2": True,
            "payload": {"raw_volume": 63, "raw_amount": 75000},
        }
    )
    open_uuid = bridge._tx_by_address[2]
    assert open_uuid != "tx-600"

    bridge._tx_by_address[2] = "tx-600"

    await bridge._handle_state_changed(
        {
            "address": 2,
            "detail": "FILLING->FILLING_COMPLETE",
            "payload": {
                "normalized_state": PumpState.FILLING_COMPLETE.value,
                "previous_state": PumpState.FILLING.value,
                "active_transaction_id": "tx-600",
                "selected_nozzle": 1,
                "communication_healthy": True,
                "state_version": 11,
                "event": "FILLING_COMPLETED",
                "awaiting_filling_complete": False,
                "filled_volume_raw": 63,
                "filled_amount_raw": 75000,
                "sale_lifecycle": "FILLING_COMPLETED",
            },
        }
    )

    async with unit_of_work(factory) as uow:
        old = await uow.transactions.get_by_uuid("tx-600")
        assert old is not None
        assert old.status == "COMPLETED"
        assert old.raw_amount == 60000
        sold = await uow.transactions.get_by_uuid(open_uuid)
        assert sold is not None
        assert sold.status == "COMPLETED"
        assert sold.raw_amount == 75000
        assert sold.raw_volume == 63


@pytest.mark.asyncio
async def test_hangup_does_not_mint_after_sale_already_completed(
    engine_factory: tuple,
) -> None:
    """Sidecar settle then holster must not post a second SQLite sale."""
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
                transaction_uuid="tx-live",
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await TransactionService(uow).complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-live",
                source_completion_key="sidecar-settle:tx-live",
                raw_volume=170,
                raw_amount=200000,
            )
        )

    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-live"

    await bridge._handle_state_changed(
        {
            "address": 2,
            "detail": "FILLING->FILLING_COMPLETE",
            "payload": {
                "normalized_state": PumpState.FILLING_COMPLETE.value,
                "previous_state": PumpState.FILLING.value,
                "active_transaction_id": "tx-live",
                "selected_nozzle": 1,
                "communication_healthy": True,
                "state_version": 12,
                "event": "FILLING_COMPLETED",
                "awaiting_filling_complete": False,
                "filled_volume_raw": 170,
                "filled_amount_raw": 200000,
                "sale_lifecycle": "FILLING_COMPLETED",
            },
        }
    )

    async with unit_of_work(factory) as uow:
        assert await uow.transactions.count_completed(station_id=STATION) == 1
        assert await uow.transactions.list_unresolved(station_id=STATION) == ()
        sold = await uow.transactions.get_by_uuid("tx-live")
        assert sold is not None
        assert sold.status == "COMPLETED"
        assert sold.raw_amount == 200000
