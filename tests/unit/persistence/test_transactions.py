"""Transaction lifecycle persistence tests."""

from __future__ import annotations

import pytest

from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
    FillingUpdateRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService

STATION = "InteliPump-US-Lab"


async def _pump(factory) -> str:  # type: ignore[no-untyped-def]
    async with unit_of_work(factory) as uow:
        return (
            await uow.pumps.upsert(
                station_id=STATION, logical_pump_id="pump-1", dart_address=1
            )
        ).id


@pytest.mark.asyncio
async def test_create_update_complete_transaction(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        svc = TransactionService(uow)
        tx = await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-1",
                nozzle_id=1,
                raw_price=1999,
                price_decimals=3,
                volume_decimals=3,
                amount_decimals=2,
                simulated=True,
                environment="LAB",
            )
        )
        assert tx.status == "ACTIVE"
        updated = await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-1",
                raw_volume=1000,
                raw_amount=1999,
                event_key="fill:tx-1:1000:1999",
            )
        )
        assert updated is not None
        assert updated.raw_volume == 1000
        assert updated.raw_amount == 1999
        completed, newly = await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-1",
                source_completion_key="done:tx-1",
                raw_volume=2000,
                raw_amount=3998,
            )
        )
        assert newly is True
        assert completed.status == "COMPLETED"
        assert completed.raw_volume == 2000
        assert completed.source_completion_key == "done:tx-1"


@pytest.mark.asyncio
async def test_complete_once_and_duplicate_ignored(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-2",
                nozzle_id=1,
                raw_price=None,
                price_decimals=None,
                volume_decimals=None,
                amount_decimals=None,
                simulated=True,
                environment="LAB",
            )
        )
        _, first = await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-2",
                source_completion_key="key-a",
                raw_volume=5,
                raw_amount=10,
            )
        )
        assert first is True
        again, second = await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-2",
                source_completion_key="key-a",
                raw_volume=99,
                raw_amount=99,
            )
        )
        assert second is False
        assert again.raw_volume == 5


@pytest.mark.asyncio
async def test_transaction_event_deduplication(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        svc = TransactionService(uow)
        tx = await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-3",
                nozzle_id=None,
                raw_price=None,
                price_decimals=None,
                volume_decimals=None,
                amount_decimals=None,
                simulated=True,
                environment="LAB",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-3",
                raw_volume=1,
                raw_amount=1,
                event_key="same-key",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-3",
                raw_volume=2,
                raw_amount=2,
                event_key="same-key",
            )
        )
        events = await uow.transactions.list_events(tx.id)
        fill_events = [e for e in events if e.event_type == "FILLING_UPDATE"]
        assert len(fill_events) == 1
        # Volume still advanced even if event deduped
        refreshed = await uow.transactions.get_by_uuid("tx-3")
        assert refreshed is not None
        assert refreshed.raw_volume == 2


@pytest.mark.asyncio
async def test_raw_scaled_values_preserved(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-4",
                nozzle_id=1,
                raw_price=12345,
                price_decimals=None,  # deliberately unknown
                volume_decimals=None,
                amount_decimals=None,
                simulated=True,
                environment="LAB",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-4",
                raw_volume=67890,
                raw_amount=11111,
            )
        )
        tx = await uow.transactions.get_by_uuid("tx-4")
        assert tx is not None
        assert tx.raw_price == 12345
        assert tx.raw_volume == 67890
        assert tx.price_decimals is None


@pytest.mark.asyncio
async def test_update_filling_does_not_mutate_completed(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-done",
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=3,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-done",
                source_completion_key="done:tx-done",
                raw_volume=680,
                raw_amount=800000,
            )
        )
        again = await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-done",
                raw_volume=595,
                raw_amount=70000,
                event_key="fill:tx-done:595:70000",
            )
        )
        assert again is None
        frozen = await uow.transactions.get_by_uuid("tx-done")
        assert frozen is not None
        assert frozen.status == "COMPLETED"
        assert frozen.raw_amount == 800000


@pytest.mark.asyncio
async def test_transaction_started_queue_includes_pump_id(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid="tx-started-pump",
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="PROD",
                canonical_pump_id="pump-1",
                canonical_nozzle_id="1",
                source_identifier="pump-1",
            )
        )
        batch = await uow.sync_queue.claim_batch(limit=10)
    started = [row for row in batch if row.event_type == "TRANSACTION_STARTED"]
    assert started
    payload = started[0].payload
    assert payload.get("pumpId") == "pump-1"
    assert payload.get("pump_id") == "pump-1"
    assert payload.get("raw_volume") == 0
    assert payload.get("raw_amount") == 0


@pytest.mark.asyncio
async def test_complete_publishes_face_naira_when_price_decimals_omitted(
    engine_factory: tuple,
) -> None:
    """SAO face naira (1355) must not scale to 13.55 when price_decimals is None."""
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id="SAO-Redeemed-Station-1",
                pump_db_id=pump_id,
                transaction_uuid="tx-sao-price-1355",
                nozzle_id=1,
                raw_price=None,
                price_decimals=None,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="PRODUCTION",
                canonical_pump_id="pump-5",
                canonical_nozzle_id="1",
                source_identifier="pump-5",
            )
        )
        # Price arrives at hang-up (DC3/session), not always on begin.
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-sao-price-1355",
                source_completion_key="done:tx-sao-price-1355",
                raw_volume=74,
                raw_amount=100000,
                raw_price=1355,
                price_decimals=0,
            )
        )
        batch = await uow.sync_queue.claim_batch(limit=20)
    completed = [row for row in batch if row.event_type == "TRANSACTION_COMPLETED"]
    assert completed
    payload = completed[0].payload
    assert payload.get("raw_unit_price") == 1355
    assert payload.get("price_decimals") == 0
    assert payload.get("pricePerLiter") == "1355.00"
    assert payload.get("pricePerLitre") == "1355.00"
    assert payload.get("priceUncertain") is False
    assert payload.get("priceSource") == "pump_observed"


@pytest.mark.asyncio
async def test_complete_without_observed_price_marks_uncertain_keeps_totals(
    engine_factory: tuple,
) -> None:
    """Missing pump-observed price stays uncertain; amount/litres preserved.

    Rounded-volume estimate (100000/74→1351) is diagnostic only — never
    becomes pricePerLiter.
    """
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id="SAO-Redeemed-Station-1",
                pump_db_id=pump_id,
                transaction_uuid="tx-sao-price-uncertain",
                nozzle_id=1,
                raw_price=None,
                price_decimals=None,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="PRODUCTION",
                canonical_pump_id="pump-5",
                canonical_nozzle_id="1",
                source_identifier="pump-5",
            )
        )
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-sao-price-uncertain",
                source_completion_key="done:tx-sao-price-uncertain",
                raw_volume=74,
                raw_amount=100000,
                raw_price=None,
                price_decimals=None,
            )
        )
        sold = await uow.transactions.get_by_uuid("tx-sao-price-uncertain")
        batch = await uow.sync_queue.claim_batch(limit=20)
    assert sold is not None
    assert sold.raw_volume == 74
    assert sold.raw_amount == 100000
    assert sold.raw_price is None
    completed = [row for row in batch if row.event_type == "TRANSACTION_COMPLETED"]
    assert completed
    payload = completed[0].payload
    assert payload.get("priceUncertain") is True
    assert payload.get("pricePerLiter") is None
    assert payload.get("raw_unit_price") is None
    assert payload.get("volumeLitres") == "0.74"
    assert payload.get("amount") == "1000.00"
    assert payload.get("estimatedUnitPriceRaw") == 1351
    assert payload.get("estimatedPriceUncertain") is True

