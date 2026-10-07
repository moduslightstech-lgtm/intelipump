"""ACK validation: event-type gate and volumeLiters=0 preservation."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.delivery import DeliveryMapper
from intelipump_fdc.cloud.mqtt.fake import FakeMqttClient
from intelipump_fdc.cloud.sync_worker import SyncWorker
from intelipump_fdc.cloud.topics import TopicBuilder
from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.migrations import init_schema
from intelipump_fdc.persistence.unit_of_work import unit_of_work


@pytest.fixture
async def db_factory(tmp_path: Path) -> async_sessionmaker[AsyncSession]:
    engine = create_engine(f"sqlite+aiosqlite:///{tmp_path / 'ack-val.db'}")
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    yield factory
    await dispose_engine(engine)


def _worker(factory: async_sessionmaker[AsyncSession], mqtt: FakeMqttClient) -> SyncWorker:
    return SyncWorker(
        session_factory=factory,
        mqtt=mqtt,
        mapper=DeliveryMapper(
            topics=TopicBuilder(environment="LAB"),
            device_id="pi-1",
            station_id="LAB",
            environment="LAB",
            simulated=True,
        ),
        require_application_sale_ack=True,
    )


async def _awaiting(
    factory: async_sessionmaker[AsyncSession],
    *,
    tx: str,
    amount: float | int = 0,
    volume: float | int = 0,
) -> None:
    async with unit_of_work(factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id=tx,
            event_type="TRANSACTION_COMPLETED",
            payload={"amount": amount, "volumeLiters": volume, "volume": volume},
            deduplication_key=f"tx-completed:LAB:complete:{tx}",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)


@pytest.mark.asyncio
async def test_missing_event_type_rejected_before_delivered(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _awaiting(db_factory, tx="tx-missing-evt", amount=1000, volume=1)
    mqtt = FakeMqttClient(client_id="ack-missing")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    matched = await worker.handle_sale_ack_payload(
        {
            "transactionId": "tx-missing-evt",
            "deduplicationKey": "tx-completed:LAB:complete:tx-missing-evt",
            "amount": 1000,
            "volumeLiters": 1,
        }
    )
    assert matched == 0
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 1


@pytest.mark.asyncio
async def test_unrelated_event_type_rejected_before_delivered(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _awaiting(db_factory, tx="tx-bad-evt", amount=1000, volume=1)
    mqtt = FakeMqttClient(client_id="ack-bad")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    matched = await worker.handle_sale_ack_payload(
        {
            "eventType": "PUMP_STATE_CHANGED",
            "transactionId": "tx-bad-evt",
            "deduplicationKey": "tx-completed:LAB:complete:tx-bad-evt",
            "amount": 1000,
            "volumeLiters": 1,
        }
    )
    assert matched == 0
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 1


@pytest.mark.asyncio
async def test_volume_liters_zero_camel_case_conflict_preserved(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    """volumeLiters=0 must not fall through ``or`` and skip conflict checks."""
    await _awaiting(db_factory, tx="tx-vol0-camel", amount=0, volume=0)
    mqtt = FakeMqttClient(client_id="ack-vol0-c")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    # Conflicting non-zero volume must leave awaiting.
    matched = await worker.handle_sale_ack_payload(
        {
            "eventType": "SALE_COMMITTED",
            "transactionId": "tx-vol0-camel",
            "deduplicationKey": "tx-completed:LAB:complete:tx-vol0-camel",
            "amount": 0,
            "volumeLiters": 1.5,
        }
    )
    assert matched == 0
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 1
    # Matching zero volume delivers.
    matched = await worker.handle_sale_ack_payload(
        {
            "eventType": "SALE_COMMITTED",
            "transactionId": "tx-vol0-camel",
            "deduplicationKey": "tx-completed:LAB:complete:tx-vol0-camel",
            "amount": 0,
            "volumeLiters": 0,
        }
    )
    assert matched == 1
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 0


@pytest.mark.asyncio
async def test_volume_liters_zero_snake_case_conflict_preserved(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _awaiting(db_factory, tx="tx-vol0-snake", amount=0, volume=0)
    mqtt = FakeMqttClient(client_id="ack-vol0-s")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    matched = await worker.handle_sale_ack_payload(
        {
            "eventType": "SALE_COMMITTED",
            "transactionId": "tx-vol0-snake",
            "deduplicationKey": "tx-completed:LAB:complete:tx-vol0-snake",
            "amount": 0,
            "volume_liters": 2.0,
        }
    )
    assert matched == 0
    matched = await worker.handle_sale_ack_payload(
        {
            "eventType": "SALE_COMMITTED",
            "transactionId": "tx-vol0-snake",
            "deduplicationKey": "tx-completed:LAB:complete:tx-vol0-snake",
            "amount": 0,
            "volume_liters": 0,
        }
    )
    assert matched == 1
