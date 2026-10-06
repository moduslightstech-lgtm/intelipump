"""Application ACK path for completed-sale sync_queue rows."""

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
    engine = create_engine(f"sqlite+aiosqlite:///{tmp_path / 'ack.db'}")
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    yield factory
    await dispose_engine(engine)


@pytest.mark.asyncio
async def test_sale_ack_promotes_awaiting_app_ack(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with unit_of_work(db_factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-ack-1",
            event_type="TRANSACTION_COMPLETED",
            payload={"transaction_uuid": "tx-ack-1", "amount": 1000},
            deduplication_key="tx-completed:LAB:complete:tx-ack-1",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)

    mqtt = FakeMqttClient(client_id="ack-test")
    await mqtt.connect()
    worker = SyncWorker(
        session_factory=db_factory,
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
    matched = await worker.handle_sale_ack_payload(
        {
            "eventType": "SALE_COMMITTED",
            "transactionId": "tx-ack-1",
            "deduplicationKey": "tx-completed:LAB:complete:tx-ack-1",
        }
    )
    assert matched == 1
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 0
        assert await uow.sync_queue.delivered_count() >= 1
