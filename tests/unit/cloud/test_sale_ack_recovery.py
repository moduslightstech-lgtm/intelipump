"""Application-ACK recovery: lost ACK, wrong scope, conflict, identical replay."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.delivery import DeliveryMapper
from intelipump_fdc.cloud.mqtt.fake import FakeMqttClient
from intelipump_fdc.cloud.sale_ack_intake import SaleAckIntake
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
    engine = create_engine(f"sqlite+aiosqlite:///{tmp_path / 'ack-recovery.db'}")
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
            device_id="InteliPump-Lab-pi-001",
            station_id="InteliPump-US-Lab",
            environment="LAB",
            simulated=True,
        ),
        require_application_sale_ack=True,
    )


@pytest.mark.asyncio
async def test_lost_ack_leaves_awaiting_then_recovers_on_replay(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    mqtt = FakeMqttClient(client_id="ack-lost")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    async with unit_of_work(db_factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-lost-ack",
            event_type="TRANSACTION_COMPLETED",
            payload={"amount": 1370, "volume": 1},
            deduplication_key="tx-completed:LAB:complete:tx-lost-ack",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)
        assert await uow.sync_queue.awaiting_app_ack_count() == 1

    # Cloud committed but ACK lost: row still awaiting.
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 1

    matched = await worker.handle_sale_ack_payload(
        {
            "eventType": "SALE_COMMITTED",
            "deviceId": "InteliPump-Lab-pi-001",
            "transactionId": "tx-lost-ack",
            "deduplicationKey": "tx-completed:LAB:complete:tx-lost-ack",
            "amount": 1370,
            "volumeLiters": 1,
        }
    )
    assert matched == 1
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 0
        assert await uow.sync_queue.delivered_count() >= 1


@pytest.mark.asyncio
async def test_wrong_sale_ack_does_not_clear_other_awaiting(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    mqtt = FakeMqttClient(client_id="ack-wrong")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    async with unit_of_work(db_factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-keep",
            event_type="TRANSACTION_COMPLETED",
            payload={},
            deduplication_key="tx-completed:LAB:complete:tx-keep",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)

    matched = await worker.handle_sale_ack_payload(
        {
            "eventType": "SALE_COMMITTED",
            "transactionId": "tx-other",
            "deduplicationKey": "tx-completed:LAB:complete:tx-other",
        }
    )
    assert matched == 0
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 1


@pytest.mark.asyncio
async def test_sale_ack_intake_ignores_wrong_device_topic(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    mqtt = FakeMqttClient(client_id="ack-device")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    topics = TopicBuilder(environment="LAB")
    intake = SaleAckIntake(
        mqtt=mqtt,
        topics=topics,
        device_id="InteliPump-Lab-pi-001",
        sync_worker=worker,
    )
    await intake.start()
    async with unit_of_work(db_factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-device",
            event_type="TRANSACTION_COMPLETED",
            payload={"amount": 5000, "volume": 3.65},
            deduplication_key="tx-completed:LAB:complete:tx-device",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)

    await mqtt.inject(
        "intelipump/lab/devices/other-pi/sale-acks",
        {
            "eventType": "SALE_COMMITTED",
            "deviceId": "other-pi",
            "transactionId": "tx-device",
            "deduplicationKey": "tx-completed:LAB:complete:tx-device",
        },
    )
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 1

    await mqtt.inject(
        topics.sale_acks("InteliPump-Lab-pi-001"),
        {
            "eventType": "SALE_COMMITTED",
            "deviceId": "InteliPump-Lab-pi-001",
            "transactionId": "tx-device",
            "deduplicationKey": "tx-completed:LAB:complete:tx-device",
            "amount": 5000,
            "volumeLiters": 3.65,
        },
    )
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 0

    await intake.stop()


@pytest.mark.asyncio
async def test_conflicting_ack_payload_leaves_awaiting(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    mqtt = FakeMqttClient(client_id="ack-conflict")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    async with unit_of_work(db_factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-conflict",
            event_type="TRANSACTION_COMPLETED",
            payload={"amount": 5000, "volume": 3.65},
            deduplication_key="tx-completed:LAB:complete:tx-conflict",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)

    matched = await worker.handle_sale_ack_payload(
        {
            "eventType": "SALE_COMMITTED",
            "transactionId": "tx-conflict",
            "deduplicationKey": "tx-completed:LAB:complete:tx-conflict",
            "amount": 9999,
            "volumeLiters": 3.65,
        }
    )
    assert matched == 0
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 1


@pytest.mark.asyncio
async def test_identical_ack_replay_is_idempotent(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    mqtt = FakeMqttClient(client_id="ack-idem")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    async with unit_of_work(db_factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-idem",
            event_type="TRANSACTION_COMPLETED",
            payload={"amount": 1000, "volume": 1},
            deduplication_key="tx-completed:LAB:complete:tx-idem",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)
    payload = {
        "eventType": "SALE_COMMITTED",
        "transactionId": "tx-idem",
        "deduplicationKey": "tx-completed:LAB:complete:tx-idem",
        "amount": 1000,
        "volumeLiters": 1,
    }
    assert await worker.handle_sale_ack_payload(payload) == 1
    # Second identical ACK finds nothing awaiting — safe no-op.
    assert await worker.handle_sale_ack_payload(payload) == 0
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.delivered_count() >= 1


@pytest.mark.asyncio
async def test_pi_restart_preserves_awaiting_app_ack(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Simulate process restart: same SQLite, new SyncWorker, still awaiting."""
    async with unit_of_work(db_factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-restart",
            event_type="TRANSACTION_COMPLETED",
            payload={"amount": 2000, "volume": 1.5},
            deduplication_key="tx-completed:LAB:complete:tx-restart",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)

    mqtt = FakeMqttClient(client_id="ack-restart")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 1

    assert (
        await worker.handle_sale_ack_payload(
            {
                "eventType": "SALE_COMMITTED",
                "transactionId": "tx-restart",
                "deduplicationKey": "tx-completed:LAB:complete:tx-restart",
            }
        )
        == 1
    )
