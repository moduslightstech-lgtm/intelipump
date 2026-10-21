"""Cloud-sync SyncWorker delivered prune (controller path untouched)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
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
from intelipump_fdc.persistence.models import SyncQueueRow
from intelipump_fdc.persistence.unit_of_work import unit_of_work


@pytest.fixture
async def db_factory(tmp_path: Path) -> async_sessionmaker[AsyncSession]:
    engine = create_engine(f"sqlite+aiosqlite:///{tmp_path / 'prune.db'}")
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
            device_id="dev",
            station_id="s",
            environment="LAB",
            simulated=True,
        ),
        delivered_retention_days=30,
        prune_batch_size=100,
        prune_interval_seconds=1.0,
    )


@pytest.mark.asyncio
async def test_sync_worker_prune_deletes_old_delivered_only(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with unit_of_work(db_factory) as uow:
        old = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-old",
            event_type="TRANSACTION_COMPLETED",
            payload={"environment": "LAB"},
            deduplication_key="prune-old",
        )
        keep = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-keep",
            event_type="TRANSACTION_COMPLETED",
            payload={"environment": "LAB"},
            deduplication_key="prune-keep-pending",
        )
        assert old is not None and keep is not None
        await uow.sync_queue.mark_delivered(old.id)
        row = (
            await uow.session.execute(
                select(SyncQueueRow).where(SyncQueueRow.id == old.id)
            )
        ).scalar_one()
        row.updated_at = datetime.now(UTC) - timedelta(days=40)
        await uow.session.flush()

    mqtt = FakeMqttClient(client_id="prune-test")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    await worker._maybe_prune_and_alert()
    assert worker.stats.pruned_delivered == 1

    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.pending_count() == 1
        assert await uow.sync_queue.delivered_count() == 0


@pytest.mark.asyncio
async def test_sync_worker_degraded_skips_cycle(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    mqtt = FakeMqttClient(client_id="degraded-test")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    worker.mark_database_degraded(detail="malformed")
    assert worker.stats.database_degraded is True
    await worker._cycle()  # must no-op without raising


@pytest.mark.asyncio
async def test_sync_worker_periodic_integrity_ok(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    mqtt = FakeMqttClient(client_id="integrity-ok")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    worker.integrity_interval_seconds = 1.0
    await worker._maybe_integrity_check()
    assert worker.stats.database_degraded is False
    assert worker.stats.last_integrity_at is not None


@pytest.mark.asyncio
async def test_sync_worker_periodic_integrity_degrades(
    db_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _bad(_factory: object) -> tuple[bool, str]:
        return False, "btree corruption simulated"

    monkeypatch.setattr(
        "intelipump_fdc.cloud.sync_worker.sqlite_quick_check_factory",
        _bad,
    )
    mqtt = FakeMqttClient(client_id="integrity-bad")
    await mqtt.connect()
    worker = _worker(db_factory, mqtt)
    worker.integrity_interval_seconds = 1.0
    await worker._maybe_integrity_check()
    assert worker.stats.database_degraded is True
    assert "btree" in (worker.stats.last_error or "")
