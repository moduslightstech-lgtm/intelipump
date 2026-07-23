"""Sync queue repository tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from intelipump_fdc.persistence.unit_of_work import unit_of_work


@pytest.mark.asyncio
async def test_sync_queue_deduplication(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        first = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-1",
            event_type="TRANSACTION_COMPLETED",
            payload={"environment": "LAB", "station_id": "s", "simulated": True},
            deduplication_key="tx-completed:key-1",
        )
        second = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-1",
            event_type="TRANSACTION_COMPLETED",
            payload={"environment": "LAB"},
            deduplication_key="tx-completed:key-1",
        )
        assert first is not None
        assert second is None
        assert await uow.sync_queue.pending_count() == 1


@pytest.mark.asyncio
async def test_sync_queue_claim_lock_and_stale_release(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        await uow.sync_queue.enqueue(
            entity_type="alarm",
            entity_id="a1",
            event_type="ALARM_ACTIVE",
            payload={"environment": "LAB"},
            deduplication_key="alarm:1",
        )
        claimed = await uow.sync_queue.claim_batch(limit=5)
        assert len(claimed) == 1
        assert claimed[0].status == "CLAIMED"
        assert claimed[0].locked_at is not None
        # Force stale lock
        from sqlalchemy import select

        from intelipump_fdc.persistence.models import SyncQueueRow

        row = (
            await uow.session.execute(
                select(SyncQueueRow).where(SyncQueueRow.id == claimed[0].id)
            )
        ).scalar_one()
        row.locked_at = datetime.now(UTC) - timedelta(seconds=120)
        await uow.session.flush()
        released = await uow.sync_queue.release_stale_locks(older_than_seconds=60)
        assert released == 1
        again = await uow.sync_queue.claim_batch(limit=5)
        assert len(again) == 1


@pytest.mark.asyncio
async def test_failed_queue_retry_metadata(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        item = await uow.sync_queue.enqueue(
            entity_type="state",
            entity_id="p1",
            event_type="STATE_CHANGED",
            payload={},
            deduplication_key="state:1",
        )
        assert item is not None
        claimed = await uow.sync_queue.claim_batch(limit=1)
        await uow.sync_queue.mark_failed(
            claimed[0].id, error="mqtt_unavailable", backoff_seconds=30
        )
        from sqlalchemy import select

        from intelipump_fdc.persistence.models import SyncQueueRow

        row = (
            await uow.session.execute(
                select(SyncQueueRow).where(SyncQueueRow.id == claimed[0].id)
            )
        ).scalar_one()
        assert row.status == "PENDING"
        assert row.attempt_count == 1
        assert row.last_error == "mqtt_unavailable"
        available = row.available_at
        if available.tzinfo is None:
            available = available.replace(tzinfo=UTC)
        assert available > datetime.now(UTC)
        await uow.sync_queue.mark_delivered(claimed[0].id)
