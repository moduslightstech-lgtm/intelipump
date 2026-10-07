"""Process-termination fault tests around durable handoff and app ACK."""

from __future__ import annotations

import asyncio
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
from intelipump_fdc.services.persist_recovery import PersistRecoveryStore
from intelipump_fdc.services.persistence_worker import PersistPriority, PersistenceWorker


def _completion_payload(tx: str, *, address: int = 1) -> dict:
    return {
        "address": address,
        "payload": {
            "address": address,
            "event": "FILLING_COMPLETE",
            "normalized_state": "FILLING_COMPLETE",
            "active_transaction_id": tx,
            "filled_volume_raw": 365,
            "filled_amount_raw": 500000,
            "selected_nozzle": 1,
            "completion_evidence_key": f"complete:{tx}",
            "may_publish_sale": True,
            "filling_seen_this_boot": True,
        },
    }


@pytest.mark.asyncio
async def test_kill_before_durable_write_leaves_handoff_pending_not_queued(
    tmp_path: Path,
) -> None:
    """Crash window: handoff scheduled, write not finished → nothing durable yet."""
    gate = asyncio.Event()

    class BlockedStore(PersistRecoveryStore):
        def upsert(self, **kwargs):  # type: ignore[no-untyped-def]
            # Block forever until cancelled — simulates death mid-write.
            import time

            while not gate.is_set():
                time.sleep(0.01)
            raise PersistRecoveryIOError("unreachable")

    from intelipump_fdc.services.persist_recovery import PersistRecoveryIOError

    blocked = BlockedStore(tmp_path / "before.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=blocked)
    handled: list[str] = []

    async def handler(payload: dict) -> None:
        handled.append(str(payload["payload"]["active_transaction_id"]))

    worker.register_handler("state_changed", handler)
    worker.start()
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-pre-write"),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    await asyncio.sleep(0.1)
    # Mid-write: handoff pending, not yet on recovery store / handler.
    assert worker.handoff_pending_count >= 1
    assert blocked.pending_count() == 0
    assert handled == []
    worker.note_capture_uncertainty(reason="kill_before_durable_write")
    assert worker.capture_uncertainty_count >= 1
    # Simulate hard kill — do not flush (would wait on blocked write).
    worker._stop.set()
    for t in list(worker._pending_writes.values()):
        t.cancel()
    gate.set()
    await worker.stop(flush=False)


@pytest.mark.asyncio
async def test_kill_after_durable_write_before_sqlite_recovers_identity(
    tmp_path: Path,
) -> None:
    store = PersistRecoveryStore(tmp_path / "after-write.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=store)

    async def noop(_payload: dict) -> None:
        return None

    # Do not start worker — simulate crash after handoff, before handler.
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-after-write"),
        handler=noop,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(50):
        if store.pending_count() >= 1:
            break
        await asyncio.sleep(0.02)
    assert store.pending_count() >= 1
    # New process recovers identity from durable store.
    worker2 = PersistenceWorker(maxsize=8, recovery_store=store)
    recovered_ids: list[str] = []

    async def handler(payload: dict) -> None:
        recovered_ids.append(str(payload["payload"]["active_transaction_id"]))

    worker2.register_handler("state_changed", handler)
    worker2.start()
    n = worker2.recover_pending()
    assert n >= 1
    for _ in range(50):
        if recovered_ids:
            break
        await asyncio.sleep(0.05)
    assert "tx-after-write" in recovered_ids
    await worker2.stop(flush=True, timeout_s=2.0)
    assert store.pending_count() == 0


@pytest.fixture
async def db_factory(tmp_path: Path) -> async_sessionmaker[AsyncSession]:
    engine = create_engine(f"sqlite+aiosqlite:///{tmp_path / 'fault-ack.db'}")
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    yield factory
    await dispose_engine(engine)


@pytest.mark.asyncio
async def test_kill_before_app_ack_preserves_awaiting_totals(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with unit_of_work(db_factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-pre-ack",
            event_type="TRANSACTION_COMPLETED",
            payload={"amount": 5000, "volume": 3.65},
            deduplication_key="tx-completed:LAB:complete:tx-pre-ack",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)

    # Simulate process restart: new SyncWorker, same SQLite.
    mqtt = FakeMqttClient(client_id="fault-pre-ack")
    await mqtt.connect()
    worker = SyncWorker(
        session_factory=db_factory,
        mqtt=mqtt,
        mapper=DeliveryMapper(
            topics=TopicBuilder(environment="LAB"),
            device_id="pi-lab",
            station_id="LAB",
            environment="LAB",
            simulated=True,
        ),
        require_application_sale_ack=True,
    )
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 1
    assert (
        await worker.handle_sale_ack_payload(
            {
                "eventType": "SALE_COMMITTED",
                "transactionId": "tx-pre-ack",
                "deduplicationKey": "tx-completed:LAB:complete:tx-pre-ack",
                "amount": 5000,
                "volumeLiters": 3.65,
            }
        )
        == 1
    )
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 0
        assert await uow.sync_queue.delivered_count() >= 1


@pytest.mark.asyncio
async def test_kill_after_app_ack_keeps_delivered_ledger(
    db_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with unit_of_work(db_factory) as uow:
        row = await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-post-ack",
            event_type="TRANSACTION_COMPLETED",
            payload={"amount": 1370, "volume": 1},
            deduplication_key="tx-completed:LAB:complete:tx-post-ack",
        )
        assert row is not None
        await uow.sync_queue.mark_awaiting_app_ack(row.id)
        await uow.sync_queue.mark_delivered_by_dedupe_key(
            "tx-completed:LAB:complete:tx-post-ack"
        )

    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.awaiting_app_ack_count() == 0
        assert await uow.sync_queue.delivered_count() >= 1
        # Totals still on ledger payload
        from sqlalchemy import select
        from intelipump_fdc.persistence.models import SyncQueueRow

        result = await uow.session.execute(
            select(SyncQueueRow).where(SyncQueueRow.entity_id == "tx-post-ack")
        )
        row = result.scalar_one()
        assert row.status == "DELIVERED"
        assert row.payload["amount"] == 1370
        assert row.payload["volume"] == 1
