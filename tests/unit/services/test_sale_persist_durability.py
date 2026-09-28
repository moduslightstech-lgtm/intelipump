"""Failure-injection tests for CRITICAL sale persistence durability."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from intelipump_fdc.services.persist_recovery import (
    PersistRecoveryStore,
    sale_persist_identity,
)
from intelipump_fdc.services.persistence_worker import (
    PersistenceWorker,
    PersistPriority,
)


@pytest.mark.asyncio
async def test_temporary_sqlite_error_retries_without_duplicate(
    tmp_path: Path,
) -> None:
    store = PersistRecoveryStore(tmp_path / "rec.jsonl")
    worker = PersistenceWorker(
        maxsize=16,
        critical_max_attempts=5,
        recovery_store=store,
    )
    worker.start()
    calls = {"n": 0}
    seen_ids: list[str] = []

    async def flaky(payload: dict) -> None:
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError("database is locked")
        tx = payload["payload"]["active_transaction_id"]
        seen_ids.append(str(tx))

    payload = {
        "address": 1,
        "detail": "FILLING->FILLING_COMPLETE",
        "payload": {
            "normalized_state": "FILLING_COMPLETE",
            "active_transaction_id": "tx-retry-1",
        },
    }
    assert sale_persist_identity("state_changed", payload) == "sale:1:tx-retry-1"
    worker.submit(
        kind="state_changed",
        payload=payload,
        handler=flaky,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(40):
        if worker.processed >= 1:
            break
        await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=2.0)
    assert calls["n"] == 3
    assert seen_ids == ["tx-retry-1"]
    assert worker.critical_retries >= 2
    assert store.pending_count() == 0
    assert worker.is_degraded is False


@pytest.mark.asyncio
async def test_pi_restart_replays_unfinished_critical_sale(tmp_path: Path) -> None:
    path = tmp_path / "rec.jsonl"
    store = PersistRecoveryStore(path)
    worker1 = PersistenceWorker(
        maxsize=8,
        critical_max_attempts=1,
        recovery_store=store,
    )
    worker1.start()

    async def always_fail(_payload: dict) -> None:
        raise RuntimeError("sqlite boom")

    payload = {
        "address": 2,
        "detail": "complete",
        "payload": {
            "normalized_state": "FILLING_COMPLETE",
            "active_transaction_id": "tx-unfinished",
        },
    }
    worker1.submit(
        kind="state_changed",
        payload=payload,
        handler=always_fail,
        priority=PersistPriority.CRITICAL,
    )
    await asyncio.sleep(0.3)
    await worker1.stop(flush=False, timeout_s=1.0)
    assert store.pending_count() == 1
    assert worker1.is_degraded is True

    # Simulate process restart: new worker, same durable store.
    worker2 = PersistenceWorker(
        maxsize=8,
        critical_max_attempts=3,
        recovery_store=PersistRecoveryStore(path),
    )
    done: list[str] = []

    async def succeed(p: dict) -> None:
        done.append(str(p["payload"]["active_transaction_id"]))

    worker2.register_handler("state_changed", succeed)
    worker2.start()
    restored = worker2.recover_pending()
    assert restored == 1
    for _ in range(40):
        if worker2.processed >= 1:
            break
        await asyncio.sleep(0.05)
    await worker2.stop(flush=True, timeout_s=2.0)
    assert done == ["tx-unfinished"]
    assert PersistRecoveryStore(path).pending_count() == 0


@pytest.mark.asyncio
async def test_critical_failure_sets_degraded_health_signal(tmp_path: Path) -> None:
    store = PersistRecoveryStore(tmp_path / "d.jsonl")
    worker = PersistenceWorker(
        maxsize=4,
        critical_max_attempts=1,
        recovery_store=store,
    )
    worker.start()

    async def boom(_p: dict) -> None:
        raise RuntimeError("disk full")

    worker.submit(
        kind="state_changed",
        payload={
            "address": 1,
            "detail": "x",
            "payload": {
                "normalized_state": "FILLING_COMPLETE",
                "active_transaction_id": "tx-deg",
            },
        },
        handler=boom,
        priority=PersistPriority.CRITICAL,
    )
    await asyncio.sleep(0.25)
    assert worker.is_degraded is True
    assert worker.degraded_reason is not None
    assert store.pending_count() == 1
    await worker.stop(flush=False, timeout_s=1.0)


@pytest.mark.asyncio
async def test_same_sale_identity_not_duplicated_in_recovery_store(
    tmp_path: Path,
) -> None:
    store = PersistRecoveryStore(tmp_path / "id.jsonl")
    payload = {
        "address": 1,
        "detail": "FILLING_COMPLETE",
        "payload": {
            "normalized_state": "FILLING_COMPLETE",
            "active_transaction_id": "same-tx",
        },
    }
    key = sale_persist_identity("state_changed", payload)
    assert key == "sale:1:same-tx"
    store.upsert(identity_key=key, kind="state_changed", payload=payload, attempt=1)
    store.upsert(identity_key=key, kind="state_changed", payload=payload, attempt=2)
    assert store.pending_count() == 1
