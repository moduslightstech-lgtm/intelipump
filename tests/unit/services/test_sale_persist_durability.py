"""Failure-injection and in-process recovery for CRITICAL sale persistence."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from intelipump_fdc.services.persist_recovery import (
    PersistRecoveryCorruptError,
    PersistRecoveryIdentityConflict,
    PersistRecoveryIOError,
    PersistRecoveryStore,
    sale_persist_identity,
)
from intelipump_fdc.services.persistence_worker import (
    PersistenceWorker,
    PersistPriority,
)


@pytest.fixture(autouse=True)
def _allow_tmp_recovery(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")


def _completion_payload(tx: str, *, address: int = 1) -> dict:
    return {
        "address": address,
        "detail": "FILLING->FILLING_COMPLETE",
        "payload": {
            "normalized_state": "FILLING_COMPLETE",
            "active_transaction_id": tx,
        },
    }


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
        seen_ids.append(str(payload["payload"]["active_transaction_id"]))

    payload = _completion_payload("tx-retry-1")
    assert (
        sale_persist_identity("state_changed", payload)
        == "state_changed:1:tx-retry-1:FILLING_COMPLETE"
    )
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


@pytest.mark.asyncio
async def test_queue_full_critical_requeues_without_restart(tmp_path: Path) -> None:
    store = PersistRecoveryStore(tmp_path / "qf.jsonl")
    worker = PersistenceWorker(
        maxsize=1,
        critical_max_attempts=3,
        recovery_store=store,
    )
    gate = asyncio.Event()
    done: list[str] = []

    async def blocker(payload: dict) -> None:
        await gate.wait()
        done.append(str(payload["payload"]["active_transaction_id"]))

    worker.register_handler("state_changed", blocker)
    worker.start()
    # A executing, B fills the single queue slot, C must spill to durable store.
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-a"),
        handler=blocker,
        priority=PersistPriority.CRITICAL,
    )
    await asyncio.sleep(0.05)
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-b"),
        handler=blocker,
        priority=PersistPriority.CRITICAL,
    )
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-c"),
        handler=blocker,
        priority=PersistPriority.CRITICAL,
    )
    assert store.pending_count() >= 1
    assert worker.critical_durable_spills >= 1
    assert worker.is_degraded is True
    gate.set()
    for _ in range(80):
        if set(done) >= {"tx-a", "tx-b", "tx-c"}:
            break
        await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=3.0)
    assert set(done) == {"tx-a", "tx-b", "tx-c"}
    assert store.pending_count() == 0


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

    worker1.submit(
        kind="state_changed",
        payload=_completion_payload("tx-unfinished"),
        handler=always_fail,
        priority=PersistPriority.CRITICAL,
    )
    await asyncio.sleep(0.3)
    await worker1.stop(flush=False, timeout_s=1.0)
    assert store.pending_count() == 1

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


def test_identity_unique_per_sale_and_event_no_overwrite(tmp_path: Path) -> None:
    store = PersistRecoveryStore(tmp_path / "id.jsonl")
    p1 = _completion_payload("sale-a")
    p2 = _completion_payload("sale-b")
    k1 = sale_persist_identity("state_changed", p1)
    k2 = sale_persist_identity("state_changed", p2)
    assert k1 != k2
    store.upsert(identity_key=k1, kind="state_changed", payload=p1, attempt=0)
    store.upsert(identity_key=k2, kind="state_changed", payload=p2, attempt=0)
    assert store.pending_count() == 2
    # Same identity + same payload: allowed (retry metadata refresh).
    store.upsert(identity_key=k1, kind="state_changed", payload=p1, attempt=2)
    assert store.pending_count() == 2
    # Same identity + different payload: refuse overwrite.
    with pytest.raises(PersistRecoveryIdentityConflict):
        store.upsert(
            identity_key=k1,
            kind="state_changed",
            payload=p2,
            attempt=0,
        )


def test_corrupt_recovery_storage_fails_visibly(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text("{not-json\n", encoding="utf-8")
    store = PersistRecoveryStore(path)
    with pytest.raises(PersistRecoveryCorruptError):
        store.list_pending()


def test_unwritable_recovery_storage_fails_visibly(tmp_path: Path) -> None:
    parent = tmp_path / "readonly"
    parent.mkdir()
    store = PersistRecoveryStore(parent / "rec.jsonl")
    store.upsert(
        identity_key="state_changed:1:tx:FILLING_COMPLETE",
        kind="state_changed",
        payload=_completion_payload("tx"),
        attempt=0,
    )
    os.chmod(parent, 0o500)
    try:
        with pytest.raises(PersistRecoveryIOError):
            store.upsert(
                identity_key="state_changed:1:tx2:FILLING_COMPLETE",
                kind="state_changed",
                payload=_completion_payload("tx2"),
                attempt=0,
            )
    finally:
        os.chmod(parent, 0o700)


def test_tmp_path_rejected_without_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", raising=False)
    with pytest.raises(PersistRecoveryIOError):
        PersistRecoveryStore(Path("/tmp/intelipump_forbid.jsonl"))
