"""Failure-injection and in-process recovery for CRITICAL sale persistence."""

from __future__ import annotations

import asyncio
import os
import threading
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
    PersistJob,
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
async def test_spilled_critical_requeues_while_worker_still_busy(tmp_path: Path) -> None:
    """Spilled CRITICAL jobs re-enter the queue while another handler is still gated.

    Does not require an idle/empty worker or a process restart: the background
    requeue task fills a free slot as soon as depth < maxsize.
    """
    store = PersistRecoveryStore(tmp_path / "busy.jsonl")
    worker = PersistenceWorker(
        maxsize=2,
        critical_max_attempts=3,
        recovery_store=store,
    )
    gates = {
        "busy-a": asyncio.Event(),
        "busy-b": asyncio.Event(),
        "busy-c": asyncio.Event(),
        "busy-d": asyncio.Event(),
    }
    entered: list[str] = []

    async def gated(payload: dict) -> None:
        tx = str(payload["payload"]["active_transaction_id"])
        entered.append(tx)
        await gates[tx].wait()

    worker.register_handler("state_changed", gated)
    worker.start()
    # A running; B+C fill the waiting queue; D spills to durable store.
    for tx in ("busy-a", "busy-b", "busy-c"):
        worker.submit(
            kind="state_changed",
            payload=_completion_payload(tx),
            handler=gated,
            priority=PersistPriority.CRITICAL,
        )
        await asyncio.sleep(0.02)
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("busy-d"),
        handler=gated,
        priority=PersistPriority.CRITICAL,
    )
    assert worker.critical_durable_spills >= 1
    assert any("busy-d" in p.identity_key for p in store.list_pending())
    assert "busy-a" in entered

    # Free A so B starts; one queue slot opens while B remains gated.
    gates["busy-a"].set()
    for _ in range(50):
        if any("busy-d" in key for key in worker._inflight_identities):
            break
        await asyncio.sleep(0.05)
    assert any("busy-d" in key for key in worker._inflight_identities)
    assert not gates["busy-b"].is_set()
    for g in gates.values():
        g.set()
    for _ in range(40):
        if worker.processed >= 4:
            break
        await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=2.0)
    assert worker.processed >= 4
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


@pytest.mark.asyncio
async def test_delayed_write_ahead_completes_before_handler(tmp_path: Path) -> None:
    """One tracked write per sale; handler must not run until that write finishes."""
    store = PersistRecoveryStore(tmp_path / "delay.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=store)
    release_write = threading.Event()
    handler_started = asyncio.Event()
    upsert_calls = {"n": 0}
    real_upsert = store.upsert

    def gated_upsert(**kwargs):
        upsert_calls["n"] += 1
        if not release_write.wait(timeout=2.0):
            raise TimeoutError("write gate not released")
        return real_upsert(**kwargs)

    store.upsert = gated_upsert  # type: ignore[method-assign]
    worker.start()
    order: list[str] = []

    async def handler(_payload: dict) -> None:
        order.append("handler")
        handler_started.set()

    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-delay"),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(100):
        if upsert_calls["n"] >= 1:
            break
        await asyncio.sleep(0.02)
    assert upsert_calls["n"] == 1
    assert worker.pending_durable_writes >= 1
    assert not handler_started.is_set()
    assert "handler" not in order

    release_write.set()
    for _ in range(40):
        if worker.processed >= 1:
            break
        await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=2.0)
    assert order == ["handler"]
    assert upsert_calls["n"] == 1  # one tracked write, not a second execute upsert
    assert store.pending_count() == 0


@pytest.mark.asyncio
async def test_write_ahead_failure_retries_without_killing_worker(
    tmp_path: Path,
) -> None:
    """Write-ahead I/O failure must not kill the worker or drop the sale."""
    store = PersistRecoveryStore(tmp_path / "fail.jsonl")
    worker = PersistenceWorker(
        maxsize=8,
        critical_max_attempts=4,
        recovery_store=store,
    )
    real_upsert = store.upsert
    fails_left = {"n": 2}

    def flaky_upsert(**kwargs):
        if fails_left["n"] > 0:
            fails_left["n"] -= 1
            raise PersistRecoveryIOError("simulated disk failure")
        return real_upsert(**kwargs)

    store.upsert = flaky_upsert  # type: ignore[method-assign]
    worker.start()
    handled: list[str] = []

    async def handler(payload: dict) -> None:
        handled.append(str(payload["payload"]["active_transaction_id"]))

    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-write-fail"),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(80):
        if worker.processed >= 1:
            break
        await asyncio.sleep(0.05)
    # Worker still alive and accepted further work.
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-write-ok"),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(40):
        if worker.processed >= 2:
            break
        await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=2.0)
    assert "tx-write-fail" in handled
    assert "tx-write-ok" in handled
    assert worker.critical_retries >= 1
    assert store.pending_count() == 0
    assert worker._task is None  # clean stop, not a crashed loop


@pytest.mark.asyncio
async def test_shutdown_waits_for_pending_durable_write(tmp_path: Path) -> None:
    """stop(flush=True) must account for in-flight write-ahead before returning."""
    store = PersistRecoveryStore(tmp_path / "shutdown.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=store)
    release_write = threading.Event()
    real_upsert = store.upsert
    write_finished = {"done": False}

    def gated_upsert(**kwargs):
        if not release_write.wait(timeout=2.0):
            raise TimeoutError("write gate not released")
        result = real_upsert(**kwargs)
        write_finished["done"] = True
        return result

    store.upsert = gated_upsert  # type: ignore[method-assign]
    worker.start()

    async def handler(_payload: dict) -> None:
        return None

    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-shutdown"),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(50):
        if worker.pending_durable_writes >= 1:
            break
        await asyncio.sleep(0.02)
    assert worker.pending_durable_writes >= 1

    async def release_soon() -> None:
        await asyncio.sleep(0.15)
        release_write.set()

    releaser = asyncio.create_task(release_soon())
    await worker.stop(flush=True, timeout_s=2.0)
    await releaser
    assert write_finished["done"] is True
    assert store.pending_count() == 0


@pytest.mark.asyncio
async def test_late_write_does_not_recreate_after_mark_done(tmp_path: Path) -> None:
    """A stalled write thread must not resurrect a record after mark_done."""
    store = PersistRecoveryStore(tmp_path / "late.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=store)
    enter_write = threading.Event()
    release_write = threading.Event()
    real_upsert = store.upsert

    def stalled_upsert(**kwargs):
        enter_write.set()
        if not release_write.wait(timeout=2.0):
            raise TimeoutError("late write not released")
        return real_upsert(**kwargs)

    store.upsert = stalled_upsert  # type: ignore[method-assign]
    worker.start()
    key = "state_changed:1:tx-late:FILLING_COMPLETE"
    worker._schedule_durable_upsert(
        identity_key=key,
        kind="state_changed",
        payload=_completion_payload("tx-late"),
        attempt=0,
    )
    entered = await asyncio.to_thread(enter_write.wait, 2.0)
    assert entered, "durable write task never entered upsert"
    # Completion path: bump epoch then remove the row while the write is stalled.
    worker._invalidate_durable_write(key)
    # Row may not exist yet (stalled before upsert); mark_done is still safe.
    PersistRecoveryStore.mark_done(store, key)
    assert store.pending_count() == 0
    release_write.set()
    for _ in range(40):
        if worker.pending_durable_writes == 0:
            break
        await asyncio.sleep(0.05)
    # Allow the late-write undo mark_done to settle.
    await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=2.0)
    assert store.pending_count() == 0


@pytest.mark.asyncio
async def test_fast_write_failure_result_retained_until_consumed(
    tmp_path: Path,
) -> None:
    """A write that finishes before the worker awaits must still surface failure.

    Regression: done-callback removal made ``_await_durable_write`` see no task
    and treat a fast failure as success.
    """
    store = PersistRecoveryStore(tmp_path / "fastfail.jsonl")
    worker = PersistenceWorker(
        maxsize=8,
        critical_max_attempts=3,
        recovery_store=store,
    )
    real_upsert = store.upsert
    fails_left = {"n": 1}
    observed: dict[str, object] = {}

    def flaky_upsert(**kwargs):
        if fails_left["n"] > 0:
            fails_left["n"] -= 1
            raise PersistRecoveryIOError("fast disk failure")
        return real_upsert(**kwargs)

    store.upsert = flaky_upsert  # type: ignore[method-assign]

    real_execute = worker._execute

    async def delay_until_write_done(job: PersistJob) -> None:
        key = job.identity_key
        assert key is not None
        # Observe only the first execute — retries must not overwrite the race check.
        if "present_before_await" not in observed:
            for _ in range(100):
                task = worker._pending_writes.get(key)
                if task is not None and task.done():
                    observed["present_before_await"] = True
                    observed["task_done"] = True
                    observed["had_exception"] = task.exception() is not None
                    break
                await asyncio.sleep(0.01)
            else:
                observed["present_before_await"] = key in worker._pending_writes
                observed["task_done"] = False
                observed["had_exception"] = False
        await real_execute(job)

    worker._execute = delay_until_write_done  # type: ignore[method-assign]
    worker.start()
    handled: list[str] = []

    async def handler(payload: dict) -> None:
        handled.append(str(payload["payload"]["active_transaction_id"]))

    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-fast-fail"),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(80):
        if worker.processed >= 1:
            break
        await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=2.0)

    assert observed.get("present_before_await") is True
    assert observed.get("task_done") is True
    assert observed.get("had_exception") is True
    assert handled == ["tx-fast-fail"]
    assert worker.critical_retries >= 1
    assert store.pending_count() == 0


@pytest.mark.asyncio
async def test_persistent_disk_failure_retains_sale_until_storage_returns(
    tmp_path: Path,
) -> None:
    """Final spill failure keeps the sale (degraded); recovers when disk works."""
    store = PersistRecoveryStore(tmp_path / "persistfail.jsonl")
    worker = PersistenceWorker(
        maxsize=8,
        critical_max_attempts=2,
        recovery_store=store,
    )
    real_upsert = store.upsert
    disk_up = {"ok": False}

    def gated_disk(**kwargs):
        if not disk_up["ok"]:
            raise PersistRecoveryIOError("persistent disk failure")
        return real_upsert(**kwargs)

    store.upsert = gated_disk  # type: ignore[method-assign]
    worker.start()
    handled: list[str] = []

    async def handler(payload: dict) -> None:
        handled.append(str(payload["payload"]["active_transaction_id"]))

    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-retained"),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(80):
        if worker.retained_sale_count >= 1:
            break
        await asyncio.sleep(0.05)

    assert worker.retained_sale_count >= 1
    assert worker.is_degraded is True
    assert worker.processed == 0
    assert handled == []
    assert store.pending_count() == 0

    # Storage returns — retained sale must spill and complete.
    disk_up["ok"] = True
    for _ in range(80):
        if worker.processed >= 1 and worker.retained_sale_count == 0:
            break
        await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=2.0)

    assert handled == ["tx-retained"]
    assert worker.retained_sale_count == 0
    assert store.pending_count() == 0
    assert worker.processed >= 1
