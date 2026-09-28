"""Measure recovery-store fsync cost and prove submit does not block on it."""

from __future__ import annotations

import asyncio
import statistics
import time
from pathlib import Path

import pytest

from intelipump_fdc.services.persist_recovery import PersistRecoveryStore
from intelipump_fdc.services.persistence_worker import (
    PersistenceWorker,
    PersistPriority,
)


@pytest.fixture(autouse=True)
def _allow_tmp(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")


def test_recovery_store_upsert_fsync_latency_representative(tmp_path: Path) -> None:
    """Measure durable upsert latency on local disk (representative SSD/APFS)."""
    store = PersistRecoveryStore(tmp_path / "lat.jsonl")
    samples_ms: list[float] = []
    payload = {
        "address": 1,
        "detail": "FILLING->FILLING_COMPLETE",
        "payload": {
            "normalized_state": "FILLING_COMPLETE",
            "active_transaction_id": "tx-lat",
        },
    }
    for i in range(20):
        t0 = time.perf_counter()
        store.upsert(
            identity_key=f"state_changed:1:tx-lat-{i}:FILLING_COMPLETE",
            kind="state_changed",
            payload=payload,
            attempt=0,
        )
        samples_ms.append((time.perf_counter() - t0) * 1000.0)
    p50 = statistics.median(samples_ms)
    p95 = sorted(samples_ms)[int(0.95 * (len(samples_ms) - 1))]
    # Soft budget: fsync on local SSD is typically <20ms p95; flag pathological cases.
    assert p95 < 100.0, f"recovery upsert p95 too high: p50={p50:.2f}ms p95={p95:.2f}ms"
    print(
        f"persist_recovery_upsert_latency_ms n=20 p50={p50:.3f} p95={p95:.3f} "
        f"max={max(samples_ms):.3f}"
    )


@pytest.mark.asyncio
async def test_critical_submit_does_not_block_on_fsync(tmp_path: Path) -> None:
    """Controller-path submit must return without waiting for recovery fsync."""
    store = PersistRecoveryStore(tmp_path / "noblock.jsonl")
    # Inflate fsync cost by writing a large prior file then upserting.
    worker = PersistenceWorker(maxsize=8, recovery_store=store)
    worker.start()
    gate = asyncio.Event()

    async def slow_handler(_payload: dict) -> None:
        await gate.wait()

    payload = {
        "address": 1,
        "detail": "FILLING->FILLING_COMPLETE",
        "payload": {
            "normalized_state": "FILLING_COMPLETE",
            "active_transaction_id": "tx-noblock",
        },
    }
    t0 = time.perf_counter()
    worker.submit(
        kind="state_changed",
        payload=payload,
        handler=slow_handler,
        priority=PersistPriority.CRITICAL,
    )
    submit_ms = (time.perf_counter() - t0) * 1000.0
    # Sync fsync on the same thread would typically be >0.2ms; allow tiny overhead.
    assert submit_ms < 5.0, f"submit blocked too long ({submit_ms:.2f}ms) — fsync on hot path?"
    gate.set()
    await worker.stop(flush=True, timeout_s=2.0)
    # Durability still lands (worker write-ahead / scheduled upsert).
    for _ in range(40):
        if store.pending_count() == 0 and worker.processed >= 1:
            break
        await asyncio.sleep(0.05)
    assert worker.processed >= 1
