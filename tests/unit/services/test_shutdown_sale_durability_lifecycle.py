"""Controller/API lifecycle when sale durability flush is incomplete at shutdown."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest
from fastapi import FastAPI

from intelipump_fdc.api.app import create_app
from intelipump_fdc.api.lifespan import app_lifespan
from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.services.lab_persistence import start_persistence
from intelipump_fdc.services.persist_recovery import (
    PersistRecoveryIOError,
    PersistRecoveryStore,
)
from intelipump_fdc.services.persistence_worker import (
    PersistFlushIncompleteError,
    PersistenceWorker,
    PersistPriority,
)


def _completion_payload(tx: str, *, address: int = 1) -> dict:
    return {
        "address": address,
        "detail": "FILLING->FILLING_COMPLETE",
        "payload": {
            "normalized_state": "FILLING_COMPLETE",
            "active_transaction_id": tx,
        },
    }


async def _force_undurable_retained(worker, *, tx: str) -> None:
    store = worker._recovery_store
    assert store is not None

    def always_down(**kwargs):
        raise PersistRecoveryIOError("storage unavailable")

    store.upsert = always_down  # type: ignore[method-assign]
    worker._critical_max_attempts = 2

    async def handler(_payload: dict) -> None:
        return None

    worker.submit(
        kind="state_changed",
        payload=_completion_payload(tx),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(80):
        if worker.retained_sale_count >= 1:
            break
        await asyncio.sleep(0.05)
    assert worker.retained_sale_count >= 1


@pytest.mark.asyncio
async def test_persistence_runtime_shutdown_raises_not_safe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PersistenceRuntime.shutdown must not look successful with memory-only sales."""
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    db = tmp_path / "runtime.db"
    events = EventBus()
    runtime = await start_persistence(
        database_url=f"sqlite+aiosqlite:///{db}",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        addresses=(1,),
        events=events,
        simulated=True,
    )
    await _force_undurable_retained(runtime.worker, tx="tx-runtime-flush")
    assert runtime.worker.retained_sale_count >= 1

    with pytest.raises(PersistFlushIncompleteError) as exc_info:
        await runtime.shutdown(flush_timeout_s=1.0)

    msg = exc_info.value.operator_message()
    assert "SALE_DURABILITY_NOT_SAFE" in msg
    assert "LOST" in msg
    assert "tx-runtime-flush" in msg
    assert "retained_undurable" in exc_info.value.reasons


@pytest.mark.asyncio
async def test_stop_flush_incomplete_on_queue_join_timeout_blocked_handler(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Blocked CRITICAL handler → queue.join timeout → incomplete flush."""
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    store = PersistRecoveryStore(tmp_path / "join.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=store)
    gate = asyncio.Event()
    entered = asyncio.Event()
    worker.start()

    async def blocked(_payload: dict) -> None:
        entered.set()
        await gate.wait()

    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-join-block"),
        handler=blocked,
        priority=PersistPriority.CRITICAL,
    )
    await asyncio.wait_for(entered.wait(), timeout=2.0)

    with pytest.raises(PersistFlushIncompleteError) as exc_info:
        await worker.stop(flush=True, timeout_s=0.15)
    assert "queue_join_timeout" in exc_info.value.reasons
    assert any("tx-join-block" in k for k in exc_info.value.identity_keys)
    assert "SALE_DURABILITY_NOT_SAFE" in exc_info.value.operator_message()
    gate.set()


@pytest.mark.asyncio
async def test_stop_flush_incomplete_on_pending_write_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Blocked durable write-ahead → pending_write_timeout → incomplete flush."""
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    store = PersistRecoveryStore(tmp_path / "write.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=store)
    release_write = threading.Event()
    enter_write = threading.Event()
    real_upsert = store.upsert

    def gated_upsert(**kwargs):
        enter_write.set()
        if not release_write.wait(timeout=5.0):
            raise TimeoutError("write still gated")
        return real_upsert(**kwargs)

    store.upsert = gated_upsert  # type: ignore[method-assign]
    worker.start()

    async def handler(_payload: dict) -> None:
        return None

    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-write-block"),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    assert await asyncio.to_thread(enter_write.wait, 2.0)

    with pytest.raises(PersistFlushIncompleteError) as exc_info:
        await worker.stop(flush=True, timeout_s=0.15)
    assert "pending_write_timeout" in exc_info.value.reasons
    assert any("tx-write-block" in k for k in exc_info.value.identity_keys)
    release_write.set()


@pytest.mark.asyncio
async def test_stop_flush_incomplete_on_retained_spill_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stalled final _retry_retained_sales → retained_spill_timeout → incomplete."""
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    store = PersistRecoveryStore(tmp_path / "retained-spill.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=store)
    release_spill = threading.Event()
    enter_spill = threading.Event()
    real_upsert = store.upsert

    def stalled_upsert(**kwargs):
        enter_spill.set()
        if not release_spill.wait(timeout=5.0):
            raise TimeoutError("retained spill still gated")
        return real_upsert(**kwargs)

    store.upsert = stalled_upsert  # type: ignore[method-assign]

    async def handler(_payload: dict) -> None:
        return None

    key = "state_changed:1:tx-spill-stall:FILLING_COMPLETE"
    from intelipump_fdc.services.persistence_worker import _RetainedSale

    worker._retained_sales[key] = _RetainedSale(
        kind="state_changed",
        payload=_completion_payload("tx-spill-stall"),
        identity_key=key,
        handler=handler,
        last_error="prior_disk_down",
        attempt=2,
    )
    worker._inflight_identities.add(key)
    # Do not start background requeue — only the stop() final spill should run.

    with pytest.raises(PersistFlushIncompleteError) as exc_info:
        await worker.stop(flush=True, timeout_s=0.2)
    assert "retained_spill_timeout" in exc_info.value.reasons
    assert exc_info.value.retained_count >= 1
    assert key in exc_info.value.identity_keys
    assert "SALE_DURABILITY_NOT_SAFE" in exc_info.value.operator_message()
    # Cleanup completed (worker tasks cleared) despite incomplete flush.
    assert worker._task is None
    release_spill.set()
    # Confirm the spill did start (stalled under the deadline).
    assert enter_spill.is_set()


@pytest.mark.asyncio
async def test_api_lifespan_incomplete_flush_does_not_claim_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """API lifespan must not log api_shutdown_complete when sales are undurable."""
    get_settings.cache_clear()
    monkeypatch.setenv(
        "INTELIPUMP_DATABASE__URL", f"sqlite+aiosqlite:///{tmp_path / 'api.db'}"
    )
    monkeypatch.setenv("INTELIPUMP_CONTROLLER__MODE", "LISTEN_ONLY")
    monkeypatch.setenv("INTELIPUMP_API__START_CONTROLLER_LOOP", "false")
    monkeypatch.setenv("INTELIPUMP_MQTT__ENABLED", "false")
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    get_settings.cache_clear()

    info_events: list[str] = []
    error_events: list[str] = []

    import intelipump_fdc.api.lifespan as lifespan_mod

    real_info = lifespan_mod.logger.info
    real_error = lifespan_mod.logger.error

    def track_info(event: str | None = None, *args, **kwargs):
        if isinstance(event, str):
            info_events.append(event)
        return real_info(event, *args, **kwargs)

    def track_error(event: str | None = None, *args, **kwargs):
        if isinstance(event, str):
            error_events.append(event)
        return real_error(event, *args, **kwargs)

    monkeypatch.setattr(lifespan_mod.logger, "info", track_info)
    monkeypatch.setattr(lifespan_mod.logger, "error", track_error)

    app: FastAPI = create_app()
    with pytest.raises(PersistFlushIncompleteError):
        async with app_lifespan(app):
            state = app.state.app_state
            worker = state.worker
            assert worker is not None
            await _force_undurable_retained(worker, tx="tx-api-flush")
            assert worker.retained_sale_count >= 1

    assert "api_shutdown_sale_durability_incomplete" in error_events
    assert "api_shutdown_complete" not in info_events
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_api_lifespan_preserves_body_exception_over_flush_incomplete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Body exception must propagate; flush incomplete must not suppress it."""
    get_settings.cache_clear()
    monkeypatch.setenv(
        "INTELIPUMP_DATABASE__URL", f"sqlite+aiosqlite:///{tmp_path / 'api2.db'}"
    )
    monkeypatch.setenv("INTELIPUMP_CONTROLLER__MODE", "LISTEN_ONLY")
    monkeypatch.setenv("INTELIPUMP_API__START_CONTROLLER_LOOP", "false")
    monkeypatch.setenv("INTELIPUMP_MQTT__ENABLED", "false")
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    get_settings.cache_clear()

    error_events: list[str] = []
    info_events: list[str] = []
    import intelipump_fdc.api.lifespan as lifespan_mod

    real_info = lifespan_mod.logger.info
    real_error = lifespan_mod.logger.error

    def track_info(event: str | None = None, *args, **kwargs):
        if isinstance(event, str):
            info_events.append(event)
        return real_info(event, *args, **kwargs)

    def track_error(event: str | None = None, *args, **kwargs):
        if isinstance(event, str):
            error_events.append(event)
        return real_error(event, *args, **kwargs)

    monkeypatch.setattr(lifespan_mod.logger, "info", track_info)
    monkeypatch.setattr(lifespan_mod.logger, "error", track_error)

    app: FastAPI = create_app()
    with pytest.raises(RuntimeError, match="body boom"):
        async with app_lifespan(app):
            state = app.state.app_state
            assert state.worker is not None
            await _force_undurable_retained(state.worker, tx="tx-api-body")
            raise RuntimeError("body boom")

    assert "api_shutdown_sale_durability_incomplete" in error_events
    assert "api_shutdown_complete" not in info_events
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_controller_shutdown_policy_exits_nonzero_on_undurable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Controller CLI finally-path: PersistFlushIncompleteError → SystemExit(1)."""
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    db = tmp_path / "cli.db"
    events = EventBus()
    persistence = await start_persistence(
        database_url=f"sqlite+aiosqlite:///{db}",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        addresses=(1,),
        events=events,
        simulated=True,
    )
    await _force_undurable_retained(persistence.worker, tx="tx-cli-flush")

    # Mirror controller/cli.py finally policy.
    with pytest.raises(SystemExit) as exit_info:
        try:
            await persistence.shutdown(flush_timeout_s=1.0)
        except PersistFlushIncompleteError as exc:
            assert "SALE_DURABILITY_NOT_SAFE" in exc.operator_message()
            raise SystemExit(1) from exc
    assert exit_info.value.code == 1
