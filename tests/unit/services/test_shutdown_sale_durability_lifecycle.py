"""Controller/API lifecycle when sale durability flush is incomplete at shutdown."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi import FastAPI

from intelipump_fdc.api.app import create_app
from intelipump_fdc.api.lifespan import app_lifespan
from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.services.lab_persistence import start_persistence
from intelipump_fdc.services.persist_recovery import PersistRecoveryIOError
from intelipump_fdc.services.persistence_worker import (
    PersistFlushIncompleteError,
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
