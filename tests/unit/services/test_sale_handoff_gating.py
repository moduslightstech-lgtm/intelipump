"""Per-sale / per-address durable handoff gating and storage-recovery settle."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest

from intelipump_fdc.controller.controller_loop import ControllerLoop, ControllerRuntime
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.services.persist_recovery import (
    PersistRecoveryIOError,
    PersistRecoveryStore,
)
from intelipump_fdc.services.persistence_worker import (
    PersistPriority,
    PersistenceWorker,
    identity_address,
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


def _loop(addresses: tuple[int, ...] = (1, 2)) -> ControllerLoop:
    """Memory transport allows only legacy iGEM addresses 1 and 2."""
    ctrl, _pump = create_memory_transport_pair()
    return ControllerLoop(
        ControllerRuntime(
            transport=ctrl,
            safety=ControllerSafetyContext(
                environment="PRODUCTION",
                mode=ControllerMode.LISTEN_ONLY,
                active_commands_enabled=False,
                require_physical_control_enable=True,
                physical_enable_present=False,
                allow_virtual_polling=True,
                allow_lab_simulator_commands=False,
                owned_lab_active_session=False,
            ),
            config=PollSchedulerConfig(addresses=addresses),
            logical_nozzle_count=1,
        )
    )


def test_identity_address_parses_kind_addr_tx_event() -> None:
    assert identity_address("state_changed:5:tx-a:FILLING_COMPLETE") == 5
    assert identity_address("app_decoded:9:tx-b:COMPLETE") == 9
    assert identity_address(None) is None
    assert identity_address("nope") is None


def test_mark_sale_handoff_durable_rejects_foreign_identity() -> None:
    """One sale identity must not release another sale's address gate."""
    loop = _loop()
    loop.note_sale_handoff_identity(1, "state_changed:1:tx-a:FILLING_COMPLETE")
    loop.note_sale_handoff_identity(2, "state_changed:2:tx-b:FILLING_COMPLETE")
    loop.mark_sale_handoff_durable(
        1, identity_key="state_changed:1:tx-other:FILLING_COMPLETE"
    )
    assert 1 in loop._sale_handoff_pending
    assert loop._sale_handoff_identity[1] == "state_changed:1:tx-a:FILLING_COMPLETE"
    loop.mark_sale_handoff_durable(
        1, identity_key="state_changed:1:tx-a:FILLING_COMPLETE"
    )
    assert 1 not in loop._sale_handoff_pending
    assert 2 in loop._sale_handoff_pending


@pytest.mark.asyncio
async def test_storage_recovery_settles_handoff_gate_and_processes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failed handoff → storage returns → one process → address gate released."""
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    store = PersistRecoveryStore(tmp_path / "recover-gate.jsonl")
    worker = PersistenceWorker(
        maxsize=8, critical_max_attempts=2, recovery_store=store
    )
    real_upsert = store.upsert
    disk_up = {"ok": False}

    def gated(**kwargs):
        if not disk_up["ok"]:
            raise PersistRecoveryIOError("disk down")
        return real_upsert(**kwargs)

    store.upsert = gated  # type: ignore[method-assign]
    handled: list[str] = []
    handoffs: list[tuple[str, bool]] = []

    async def handler(payload: dict) -> None:
        handled.append(str(payload["payload"]["active_transaction_id"]))

    def on_handoff(identity_key: str, durable_ok: bool) -> None:
        handoffs.append((identity_key, durable_ok))

    worker.on_handoff(on_handoff)
    worker.start()
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-recover-gate", address=1),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    for _ in range(80):
        if worker.retained_sale_count >= 1:
            break
        await asyncio.sleep(0.05)
    assert worker.retained_sale_count >= 1
    assert worker.is_address_handoff_pending(1) is True
    assert handled == []
    assert any(ok is False for _, ok in handoffs)

    disk_up["ok"] = True
    for _ in range(100):
        if worker.processed >= 1 and worker.retained_sale_count == 0:
            break
        await asyncio.sleep(0.05)

    assert handled == ["tx-recover-gate"]
    assert worker.is_address_handoff_pending(1) is False
    assert any(ok is True for _, ok in handoffs)
    assert sum(1 for _, ok in handoffs if ok) >= 1
    await worker.stop(flush=True, timeout_s=2.0)
    assert store.pending_count() == 0


@pytest.mark.asyncio
async def test_repeated_completion_while_write_blocked_single_callback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Blocked write + repeated submit → one durable identity, one completion."""
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    store = PersistRecoveryStore(tmp_path / "repeat.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=store)
    release = threading.Event()
    enter = threading.Event()
    real_upsert = store.upsert
    upsert_calls = {"n": 0}

    def gated(**kwargs):
        upsert_calls["n"] += 1
        enter.set()
        if not release.wait(timeout=3.0):
            raise TimeoutError("write gate")
        return real_upsert(**kwargs)

    store.upsert = gated  # type: ignore[method-assign]
    handled: list[str] = []
    handoffs: list[tuple[str, bool]] = []

    async def handler(payload: dict) -> None:
        handled.append(str(payload["payload"]["active_transaction_id"]))

    worker.on_handoff(lambda k, ok: handoffs.append((k, ok)))
    worker.start()
    payload = _completion_payload("tx-repeat", address=3)
    for _ in range(5):
        worker.submit(
            kind="state_changed",
            payload=payload,
            handler=handler,
            priority=PersistPriority.CRITICAL,
        )
    assert await asyncio.to_thread(enter.wait, 2.0)
    assert worker.handoff_pending_count == 1
    assert upsert_calls["n"] == 1
    release.set()
    for _ in range(80):
        if worker.processed >= 1:
            break
        await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=2.0)
    assert handled == ["tx-repeat"]
    assert sum(1 for _, ok in handoffs if ok) == 1
    assert store.pending_count() == 0


@pytest.mark.asyncio
async def test_one_sale_does_not_release_other_address_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Durable settle for addr A must not clear RESET gate on addr B."""
    monkeypatch.setenv("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "1")
    store = PersistRecoveryStore(tmp_path / "addr-gate.jsonl")
    worker = PersistenceWorker(maxsize=8, recovery_store=store)
    release_b = threading.Event()
    enter_b = threading.Event()
    real_upsert = store.upsert

    def gated(**kwargs):
        key = str(kwargs.get("identity_key") or "")
        if ":2:" in key:
            enter_b.set()
            if not release_b.wait(timeout=3.0):
                raise TimeoutError("b still gated")
        return real_upsert(**kwargs)

    store.upsert = gated  # type: ignore[method-assign]

    loop = _loop((1, 2))
    loop.note_sale_handoff_identity(1, "state_changed:1:tx-a:FILLING_COMPLETE")
    loop.note_sale_handoff_identity(2, "state_changed:2:tx-b:FILLING_COMPLETE")

    def _on_handoff(identity_key: str, durable_ok: bool) -> None:
        addr = identity_address(identity_key)
        if addr is None:
            return
        if durable_ok:
            loop.mark_sale_handoff_durable(addr, identity_key=identity_key)

    worker.on_handoff(_on_handoff)

    async def handler(_payload: dict) -> None:
        return None

    worker.start()
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-a", address=1),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    worker.submit(
        kind="state_changed",
        payload=_completion_payload("tx-b", address=2),
        handler=handler,
        priority=PersistPriority.CRITICAL,
    )
    assert await asyncio.to_thread(enter_b.wait, 2.0)
    for _ in range(80):
        if not worker.is_address_handoff_pending(1):
            break
        await asyncio.sleep(0.05)
    assert worker.is_address_handoff_pending(1) is False
    assert worker.is_address_handoff_pending(2) is True
    assert 1 not in loop._sale_handoff_pending
    assert 2 in loop._sale_handoff_pending
    release_b.set()
    for _ in range(80):
        if worker.processed >= 2:
            break
        await asyncio.sleep(0.05)
    await worker.stop(flush=True, timeout_s=2.0)
    assert 2 not in loop._sale_handoff_pending
