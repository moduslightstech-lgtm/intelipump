"""Persistence worker and recovery service tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.errors import PersistenceQueueFullError
from intelipump_fdc.persistence.migrations import init_schema
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_worker import PersistenceWorker, PersistPriority
from intelipump_fdc.services.pump_state_service import PumpStateService
from intelipump_fdc.services.recovery_service import RecoveryService
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService
from intelipump_fdc.state_machine.models import PumpContext

STATION = "InteliPump-US-Lab"


@pytest.mark.asyncio
async def test_persistence_queue_backpressure() -> None:
    worker = PersistenceWorker(maxsize=1)
    worker.start()
    blocker = asyncio.Event()

    async def slow(_payload: dict) -> None:
        await blocker.wait()

    worker.submit(
        kind="a", payload={}, handler=slow, priority=PersistPriority.NORMAL
    )
    # Fill the queue while first job is running
    await asyncio.sleep(0.05)
    worker.submit(
        kind="b", payload={}, handler=slow, priority=PersistPriority.NORMAL
    )
    # Next NORMAL should drop
    worker.submit(
        kind="c", payload={}, handler=slow, priority=PersistPriority.NORMAL
    )
    assert worker.dropped_normal >= 1
    with pytest.raises(PersistenceQueueFullError):
        worker.submit(
            kind="crit",
            payload={},
            handler=slow,
            priority=PersistPriority.CRITICAL,
        )
    blocker.set()
    await worker.stop(flush=True, timeout_s=2.0)


@pytest.mark.asyncio
async def test_clean_shutdown_flush() -> None:
    worker = PersistenceWorker(maxsize=16)
    worker.start()
    seen: list[str] = []

    async def handler(payload: dict) -> None:
        seen.append(str(payload["id"]))

    for i in range(5):
        worker.submit(
            kind="n",
            payload={"id": str(i)},
            handler=handler,
            priority=PersistPriority.NORMAL,
        )
    await worker.stop(flush=True, timeout_s=2.0)
    assert seen == ["0", "1", "2", "3", "4"]


@pytest.mark.asyncio
async def test_restart_with_active_transaction(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'r1.db'}"
    engine = create_engine(url)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-1", dart_address=1
        )
        await TransactionService(uow).begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump.id,
                transaction_uuid="open-tx",
                nozzle_id=1,
                raw_price=None,
                price_decimals=None,
                volume_decimals=None,
                amount_decimals=None,
                simulated=True,
                environment="LAB",
            )
        )
        await PumpStateService(uow).persist_context(
            pump_db_id=pump.id,
            context=PumpContext(
                pump_id="pump-1",
                dart_address=1,
                current_state=PumpState.FILLING,
                active_transaction_id="open-tx",
                communication_healthy=True,
                state_version=3,
            ),
        )
    await dispose_engine(engine)

    engine2 = create_engine(url)
    factory2 = create_session_factory(engine2)
    report = await RecoveryService(
        engine2, factory2, station_id=STATION, environment="LAB"
    ).recover()
    assert "open-tx" in report.unresolved_transactions
    assert report.pump_contexts["pump-1"].current_state is PumpState.FILLING
    assert report.pump_contexts["pump-1"].communication_healthy is False
    await dispose_engine(engine2)


@pytest.mark.asyncio
async def test_restart_pending_authorize_not_replayed(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'r2.db'}"
    engine = create_engine(url)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    async with unit_of_work(factory) as uow:
        await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-1", dart_address=1
        )
        await uow.commands.create(
            correlation_id="auth-pending",
            station_id=STATION,
            pump_id=None,
            command_type="AUTHORIZE",
            status="PENDING",
            idempotency_class="NON_IDEMPOTENT",
            simulator_only=True,
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
    await dispose_engine(engine)

    engine2 = create_engine(url)
    factory2 = create_session_factory(engine2)
    report = await RecoveryService(
        engine2, factory2, station_id=STATION, environment="LAB"
    ).recover()
    assert "auth-pending" in report.commands_needing_reconciliation
    async with unit_of_work(factory2) as uow:
        cmd = await uow.commands.get("auth-pending")
        assert cmd is not None
        assert cmd.status == "NEEDS_RECONCILIATION"
    await dispose_engine(engine2)


@pytest.mark.asyncio
async def test_restart_expired_simulator_request(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'r3.db'}"
    engine = create_engine(url)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    async with unit_of_work(factory) as uow:
        await uow.commands.create(
            correlation_id="expired-read",
            station_id=STATION,
            pump_id=None,
            command_type="READ_STATUS",
            status="PENDING",
            idempotency_class="IDEMPOTENT",
            simulator_only=True,
            expires_at=datetime.now(UTC) - timedelta(minutes=1),
        )
    await dispose_engine(engine)

    engine2 = create_engine(url)
    factory2 = create_session_factory(engine2)
    report = await RecoveryService(
        engine2, factory2, station_id=STATION, environment="LAB"
    ).recover()
    assert "expired-read" in report.commands_expired
    await dispose_engine(engine2)


@pytest.mark.asyncio
async def test_restart_without_communication(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'r4.db'}"
    engine = create_engine(url)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        await PumpStateService(uow).persist_context(
            pump_db_id=pump.id,
            context=PumpContext(
                pump_id="pump-2",
                dart_address=2,
                current_state=PumpState.READY,
                communication_healthy=True,
                state_version=1,
            ),
        )
    await dispose_engine(engine)

    engine2 = create_engine(url)
    factory2 = create_session_factory(engine2)
    report = await RecoveryService(
        engine2, factory2, station_id=STATION, environment="LAB"
    ).recover()
    assert report.pump_contexts["pump-2"].communication_healthy is False
    assert any("forces unhealthy" in w for w in report.warnings)
    await dispose_engine(engine2)


@pytest.mark.asyncio
async def test_recovery_report_and_completed_survives(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'r5.db'}"
    engine = create_engine(url)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-1", dart_address=1
        )
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump.id,
                transaction_uuid="done-tx",
                nozzle_id=1,
                raw_price=None,
                price_decimals=None,
                volume_decimals=None,
                amount_decimals=None,
                simulated=True,
                environment="LAB",
            )
        )
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="done-tx",
                source_completion_key="done-key",
                raw_volume=100,
                raw_amount=200,
            )
        )
    await dispose_engine(engine)

    engine2 = create_engine(url)
    factory2 = create_session_factory(engine2)
    report = await RecoveryService(
        engine2, factory2, station_id=STATION, environment="LAB"
    ).recover()
    assert report.schema_version >= 1
    assert "done-tx" not in report.unresolved_transactions
    async with unit_of_work(factory2) as uow:
        tx = await uow.transactions.get_by_uuid("done-tx")
        assert tx is not None
        assert tx.status == "COMPLETED"
        assert await uow.transactions.count_completed(station_id=STATION) == 1
    d = report.to_dict()
    assert "schema_version" in d
    await dispose_engine(engine2)
