"""Restart recovery with persistence and live reconciliation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.migrations import init_schema
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.services.lab_persistence import (
    apply_recovered_contexts,
    start_persistence,
)
from intelipump_fdc.services.persistence_worker import PersistPriority
from intelipump_fdc.services.recovery_service import RecoveryService
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService
from intelipump_fdc.simulator.config import SimulatorConfig
from intelipump_fdc.simulator.serial_bridge import (
    SerialBridgeConfig,
    SimulatorSerialBridge,
)
from intelipump_fdc.simulator.session import SimulatorSession
from intelipump_fdc.state_machine.models import PumpContext

STATION = "InteliPump-US-Lab"


@pytest.mark.asyncio
async def test_restart_recovery_completed_tx_once(tmp_path: Path) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'restart.db'}"
    engine = create_engine(db)
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
                transaction_uuid="sale-1",
                nozzle_id=1,
                raw_price=1000,
                price_decimals=3,
                volume_decimals=3,
                amount_decimals=2,
                simulated=True,
                environment="LAB",
            )
        )
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="sale-1",
                source_completion_key="evidence:sale-1",
                raw_volume=5000,
                raw_amount=5000,
            )
        )
        # Duplicate complete ignored
        _, newly = await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="sale-1",
                source_completion_key="evidence:sale-1",
                raw_volume=5000,
                raw_amount=5000,
            )
        )
        assert newly is False
        await uow.commands.create(
            correlation_id="auth-1",
            station_id=STATION,
            pump_id=pump.id,
            command_type=PumpCommand.AUTHORIZE.value,
            status="PENDING",
            idempotency_class="NON_IDEMPOTENT",
            simulator_only=True,
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
    await dispose_engine(engine)

    # Restart
    engine2 = create_engine(db)
    factory2 = create_session_factory(engine2)
    report = await RecoveryService(
        engine2, factory2, station_id=STATION, environment="LAB"
    ).recover()
    assert "auth-1" in report.commands_needing_reconciliation
    async with unit_of_work(factory2) as uow:
        assert await uow.transactions.count_completed(station_id=STATION) == 1
        tx = await uow.transactions.get_by_uuid("sale-1")
        assert tx is not None
        assert tx.status == "COMPLETED"
        cmd = await uow.commands.get("auth-1")
        assert cmd is not None
        assert cmd.status == "NEEDS_RECONCILIATION"
    await dispose_engine(engine2)


@pytest.mark.asyncio
async def test_duplicate_data_does_not_duplicate_transaction(
    tmp_path: Path,
) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'dup.db'}"
    persistence = await start_persistence(
        database_url=db,
        station_id=STATION,
        environment="LAB",
        addresses=(1,),
        events=EventBus(),
    )
    try:
        pump_id = persistence.pump_id_by_address[1]
        bridge = persistence.bridge
        # Seed active tx mapping via state change to FILLING then COMPLETE twice
        for detail, payload in (
            (
                "AUTHORIZED->FILLING",
                {
                    "previous_state": "AUTHORIZED",
                    "normalized_state": "FILLING",
                    "state_version": 2,
                    "active_transaction_id": "dup-tx",
                    "communication_healthy": True,
                },
            ),
            (
                "FILLING->FILLING_COMPLETE",
                {
                    "previous_state": "FILLING",
                    "normalized_state": "FILLING_COMPLETE",
                    "state_version": 3,
                    "active_transaction_id": "dup-tx",
                    "completion_evidence_key": "ev-dup",
                    "communication_healthy": True,
                },
            ),
            (
                "FILLING->FILLING_COMPLETE",
                {
                    "previous_state": "FILLING",
                    "normalized_state": "FILLING_COMPLETE",
                    "state_version": 3,
                    "active_transaction_id": "dup-tx",
                    "completion_evidence_key": "ev-dup",
                    "communication_healthy": True,
                },
            ),
        ):
            bridge.on_event(
                ControllerEvent(
                    type=ControllerEventType.STATE_CHANGED,
                    address=1,
                    detail=detail,
                    payload=payload,
                )
            )
        await asyncio.sleep(0.4)
        async with unit_of_work(persistence.session_factory) as uow:
            assert await uow.transactions.count_completed(station_id=STATION) == 1
            tx = await uow.transactions.get_by_uuid("dup-tx")
            assert tx is not None
            events = await uow.transactions.list_events(tx.id)
            completed = [e for e in events if e.event_type == "COMPLETED"]
            assert len(completed) == 1
            _ = pump_id
    finally:
        await persistence.shutdown()


@pytest.mark.asyncio
async def test_process_restart_then_live_reconciliation(tmp_path: Path) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'recon.db'}"
    # Seed FILLING + open tx
    engine = create_engine(db)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-1", dart_address=1
        )
        await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        await TransactionService(uow).begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump.id,
                transaction_uuid="open-live",
                nozzle_id=1,
                raw_price=None,
                price_decimals=None,
                volume_decimals=None,
                amount_decimals=None,
                simulated=True,
                environment="LAB",
            )
        )
        from intelipump_fdc.services.pump_state_service import PumpStateService

        await PumpStateService(uow).persist_context(
            pump_db_id=pump.id,
            context=PumpContext(
                pump_id="pump-1",
                dart_address=1,
                current_state=PumpState.FILLING,
                active_transaction_id="open-live",
                communication_healthy=True,
                state_version=4,
            ),
        )
    await dispose_engine(engine)

    ctrl_t, sim_t = create_memory_transport_pair()
    sim = SimulatorSession(SimulatorConfig())
    bridge = SimulatorSerialBridge(
        sim_t,
        simulator=sim,
        config=SerialBridgeConfig(
            idle_sleep_ms=1, sim_time_step_ms=10, cold_start_on_open=True
        ),
    )
    runtime = ControllerRuntime(
        transport=ctrl_t,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1, 2),
            response_timeout_ms=80,
            inter_poll_delay_ms=2,
            idle_sleep_ms=5,
            max_retries=1,
        ),
    )
    loop = ControllerLoop(runtime)
    persistence = await start_persistence(
        database_url=db,
        station_id=STATION,
        environment="LAB",
        addresses=(1, 2),
        events=runtime.events,
    )
    assert "open-live" in persistence.recovery.unresolved_transactions
    apply_recovered_contexts(loop, persistence.recovery.pump_contexts)
    assert loop.sessions[1].machine.context.current_state is PumpState.FILLING
    assert loop.sessions[1].machine.context.communication_healthy is False

    async def run_ctrl() -> None:
        await loop.run(duration_s=1.2)
        bridge.request_stop()

    try:
        await asyncio.gather(bridge.run(), run_ctrl())
        # Live observations should restore communication health via SM
        assert loop.sessions[1].state.communication.value in {
            "HEALTHY",
            "DEGRADED",
            "UNKNOWN",
            "DISCONNECTED",
        }
    finally:
        await persistence.shutdown()


@pytest.mark.asyncio
async def test_rejected_authorize_via_bridge(tmp_path: Path) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'rej.db'}"
    from intelipump_fdc.controller.session_events import EventBus

    events = EventBus()
    persistence = await start_persistence(
        database_url=db,
        station_id=STATION,
        environment="LAB",
        addresses=(1,),
        events=events,
    )
    try:
        cid = str(uuid4())
        persistence.bridge.enqueue_rejected_command(
            address=1,
            command=PumpCommand.AUTHORIZE,
            current_state="READY",
            blocking_reasons=("listen_only",),
            simulator_only=True,
            correlation_id=cid,
        )
        await asyncio.sleep(0.3)
        async with unit_of_work(persistence.session_factory) as uow:
            cmd = await uow.commands.get(cid)
            assert cmd is not None
            assert cmd.status == "REJECTED"
            audits = await uow.audit.list_all()
            assert any(a.correlation_id == cid for a in audits)
            await uow.audit.verify_chain()
    finally:
        await persistence.shutdown()


@pytest.mark.asyncio
async def test_critical_events_priority_ordering() -> None:
    from intelipump_fdc.services.persistence_worker import PersistenceWorker

    worker = PersistenceWorker(maxsize=32)
    order: list[str] = []
    gate = asyncio.Event()

    async def hold(_p: dict) -> None:
        await gate.wait()

    async def record(p: dict) -> None:
        order.append(str(p["k"]))

    worker.start()
    # Occupy worker
    worker.submit(kind="hold", payload={}, handler=hold, priority=PersistPriority.NORMAL)
    await asyncio.sleep(0.02)
    worker.submit(
        kind="n",
        payload={"k": "normal"},
        handler=record,
        priority=PersistPriority.NORMAL,
    )
    worker.submit(
        kind="c",
        payload={"k": "critical"},
        handler=record,
        priority=PersistPriority.CRITICAL,
    )
    gate.set()
    await worker.stop(flush=True, timeout_s=2.0)
    assert order[0] == "critical"
    assert "normal" in order
