"""API + controller + simulator lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from intelipump_fdc.api.app import create_app
from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.services.lab_persistence import apply_recovered_contexts
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
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


@pytest.mark.asyncio
async def test_api_controller_simulator_and_transaction_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    get_settings.cache_clear()
    db = tmp_path / "life.db"
    monkeypatch.setenv("INTELIPUMP_DATABASE__URL", f"sqlite+aiosqlite:///{db}")
    app = create_app()
    with TestClient(app) as client:
        state = app.state.app_state
        # Seed a completed transaction via shared factory (same engine as API)
        async with unit_of_work(state.session_factory) as uow:
            pump = (
                await uow.pumps.list_for_station(state.settings.controller.station_id)
            )[0]
            svc = TransactionService(uow)
            await svc.begin(
                BeginTransactionRequest(
                    station_id=state.settings.controller.station_id,
                    pump_db_id=pump.id,
                    transaction_uuid="life-tx",
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
                    transaction_uuid="life-tx",
                    source_completion_key="life-key",
                    raw_volume=5000,
                    raw_amount=5000,
                )
            )
            _, newly = await svc.complete(
                CompleteTransactionRequest(
                    transaction_uuid="life-tx",
                    source_completion_key="life-key",
                    raw_volume=5000,
                    raw_amount=5000,
                )
            )
            assert newly is False

        # Live event publish
        state.broker.publish_typed(
            __import__(
                "intelipump_fdc.events.models", fromlist=["LiveEventType"]
            ).LiveEventType.TRANSACTION_COMPLETED,
            station_id=state.settings.controller.station_id,
            environment="LAB",
            simulated=True,
            transaction_id="life-tx",
        )

        r = client.get("/api/v1/transactions", params={"status": "COMPLETED"})
        assert r.status_code == 200
        items = [
            i for i in r.json()["items"] if i["transaction_uuid"] == "life-tx"
        ]
        assert len(items) == 1

        health = client.get("/api/v1/controller/health")
        assert health.status_code == 200
        assert health.json()["last_recovery_report"] is not None
        assert health.json()["schema_version"] == 1

        # Attach memory controller briefly
        ctrl_t, sim_t = create_memory_transport_pair()
        sim = SimulatorSession(SimulatorConfig())
        bridge = SimulatorSerialBridge(
            sim_t,
            simulator=sim,
            config=SerialBridgeConfig(idle_sleep_ms=1, sim_time_step_ms=5),
        )
        runtime = ControllerRuntime(
            transport=ctrl_t,
            safety=default_lab_safety(),
            config=PollSchedulerConfig(
                addresses=(1, 2),
                response_timeout_ms=50,
                inter_poll_delay_ms=1,
                idle_sleep_ms=2,
            ),
        )
        # Wire live broker into a transient persistence bridge subscriber
        PersistenceBridge(
            session_factory=state.session_factory,
            station_id=state.settings.controller.station_id,
            environment="LAB",
            simulated=True,
            worker=state.worker,  # type: ignore[arg-type]
            pump_id_by_address=state.pump_id_by_address,
            logical_by_address={1: "pump-1", 2: "pump-2"},
            events=runtime.events,
            live_broker=state.broker,
        ).attach()
        loop = ControllerLoop(runtime)
        state.controller_loop = loop
        apply_recovered_contexts(loop, state.recovery_report.pump_contexts)  # type: ignore[arg-type]

        async def run_pair() -> None:
            await loop.run(duration_s=0.8)
            bridge.request_stop()

        await asyncio.gather(bridge.run(), run_pair())
        metrics = client.get("/api/v1/controller/metrics")
        assert metrics.json()["poll_count"] > 0
    get_settings.cache_clear()


def test_api_shutdown_flushes_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv(
        "INTELIPUMP_DATABASE__URL", f"sqlite+aiosqlite:///{tmp_path / 'shut.db'}"
    )
    app = create_app()
    with TestClient(app) as client:
        assert client.get("/api/v1/controller/health").status_code == 200
    # Exiting context runs lifespan shutdown
    get_settings.cache_clear()
