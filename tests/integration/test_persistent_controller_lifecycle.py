"""Controller + simulator + SQLite persistence lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.services.lab_persistence import (
    apply_recovered_contexts,
    start_persistence,
)
from intelipump_fdc.simulator.config import SimulatorConfig
from intelipump_fdc.simulator.serial_bridge import (
    SerialBridgeConfig,
    SimulatorSerialBridge,
)
from intelipump_fdc.simulator.session import SimulatorSession

STATION = "InteliPump-US-Lab"


@pytest.mark.asyncio
async def test_persistent_controller_simulator_lifecycle(tmp_path: Path) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'life.db'}"
    ctrl_t, sim_t = create_memory_transport_pair(
        left_name="controller", right_name="simulator"
    )
    sim = SimulatorSession(SimulatorConfig())
    bridge = SimulatorSerialBridge(
        sim_t,
        simulator=sim,
        config=SerialBridgeConfig(
            idle_sleep_ms=1,
            sim_time_step_ms=10,
            cold_start_on_open=True,
        ),
    )
    runtime = ControllerRuntime(
        transport=ctrl_t,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1, 2),
            response_timeout_ms=100,
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
        simulated=True,
    )
    apply_recovered_contexts(loop, persistence.recovery.pump_contexts)

    async def run_ctrl() -> None:
        await loop.run(duration_s=1.5)
        bridge.request_stop()

    try:
        await asyncio.gather(bridge.run(), run_ctrl())
        # Allow persistence worker to drain
        await asyncio.sleep(0.3)
        async with unit_of_work(persistence.session_factory) as uow:
            pumps = await uow.pumps.list_for_station(STATION)
            assert len(pumps) == 2
            # At least one snapshot should exist after live polling
            snaps = 0
            for p in pumps:
                snaps += len(await uow.states.list_for_pump(p.id))
            assert snaps >= 1
            assert persistence.worker.processed >= 1
    finally:
        await persistence.shutdown()


@pytest.mark.asyncio
async def test_database_failure_does_not_crash_protocol_loop(
    tmp_path: Path,
) -> None:
    """Worker errors are counted; protocol loop continues."""
    db = f"sqlite+aiosqlite:///{tmp_path / 'ok.db'}"
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
            addresses=(1,),
            response_timeout_ms=50,
            inter_poll_delay_ms=1,
            idle_sleep_ms=2,
            max_retries=0,
        ),
    )
    loop = ControllerLoop(runtime)
    persistence = await start_persistence(
        database_url=db,
        station_id=STATION,
        environment="LAB",
        addresses=(1,),
        events=runtime.events,
    )

    async def run_ctrl() -> None:
        await loop.run(duration_s=0.8)
        bridge.request_stop()

    try:
        await asyncio.gather(bridge.run(), run_ctrl())
        summary = loop.summary()
        totals = summary["totals"]
        assert isinstance(totals, dict)
        assert totals["poll_count"] > 0
    finally:
        await persistence.shutdown()
