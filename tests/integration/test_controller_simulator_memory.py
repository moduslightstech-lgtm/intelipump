"""Full controller ↔ simulator lifecycle over memory transport."""

from __future__ import annotations

import asyncio

import pytest

from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.simulator.config import SimulatorConfig
from intelipump_fdc.simulator.serial_bridge import (
    SerialBridgeConfig,
    SimulatorSerialBridge,
)
from intelipump_fdc.simulator.session import SimulatorSession


@pytest.mark.asyncio
async def test_full_lifecycle_memory_transport() -> None:
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

    async def run_ctrl() -> None:
        await loop.run(duration_s=1.5)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    summary = loop.summary()
    totals = summary["totals"]
    assert isinstance(totals, dict)
    assert totals["poll_count"] > 0
    assert totals["eot_count"] + totals["data_count"] > 0
    # At least one pump should have left DISCONNECTED.
    pumps = summary["pumps"]
    assert isinstance(pumps, dict)
    states = [pumps[a]["state"] for a in pumps]
    assert any(s != PumpState.DISCONNECTED.value for s in states)


@pytest.mark.asyncio
async def test_offline_pump_does_not_block_other() -> None:
    ctrl_t, sim_t = create_memory_transport_pair()
    # Simulator only has address 1; controller polls 1 and 9.
    from intelipump_fdc.simulator.config import PumpConfig

    sim = SimulatorSession(
        SimulatorConfig(pumps=(PumpConfig(pump_id="fp-1", dart_address=1),))
    )
    bridge = SimulatorSerialBridge(
        sim_t,
        simulator=sim,
        config=SerialBridgeConfig(idle_sleep_ms=1, sim_time_step_ms=5),
    )
    runtime = ControllerRuntime(
        transport=ctrl_t,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1, 9),
            response_timeout_ms=50,
            inter_poll_delay_ms=1,
            idle_sleep_ms=2,
            max_retries=0,
            max_consecutive_timeouts=3,
        ),
    )
    loop = ControllerLoop(runtime)

    async def run_ctrl() -> None:
        await loop.run(duration_s=1.0)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    assert (
        loop.sessions[1].state.stats.eot_count + loop.sessions[1].state.stats.data_count
        > 0
    )
    assert loop.sessions[9].state.stats.timeout_count > 0


@pytest.mark.asyncio
async def test_controller_cancellation_clean_shutdown() -> None:
    ctrl_t, sim_t = create_memory_transport_pair()
    bridge = SimulatorSerialBridge(
        sim_t, config=SerialBridgeConfig(idle_sleep_ms=1, sim_time_step_ms=5)
    )
    runtime = ControllerRuntime(
        transport=ctrl_t,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=50,
            idle_sleep_ms=5,
        ),
    )
    loop = ControllerLoop(runtime)

    async def stop_soon() -> None:
        await asyncio.sleep(0.2)
        loop.request_stop()
        bridge.request_stop()

    await asyncio.gather(bridge.run(), loop.run(duration_s=10), stop_soon())
    assert not ctrl_t.is_open
    assert not sim_t.is_open
