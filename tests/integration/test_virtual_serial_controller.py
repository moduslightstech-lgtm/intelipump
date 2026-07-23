"""Virtual PTY integration tests (skipped without socat)."""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.protocol.dart.transport.serial import SerialConfig, SerialTransport
from intelipump_fdc.simulator.serial_bridge import (
    SerialBridgeConfig,
    SimulatorSerialBridge,
)

CONTROLLER_PTY = Path("/tmp/dart-controller")
PUMP_PTY = Path("/tmp/dart-pump")


def _socat_available() -> bool:
    return shutil.which("socat") is not None


@pytest.fixture
def virtual_serial():
    if not _socat_available():
        pytest.skip("socat not available")
    for path in (CONTROLLER_PTY, PUMP_PTY):
        if path.exists() or path.is_symlink():
            path.unlink(missing_ok=True)
    proc = subprocess.Popen(
        [
            "socat",
            "-d",
            "-d",
            f"PTY,raw,echo=0,link={CONTROLLER_PTY}",
            f"PTY,raw,echo=0,link={PUMP_PTY}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for _ in range(50):
        if CONTROLLER_PTY.exists() and PUMP_PTY.exists():
            break
        time.sleep(0.05)
    else:
        proc.kill()
        pytest.skip("PTY links not created")
    try:
        yield proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
        for path in (CONTROLLER_PTY, PUMP_PTY):
            if path.exists() or path.is_symlink():
                path.unlink(missing_ok=True)


@pytest.mark.serial_integration
@pytest.mark.asyncio
async def test_virtual_serial_controller_simulator(virtual_serial) -> None:
    del virtual_serial
    bridge = SimulatorSerialBridge(
        SerialTransport(SerialConfig(device=str(PUMP_PTY))),
        config=SerialBridgeConfig(idle_sleep_ms=2, sim_time_step_ms=20),
    )
    runtime = ControllerRuntime(
        transport=SerialTransport(SerialConfig(device=str(CONTROLLER_PTY))),
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1, 2),
            response_timeout_ms=100,
            inter_poll_delay_ms=5,
            idle_sleep_ms=10,
            max_retries=1,
        ),
    )
    loop = ControllerLoop(runtime)

    async def run_ctrl() -> None:
        await loop.run(duration_s=3.0)
        bridge.request_stop()

    await asyncio.gather(bridge.run(), run_ctrl())
    summary = loop.summary()
    totals = summary["totals"]
    assert isinstance(totals, dict)
    assert totals["poll_count"] > 0
    assert totals["eot_count"] + totals["data_count"] > 0
