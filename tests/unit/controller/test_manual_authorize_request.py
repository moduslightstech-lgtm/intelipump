"""Owned-lab AUTHORIZE-on-lift experiment helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from intelipump_fdc.controller.controller_loop import ControllerLoop, ControllerRuntime
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair


def _lab_safety() -> ControllerSafetyContext:
    return ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.LISTEN_ONLY,
        active_commands_enabled=False,
        require_physical_control_enable=True,
        physical_enable_present=False,
        allow_virtual_polling=True,
        allow_lab_simulator_commands=True,
        owned_lab_active_session=True,
    )


async def _loop() -> ControllerLoop:
    ctrl, _pump = create_memory_transport_pair()
    await ctrl.open()
    return ControllerLoop(
        ControllerRuntime(
            transport=ctrl,
            safety=_lab_safety(),
            config=PollSchedulerConfig(
                addresses=(1,),
                response_timeout_ms=50,
                inter_poll_delay_ms=0,
                idle_sleep_ms=0,
                max_retries=0,
            ),
        )
    )


@pytest.mark.asyncio
async def test_manual_authorize_request_file_consumed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("INTELIPUMP_AUTHORIZE_REQUEST_DIR", str(tmp_path))
    loop = await _loop()
    req = tmp_path / "authorize-1"
    req.write_text("")
    assert loop._consume_manual_authorize_request(1) is True
    assert not req.exists()
    assert loop._consume_manual_authorize_request(1) is False


@pytest.mark.asyncio
async def test_manual_authorize_missing_file_is_false(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("INTELIPUMP_AUTHORIZE_REQUEST_DIR", str(tmp_path))
    loop = await _loop()
    assert loop._consume_manual_authorize_request(1) is False


@pytest.mark.asyncio
async def test_arm_file_arms_for_next_lift(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("INTELIPUMP_AUTHORIZE_REQUEST_DIR", str(tmp_path))
    loop = await _loop()
    (tmp_path / "arm-1").write_text("")
    loop._refresh_arm_requests()
    assert 1 in loop._armed_for_lift
    assert not (tmp_path / "arm-1").exists()
    # Second refresh does not clear armed state.
    loop._refresh_arm_requests()
    assert 1 in loop._armed_for_lift
