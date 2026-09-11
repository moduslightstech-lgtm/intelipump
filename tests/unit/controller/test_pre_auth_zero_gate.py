"""Pre-auth zero-meter gate must not block on retained previous-sale DC2."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from intelipump_fdc.controller.controller_loop import ControllerLoop, ControllerRuntime
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.controller.session_events import EventBus
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
    )


async def _loop() -> ControllerLoop:
    ctrl, _pump = create_memory_transport_pair()
    await ctrl.open()
    runtime = ControllerRuntime(
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
    return ControllerLoop(runtime)


@pytest.mark.asyncio
async def test_verify_zero_allows_when_no_fresh_dc2_after_invalidate() -> None:
    """Stale previous-sale totals must not withhold AUTHORIZE after RESET."""
    loop = await _loop()
    session = loop.sessions[1]
    # Simulate retained face from prior sale, then invalidate as post-RESET does.
    loop._last_dc2[1] = (46, 54050)
    session.state.filled_volume_raw = 46
    session.state.filled_amount_raw = 54050
    loop._last_dc2.pop(1, None)
    session.state.filled_volume_raw = 0
    session.state.filled_amount_raw = 0

    loop._write_frame = AsyncMock(return_value=(0.0, True))  # type: ignore[method-assign]
    loop._read_poll_session = AsyncMock(return_value="eot")  # type: ignore[method-assign]
    loop._report_observed_changes = lambda _s: None  # type: ignore[method-assign]
    loop._bus_delays_enabled = lambda: False  # type: ignore[method-assign]

    ok = await loop._verify_zero_meter_before_auth(session)
    assert ok is True
    assert session.state.filled_volume_raw == 0
    assert session.state.filled_amount_raw == 0


@pytest.mark.asyncio
async def test_verify_zero_blocks_on_fresh_nonzero_dc2() -> None:
    loop = await _loop()
    session = loop.sessions[1]
    loop._last_dc2.pop(1, None)

    async def _inject_nonzero(_s, **_kw):
        loop._last_dc2[1] = (47, 55225)
        return "eot"

    loop._write_frame = AsyncMock(return_value=(0.0, True))  # type: ignore[method-assign]
    loop._read_poll_session = AsyncMock(side_effect=_inject_nonzero)  # type: ignore[method-assign]
    loop._report_observed_changes = lambda _s: None  # type: ignore[method-assign]
    loop._bus_delays_enabled = lambda: False  # type: ignore[method-assign]

    ok = await loop._verify_zero_meter_before_auth(session)
    assert ok is False


@pytest.mark.asyncio
async def test_verify_zero_accepts_fresh_zero_dc2() -> None:
    loop = await _loop()
    session = loop.sessions[1]
    loop._last_dc2.pop(1, None)

    async def _inject_zero(_s, **_kw):
        loop._last_dc2[1] = (0, 0)
        return "eot"

    loop._write_frame = AsyncMock(return_value=(0.0, True))  # type: ignore[method-assign]
    loop._read_poll_session = AsyncMock(side_effect=_inject_zero)  # type: ignore[method-assign]
    loop._report_observed_changes = lambda _s: None  # type: ignore[method-assign]
    loop._bus_delays_enabled = lambda: False  # type: ignore[method-assign]

    ok = await loop._verify_zero_meter_before_auth(session)
    assert ok is True
