"""Cloud SET_PRICE applies per dart address without blocking on siblings."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from intelipump_fdc.cloud.set_price_request import (
    SetPriceRequest,
    read_set_price_request,
    write_set_price_request,
)
from intelipump_fdc.controller.controller_loop import ControllerLoop, ControllerRuntime
from intelipump_fdc.controller.exchange_result import ExchangeResultStatus
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.controller.session_models import ObservedStatus
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


def _dual_addr_loop() -> ControllerLoop:
    ctrl, _pump = create_memory_transport_pair()
    return ControllerLoop(
        ControllerRuntime(
            transport=ctrl,
            safety=_lab_safety(),
            config=PollSchedulerConfig(addresses=(1, 2)),
            logical_nozzle_count=1,
        )
    )


def test_set_price_defer_reason_holds_completed_not_reset() -> None:
    loop = _dual_addr_loop()
    s1 = loop.sessions[1]
    s1.state.observed_status = ObservedStatus.FILLING_COMPLETED
    assert loop._set_price_defer_reason(1, s1) == "display_hold"

    s2 = loop.sessions[2]
    s2.state.observed_status = ObservedStatus.RESET
    assert loop._set_price_defer_reason(2, s2) is None

    s2.state.observed_status = ObservedStatus.AUTHORIZED
    assert loop._set_price_defer_reason(2, s2) == "busy"


@pytest.mark.asyncio
async def test_set_price_applies_to_idle_sibling_while_other_holds_sale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: addr=1 FILLING_COMPLETED must not block CD5 on addr=2."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    loop.sessions[1].state.observed_status = ObservedStatus.FILLING_COMPLETED
    loop.sessions[2].state.observed_status = ObservedStatus.RESET
    loop._sale_display_held.add(1)

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-partial",
            command_id="cmd-partial",
            unit_price_raw=1375,
            prices_raw=(1375,),
            requested_by="admin@example.com",
        )
    )

    ok = AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.APPLICATION_CONFIRMED},
        )()
    )
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._apply_pending_cloud_set_price()

    assert ok.await_count == 1
    assert ok.await_args.args[0] is loop.sessions[2]
    assert 2 in loop._cloud_set_price_applied["corr-partial"]
    assert 1 not in loop._cloud_set_price_applied["corr-partial"]
    # Request stays until every dart address on this Pi is updated.
    assert read_set_price_request() is not None
    assert read_set_price_request().unit_price_raw == 1375

    # After the held face clears (pre-auth RESET path), finish the request.
    loop.sessions[1].state.observed_status = ObservedStatus.RESET
    loop._sale_display_held.discard(1)
    await loop._apply_pending_cloud_set_price()

    assert ok.await_count == 2
    assert read_set_price_request() is None
    assert "corr-partial" not in loop._cloud_set_price_applied
