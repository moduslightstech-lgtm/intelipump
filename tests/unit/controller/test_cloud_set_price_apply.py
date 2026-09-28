"""Cloud SET_PRICE applies per dart address without blocking on siblings."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from intelipump_fdc.cloud.set_price_request import (
    SetPriceRequest,
    consume_set_price_outcome,
    read_set_price_request,
    write_set_price_request,
)
from intelipump_fdc.controller.controller_loop import ControllerLoop, ControllerRuntime
from intelipump_fdc.controller.exchange_result import ExchangeResultStatus
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.controller.session_models import NozzlePosition, ObservedStatus
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


def _confirmed() -> AsyncMock:
    return AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.APPLICATION_CONFIRMED},
        )()
    )


def test_set_price_defer_reason_busy_and_hangup() -> None:
    loop = _dual_addr_loop()
    s1 = loop.sessions[1]
    s1.state.observed_status = ObservedStatus.FILLING_COMPLETED
    s1.state.nozzle_position = NozzlePosition.OUT
    assert loop._set_price_defer_reason(1, s1) == "await_hangup"

    s1.state.nozzle_position = NozzlePosition.IN
    assert loop._set_price_defer_reason(1, s1) is None

    s2 = loop.sessions[2]
    s2.state.observed_status = ObservedStatus.RESET
    assert loop._set_price_defer_reason(2, s2) is None

    s2.state.observed_status = ObservedStatus.AUTHORIZED
    assert loop._set_price_defer_reason(2, s2) == "busy"


@pytest.mark.asyncio
async def test_set_price_applies_to_idle_sibling_while_other_awaits_hangup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """addr=1 FILLING_COMPLETED nozzle OUT must not block CD5 on addr=2."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    loop.sessions[1].state.observed_status = ObservedStatus.FILLING_COMPLETED
    loop.sessions[1].state.nozzle_position = NozzlePosition.OUT
    loop.sessions[2].state.observed_status = ObservedStatus.RESET
    loop._sale_display_held.add(1)

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-partial",
            command_id="cmd-partial",
            unit_price_raw=1375,
            prices_raw=(1375,),
            requested_by="admin@example.com",
            pump_id="pump-1",
        )
    )

    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._apply_pending_cloud_set_price()

    assert ok.await_count == 1
    assert ok.await_args.args[0] is loop.sessions[2]
    assert 2 in loop._cloud_set_price_applied["corr-partial"]
    assert 1 not in loop._cloud_set_price_applied["corr-partial"]
    assert read_set_price_request() is not None
    assert consume_set_price_outcome() is None

    # Hang-up makes addr=1 eligible: RESET retained face, then CD5.
    loop.sessions[1].state.nozzle_position = NozzlePosition.IN
    await loop._apply_pending_cloud_set_price()

    assert ok.await_count == 3  # RESET + CD5 on addr=1
    assert read_set_price_request() is None
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.execution_status == "PRICE_CONFIRMED"
    assert outcome.pump_id == "pump-1"
    assert outcome.accepted is True
    assert set(outcome.applied_addresses) == {1, 2}


@pytest.mark.asyncio
async def test_set_price_retry_after_filling_completed_becomes_eligible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    for addr in (1, 2):
        loop.sessions[addr].state.observed_status = ObservedStatus.FILLING_COMPLETED
        loop.sessions[addr].state.nozzle_position = NozzlePosition.OUT
        loop._sale_display_held.add(addr)

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-retry",
            command_id="cmd-retry",
            unit_price_raw=1400,
            prices_raw=(1400,),
            pump_id="pump-4",
        )
    )
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._apply_pending_cloud_set_price()
    assert ok.await_count == 0
    assert read_set_price_request() is not None

    for addr in (1, 2):
        loop.sessions[addr].state.nozzle_position = NozzlePosition.IN
    await loop._apply_pending_cloud_set_price()

    # Each address: RESET then CD5
    assert ok.await_count == 4
    assert read_set_price_request() is None
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.execution_status == "PRICE_CONFIRMED"
    assert outcome.pump_id == "pump-4"


@pytest.mark.asyncio
async def test_link_ack_alone_is_not_applied_price(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    for addr in (1, 2):
        loop.sessions[addr].state.observed_status = ObservedStatus.RESET
        loop.sessions[addr].state.unit_price_raw = 1000  # not yet new price

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-link",
            command_id="cmd-link",
            unit_price_raw=1500,
            prices_raw=(1500,),
            pump_id="pump-2",
        )
    )
    link = AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.LINK_ACKNOWLEDGED},
        )()
    )
    loop._run_owned_command = link  # type: ignore[method-assign]

    await loop._apply_pending_cloud_set_price()
    assert read_set_price_request() is not None
    assert "corr-link" in loop._cloud_set_price_awaiting_dc3
    assert not loop._cloud_set_price_applied.get("corr-link")
    assert consume_set_price_outcome() is None

    # DC3 face matches → confirm without re-sending CD5 as applied.
    for addr in (1, 2):
        loop.sessions[addr].state.unit_price_raw = 1500
    await loop._apply_pending_cloud_set_price()
    assert read_set_price_request() is None
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.execution_status == "PRICE_CONFIRMED"
    assert set(outcome.applied_addresses) == {1, 2}


@pytest.mark.asyncio
async def test_cd5_timeout_is_not_applied_price(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    loop.sessions[1].state.observed_status = ObservedStatus.RESET
    loop.sessions[2].state.observed_status = ObservedStatus.RESET

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-timeout",
            command_id="cmd-timeout",
            unit_price_raw=1875,
            prices_raw=(1875,),
            pump_id="pump-8",
        )
    )

    timed_out = AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.TIMED_OUT},
        )()
    )
    loop._run_owned_command = timed_out  # type: ignore[method-assign]

    await loop._apply_pending_cloud_set_price()
    first_calls = timed_out.await_count
    assert first_calls >= 1
    await loop._apply_pending_cloud_set_price()
    assert timed_out.await_count == first_calls
    assert read_set_price_request() is not None
    assert not loop._cloud_set_price_applied.get("corr-timeout")
    assert consume_set_price_outcome() is None
