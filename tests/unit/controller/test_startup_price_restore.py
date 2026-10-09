"""Startup CD5 restore after controller restart — NOT_PROGRAMMED vs cached face."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from intelipump_fdc.cloud.set_price_request import (
    SetPriceRequest,
    write_persisted_unit_price,
    write_set_price_request,
)
from intelipump_fdc.controller.controller_loop import ControllerLoop, ControllerRuntime
from intelipump_fdc.controller.exchange_result import ExchangeResultStatus
from intelipump_fdc.controller.feature_flags import WayneFeatureFlags
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


def _loop() -> ControllerLoop:
    ctrl, _pump = create_memory_transport_pair()
    loop = ControllerLoop(
        ControllerRuntime(
            transport=ctrl,
            safety=_lab_safety(),
            config=PollSchedulerConfig(addresses=(1, 2)),
            logical_nozzle_count=1,
            startup_unit_price=1355,
        )
    )
    loop.runtime.feature_flags = WayneFeatureFlags(
        poll_and_observe=False,
        automatic_startup_price_programming=True,
        automatic_reset=False,
        automatic_authorization=False,
    )
    return loop


def _fresh_in(session, *, status: ObservedStatus) -> None:
    session.state.observed_status = status
    session.state.nozzle_position = NozzlePosition.IN
    now = time.monotonic()
    session.state.last_nozio_time = now
    session.state.last_status_time = now


def _confirmed() -> AsyncMock:
    return AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.APPLICATION_CONFIRMED},
        )()
    )


def _timed_out() -> AsyncMock:
    return AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.TIMED_OUT},
        )()
    )


def _link_ack() -> AsyncMock:
    return AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.LINK_ACKNOWLEDGED},
        )()
    )


@pytest.mark.asyncio
async def test_restart_restores_saved_price_when_face_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    loop = _loop()
    loop.runtime.startup_unit_price = 1300
    _fresh_in(loop.sessions[1], status=ObservedStatus.RESET)
    loop.sessions[1].state.unit_price_raw = 1000
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._owned_lab_tick(loop.sessions[1])

    assert loop.runtime.startup_unit_price == 1355
    assert ok.await_count == 1
    assert 1 in loop._price_programmed
    assert loop._startup_price_state[1] == "verified"


@pytest.mark.asyncio
async def test_not_programmed_with_matching_cached_face_still_sends_cd5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    loop = _loop()
    _fresh_in(loop.sessions[1], status=ObservedStatus.NOT_PROGRAMMED)
    loop.sessions[1].state.unit_price_raw = 1355  # retained face — not proof
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._owned_lab_tick(loop.sessions[1])

    assert ok.await_count == 1
    assert 1 in loop._price_programmed
    assert loop._startup_price_state[1] == "verified"


@pytest.mark.asyncio
async def test_fresh_reset_face_match_skips_cd5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    loop = _loop()
    _fresh_in(loop.sessions[1], status=ObservedStatus.RESET)
    loop.sessions[1].state.unit_price_raw = 1355
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._owned_lab_tick(loop.sessions[1])

    assert ok.await_count == 0
    assert 1 in loop._price_programmed
    assert loop._startup_price_state[1] == "verified"


@pytest.mark.asyncio
async def test_failed_first_attempt_retries_after_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    loop = _loop()
    _fresh_in(loop.sessions[1], status=ObservedStatus.NOT_PROGRAMMED)
    loop.sessions[1].state.unit_price_raw = 1355
    fail = _timed_out()
    loop._run_owned_command = fail  # type: ignore[method-assign]

    await loop._owned_lab_tick(loop.sessions[1])
    assert fail.await_count == 1
    assert 1 not in loop._price_programmed
    assert loop._startup_price_state[1] == "failed"
    assert loop._startup_price_fail_count[1] == 1

    # Immediate second tick — backoff defer, no CD5
    await loop._owned_lab_tick(loop.sessions[1])
    assert fail.await_count == 1
    assert loop._startup_price_state[1] == "deferred"

    # Expire backoff and succeed
    loop._startup_price_next_try[1] = time.monotonic() - 1
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._owned_lab_tick(loop.sessions[1])
    assert ok.await_count == 1
    assert 1 in loop._price_programmed


@pytest.mark.asyncio
async def test_nozzle_out_defers_without_marking_done(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    loop = _loop()
    session = loop.sessions[1]
    session.state.observed_status = ObservedStatus.NOT_PROGRAMMED
    session.state.nozzle_position = NozzlePosition.OUT
    now = time.monotonic()
    session.state.last_nozio_time = now
    session.state.last_status_time = now
    session.state.unit_price_raw = 1355
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._owned_lab_tick(session)

    assert ok.await_count == 0
    assert 1 not in loop._price_programmed
    assert loop._startup_price_state[1] == "deferred"

    # Hang nozzle — should attempt
    _fresh_in(session, status=ObservedStatus.NOT_PROGRAMMED)
    session.state.unit_price_raw = 1355
    await loop._owned_lab_tick(session)
    assert ok.await_count == 1
    assert 1 in loop._price_programmed


@pytest.mark.asyncio
async def test_pending_cloud_set_price_blocks_startup_cd5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-cloud",
            command_id="cmd-cloud",
            unit_price_raw=1400,
            prices_raw=(1400,),
            requested_by="admin@example.com",
            pump_id="pump-6",
        )
    )
    loop = _loop()
    _fresh_in(loop.sessions[1], status=ObservedStatus.NOT_PROGRAMMED)
    loop.sessions[1].state.unit_price_raw = 1000
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._owned_lab_tick(loop.sessions[1])

    assert ok.await_count == 0
    assert 1 not in loop._price_programmed
    assert loop._startup_price_state[1] == "deferred"


@pytest.mark.asyncio
async def test_both_addresses_restore_independently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    loop = _loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.NOT_PROGRAMMED)
        loop.sessions[addr].state.unit_price_raw = 1355
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._owned_lab_tick(loop.sessions[1])
    await loop._owned_lab_tick(loop.sessions[2])

    assert ok.await_count == 2
    assert loop._price_programmed == {1, 2}
    assert loop._startup_price_state[1] == "verified"
    assert loop._startup_price_state[2] == "verified"


@pytest.mark.asyncio
async def test_link_ack_awaits_verify_not_sticky_programmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    loop = _loop()
    _fresh_in(loop.sessions[1], status=ObservedStatus.NOT_PROGRAMMED)
    loop.sessions[1].state.unit_price_raw = 1355
    loop.sessions[1].state.unit_price_obs_gen = 3
    loop._run_owned_command = _link_ack()  # type: ignore[method-assign]

    await loop._owned_lab_tick(loop.sessions[1])

    assert 1 not in loop._price_programmed
    assert loop._startup_price_state[1] == "awaiting_verify"

    # Still NOT_PROGRAMMED — keep awaiting
    await loop._owned_lab_tick(loop.sessions[1])
    assert 1 not in loop._price_programmed
    assert loop._startup_price_state[1] == "awaiting_verify"

    # Late evidence: leave NOT_PROGRAMMED with matching face + advanced gen + fresh ages
    _fresh_in(loop.sessions[1], status=ObservedStatus.RESET)
    loop.sessions[1].state.unit_price_raw = 1355
    loop.sessions[1].state.unit_price_obs_gen = 4
    await loop._owned_lab_tick(loop.sessions[1])
    assert 1 in loop._price_programmed
    assert loop._startup_price_state[1] == "verified"


@pytest.mark.asyncio
async def test_unpersisted_completed_sale_defers_without_cd5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.controller.sale_lifecycle import SaleLifecycle

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    loop = _loop()
    session = loop.sessions[1]
    _fresh_in(session, status=ObservedStatus.FILLING_COMPLETED)
    session.state.unit_price_raw = 1000  # needs restore but sale face held
    session.state.sale_lifecycle = SaleLifecycle.FILLING_COMPLETED
    session.state.sale_evidence.filling_observed = True
    session.state.sale_evidence.filling_completed_observed = True
    session.state.sale_evidence.peak_volume_raw = 2500
    session.state.sale_evidence.peak_amount_raw = 3387500
    session.state.filled_volume_raw = 2500
    session.state.filled_amount_raw = 3387500
    session.state.sale_evidence.sale_published = False
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    # Direct restore path (owned_lab_tick may return early on LCD hold).
    await loop._maybe_startup_price_restore(session)

    assert ok.await_count == 0
    assert 1 not in loop._price_programmed
    assert loop._startup_price_state.get(1) == "deferred"


@pytest.mark.asyncio
async def test_cached_programmed_flag_reopened_on_not_programmed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1355, (1355,), source="cloud")
    loop = _loop()
    loop._price_programmed.add(1)
    _fresh_in(loop.sessions[1], status=ObservedStatus.NOT_PROGRAMMED)
    loop.sessions[1].state.unit_price_raw = 1355
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._owned_lab_tick(loop.sessions[1])

    assert ok.await_count == 1
    assert 1 in loop._price_programmed
    assert loop._startup_price_state[1] == "verified"
