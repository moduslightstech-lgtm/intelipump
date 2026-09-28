"""Cloud SET_PRICE applies per dart address without blocking on siblings."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from intelipump_fdc.cloud.set_price_request import (
    SetPriceRequest,
    consume_set_price_outcome,
    list_set_price_outcomes,
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
    assert list_set_price_outcomes() == []

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
async def test_reset_failure_does_not_clear_retained_sale_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    # Single-address focus: make addr=2 already applied via empty sessions? Use both idle
    # except addr=1 held — RESET times out; sale bookkeeping must stay.
    loop.sessions[1].state.observed_status = ObservedStatus.FILLING_COMPLETED
    loop.sessions[1].state.nozzle_position = NozzlePosition.IN
    loop.sessions[1].state.filled_volume_raw = 1234
    loop.sessions[1].state.filled_amount_raw = 5678
    loop._sale_display_held.add(1)
    loop._sale_display_hold_since[1] = 1.0
    loop._last_dc2[1] = (1234, 5678)
    loop.sessions[2].state.observed_status = ObservedStatus.RESET

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-reset-fail",
            command_id="cmd-reset-fail",
            unit_price_raw=1410,
            prices_raw=(1410,),
            pump_id="pump-1",
        )
    )

    async def _run(session, *args, **kwargs):
        label = kwargs.get("command_label") or ""
        if "RESET" in label:
            return type("R", (), {"status": ExchangeResultStatus.TIMED_OUT})()
        return type("R", (), {"status": ExchangeResultStatus.APPLICATION_CONFIRMED})()

    loop._run_owned_command = AsyncMock(side_effect=_run)  # type: ignore[method-assign]

    await loop._apply_pending_cloud_set_price()

    assert 1 in loop._sale_display_held
    assert loop._sale_display_hold_since.get(1) == 1.0
    assert loop._last_dc2.get(1) == (1234, 5678)
    assert loop.sessions[1].state.filled_volume_raw == 1234
    assert loop.sessions[1].state.filled_amount_raw == 5678
    # addr=2 still got CD5; request retained for addr=1
    assert 2 in loop._cloud_set_price_applied["corr-reset-fail"]
    assert 1 not in loop._cloud_set_price_applied["corr-reset-fail"]
    assert read_set_price_request() is not None


@pytest.mark.asyncio
async def test_link_ack_stale_price_does_not_confirm_without_fresh_dc3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    for addr in (1, 2):
        loop.sessions[addr].state.observed_status = ObservedStatus.RESET
        # Stale face already shows commanded price — must NOT confirm on LINK_ACK.
        loop.sessions[addr].state.unit_price_raw = 1500
        loop.sessions[addr].state.unit_price_obs_gen = 3

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
    assert list_set_price_outcomes() == []

    # Same gen + same price still not confirmed.
    await loop._apply_pending_cloud_set_price()
    assert not loop._cloud_set_price_applied.get("corr-link")
    assert read_set_price_request() is not None

    # Fresh DC3 observation (gen bump) with matching price → confirm.
    for addr in (1, 2):
        loop.sessions[addr].state.unit_price_obs_gen = 4
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
    assert list_set_price_outcomes() == []


@pytest.mark.asyncio
async def test_outcome_write_failure_keeps_pending_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Crash/write failure at finalization must not drop the request."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    for addr in (1, 2):
        loop.sessions[addr].state.observed_status = ObservedStatus.RESET

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-write-fail",
            command_id="cmd-write-fail",
            unit_price_raw=1420,
            prices_raw=(1420,),
            pump_id="pump-1",
        )
    )
    loop._run_owned_command = _confirmed()  # type: ignore[method-assign]

    def _boom(*_a, **_k):
        raise OSError("disk full")

    monkeypatch.setattr(
        "intelipump_fdc.cloud.set_price_request.write_set_price_outcome",
        _boom,
    )

    await loop._apply_pending_cloud_set_price()
    assert read_set_price_request() is not None
    assert read_set_price_request().correlation_id == "corr-write-fail"
    assert list_set_price_outcomes() == []
    # In-memory tracking retained so settle can retry.
    assert "corr-write-fail" in loop._cloud_set_price_applied


@pytest.mark.asyncio
async def test_crash_after_outcome_write_clears_leftover_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If outcome is durable but request remains, next tick clears the request."""
    from intelipump_fdc.cloud.set_price_request import (
        SetPriceOutcome,
        write_set_price_outcome,
    )

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-crash",
            command_id="cmd-crash",
            unit_price_raw=1430,
            prices_raw=(1430,),
            pump_id="pump-2",
        )
    )
    write_set_price_outcome(
        SetPriceOutcome(
            correlation_id="corr-crash",
            command_id="cmd-crash",
            station_id=None,
            pump_id="pump-2",
            unit_price_raw=1430,
            execution_status="PRICE_CONFIRMED",
            accepted=True,
            applied_addresses=(1, 2),
            gave_up_addresses=(),
            deferred_addresses=(),
            detail="cd5_application_or_dc3_confirmed",
        )
    )
    loop._cloud_set_price_applied["corr-crash"] = {1, 2}

    await loop._apply_pending_cloud_set_price()

    assert read_set_price_request() is None
    outcomes = list_set_price_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0].correlation_id == "corr-crash"
    assert "corr-crash" not in loop._cloud_set_price_applied


@pytest.mark.asyncio
async def test_dc3_timeout_uses_backoff_then_eventual_failed_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    loop._cloud_set_price_dc3_timeout_s = 0.01
    for addr in (1, 2):
        loop.sessions[addr].state.observed_status = ObservedStatus.RESET
        loop.sessions[addr].state.unit_price_raw = 1000
        loop.sessions[addr].state.unit_price_obs_gen = 1

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-dc3-timeout",
            command_id="cmd-dc3-timeout",
            unit_price_raw=1600,
            prices_raw=(1600,),
            pump_id="pump-3",
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
    assert loop._cloud_set_price_awaiting_dc3.get("corr-dc3-timeout")
    assert read_set_price_request() is not None

    # Expire DC3 wait — no fresh observation.
    import time as time_mod

    past = time_mod.monotonic() - 1.0
    for addr in (1, 2):
        loop._cloud_set_price_dc3_deadline[f"corr-dc3-timeout:{addr}"] = past
        # Clear next_try so CD5 can retry after miss... but miss sets next_try.
    await loop._apply_pending_cloud_set_price()
    assert not loop._cloud_set_price_awaiting_dc3.get("corr-dc3-timeout")
    assert loop._cloud_set_price_fail_count["corr-dc3-timeout:1"] == 1
    assert loop._cloud_set_price_fail_count["corr-dc3-timeout:2"] == 1
    assert read_set_price_request() is not None
    assert list_set_price_outcomes() == []

    # Force give-up via fail counts without waiting for backoff clocks.
    for addr in (1, 2):
        key = f"corr-dc3-timeout:{addr}"
        loop._cloud_set_price_fail_count[key] = 4
        loop._cloud_set_price_next_try.pop(key, None)
    # Re-enter LINK_ACK path then timeout again to hit fail>=5, or directly miss.
    await loop._apply_pending_cloud_set_price()
    # After CD5 LINK_ACK again, expire deadlines
    for addr in (1, 2):
        loop._cloud_set_price_dc3_deadline[f"corr-dc3-timeout:{addr}"] = past
    await loop._apply_pending_cloud_set_price()

    assert read_set_price_request() is None
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.execution_status == "PRICE_FAILED"
    assert outcome.accepted is False
    assert set(outcome.gave_up_addresses) == {1, 2}


@pytest.mark.asyncio
async def test_nonmatching_fresh_dc3_uses_backoff_not_confirm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    loop = _dual_addr_loop()
    for addr in (1, 2):
        loop.sessions[addr].state.observed_status = ObservedStatus.RESET
        loop.sessions[addr].state.unit_price_raw = 1000
        loop.sessions[addr].state.unit_price_obs_gen = 2

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-mismatch",
            command_id="cmd-mismatch",
            unit_price_raw=1700,
            prices_raw=(1700,),
            pump_id="pump-5",
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
    assert loop._cloud_set_price_awaiting_dc3.get("corr-mismatch")

    # Fresh DC3 with wrong price.
    for addr in (1, 2):
        loop.sessions[addr].state.unit_price_obs_gen = 3
        loop.sessions[addr].state.unit_price_raw = 1699
    await loop._apply_pending_cloud_set_price()

    assert not loop._cloud_set_price_awaiting_dc3.get("corr-mismatch")
    assert not loop._cloud_set_price_applied.get("corr-mismatch")
    assert loop._cloud_set_price_fail_count["corr-mismatch:1"] == 1
    assert read_set_price_request() is not None
    assert list_set_price_outcomes() == []
