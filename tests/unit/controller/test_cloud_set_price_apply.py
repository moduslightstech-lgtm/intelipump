"""Cloud SET_PRICE applies per dart address without blocking on siblings."""

from __future__ import annotations

import time
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
from intelipump_fdc.controller.sale_lifecycle import SaleLifecycle
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


def _fresh_in(session, *, status: ObservedStatus = ObservedStatus.RESET) -> None:
    session.state.observed_status = status
    session.state.nozzle_position = NozzlePosition.IN
    now = time.monotonic()
    session.state.last_nozio_time = now
    session.state.last_status_time = now


def _completed_sale(
    session,
    *,
    nozzle: NozzlePosition = NozzlePosition.IN,
    volume: int = 2500,
    amount: int = 33875,
    unit_price: int = 1355,
    persisted: bool = True,
) -> None:
    session.state.observed_status = ObservedStatus.FILLING_COMPLETED
    session.state.nozzle_position = nozzle
    session.state.sale_lifecycle = SaleLifecycle.FILLING_COMPLETED
    session.state.sale_evidence.filling_observed = True
    session.state.sale_evidence.filling_completed_observed = True
    session.state.sale_evidence.peak_volume_raw = volume
    session.state.sale_evidence.peak_amount_raw = amount
    session.state.filled_volume_raw = volume
    session.state.filled_amount_raw = amount
    session.state.unit_price_raw = unit_price
    now = time.monotonic()
    session.state.last_nozio_time = now
    session.state.last_status_time = now
    if persisted:
        session.state.sale_evidence.sale_published = True


def _command_labels(mock: AsyncMock) -> list[str]:
    return [str(c.kwargs.get("command_label") or "") for c in mock.await_args_list]


def _write_price(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corr: str,
    price: int,
    pump: str = "pump-1",
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_request(
        SetPriceRequest(
            correlation_id=corr,
            command_id=f"cmd-{corr}",
            unit_price_raw=price,
            prices_raw=(price,),
            requested_by="admin@example.com",
            pump_id=pump,
        )
    )


def test_set_price_defer_reason_busy_and_hangup() -> None:
    loop = _dual_addr_loop()
    s1 = loop.sessions[1]
    s1.state.observed_status = ObservedStatus.FILLING_COMPLETED
    s1.state.nozzle_position = NozzlePosition.OUT
    assert loop._set_price_defer_reason(1, s1) == "nozzle_out"

    s1.state.nozzle_position = NozzlePosition.UNKNOWN
    assert loop._set_price_defer_reason(1, s1) == "nozzle_unknown"

    s1.state.nozzle_position = NozzlePosition.IN
    s1.state.last_nozio_time = time.monotonic() - 60.0
    assert loop._set_price_defer_reason(1, s1) == "nozzle_stale"

    s1.state.last_nozio_time = time.monotonic()
    assert loop._set_price_defer_reason(1, s1) is None

    s2 = loop.sessions[2]
    _fresh_in(s2, status=ObservedStatus.RESET)
    assert loop._set_price_defer_reason(2, s2) is None

    s2.state.observed_status = ObservedStatus.AUTHORIZED
    assert loop._set_price_defer_reason(2, s2) == "busy"

    s2.state.observed_status = ObservedStatus.FILLING
    s2.state.nozzle_position = NozzlePosition.OUT
    assert loop._set_price_defer_reason(2, s2) == "busy"


@pytest.mark.asyncio
async def test_set_price_applies_to_idle_sibling_while_other_awaits_hangup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """addr=1 FILLING_COMPLETED nozzle OUT must not block CD5 on addr=2."""
    _write_price(tmp_path, monkeypatch, "corr-partial", 1375)
    loop = _dual_addr_loop()
    _completed_sale(loop.sessions[1], nozzle=NozzlePosition.OUT)
    loop._sale_display_held.add(1)
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)

    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._apply_pending_cloud_set_price()

    assert ok.await_count == 1
    assert ok.await_args.args[0] is loop.sessions[2]
    assert "CD5" in (ok.await_args.kwargs.get("command_label") or "")
    assert 2 in loop._cloud_set_price_applied["corr-partial"]
    assert 1 not in loop._cloud_set_price_applied["corr-partial"]
    assert read_set_price_request() is not None
    assert list_set_price_outcomes() == []

    loop.sessions[1].state.nozzle_position = NozzlePosition.IN
    loop.sessions[1].state.last_nozio_time = time.monotonic()
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
    _write_price(tmp_path, monkeypatch, "corr-retry", 1400, pump="pump-4")
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _completed_sale(loop.sessions[addr], nozzle=NozzlePosition.OUT)
        loop._sale_display_held.add(addr)

    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    await loop._apply_pending_cloud_set_price()
    assert ok.await_count == 0
    assert read_set_price_request() is not None

    for addr in (1, 2):
        loop.sessions[addr].state.nozzle_position = NozzlePosition.IN
        loop.sessions[addr].state.last_nozio_time = time.monotonic()
    await loop._apply_pending_cloud_set_price()

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
    _write_price(tmp_path, monkeypatch, "corr-reset-fail", 1410)
    loop = _dual_addr_loop()
    _completed_sale(loop.sessions[1], volume=1234, amount=5678, unit_price=1355)
    loop._sale_display_held.add(1)
    loop._sale_display_hold_since[1] = 1.0
    loop._last_dc2[1] = (1234, 5678)
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)

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
    assert loop._last_completed_sale[1]["volume_raw"] == 1234
    assert loop._last_completed_sale[1]["amount_raw"] == 5678
    assert loop._last_completed_sale[1]["unit_price_raw"] == 1355
    assert 2 in loop._cloud_set_price_applied["corr-reset-fail"]
    assert 1 not in loop._cloud_set_price_applied["corr-reset-fail"]
    assert read_set_price_request() is not None
    assert not any(
        "CD5" in (c.kwargs.get("command_label") or "") and c.args[0] is loop.sessions[1]
        for c in loop._run_owned_command.await_args_list
    )


@pytest.mark.asyncio
async def test_link_ack_stale_price_does_not_confirm_without_fresh_dc3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-link", 1500, pump="pump-2")
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 1500
        loop.sessions[addr].state.unit_price_obs_gen = 3

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

    await loop._apply_pending_cloud_set_price()
    assert not loop._cloud_set_price_applied.get("corr-link")
    assert read_set_price_request() is not None

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
    _write_price(tmp_path, monkeypatch, "corr-timeout", 1875, pump="pump-8")
    loop = _dual_addr_loop()
    _fresh_in(loop.sessions[1], status=ObservedStatus.RESET)
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)

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
    _write_price(tmp_path, monkeypatch, "corr-write-fail", 1420)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)

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
    _write_price(tmp_path, monkeypatch, "corr-dc3-timeout", 1600, pump="pump-3")
    loop = _dual_addr_loop()
    loop._cloud_set_price_dc3_timeout_s = 0.01
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 1000
        loop.sessions[addr].state.unit_price_obs_gen = 1

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

    past = time.monotonic() - 1.0
    for addr in (1, 2):
        loop._cloud_set_price_dc3_deadline[f"corr-dc3-timeout:{addr}"] = past
    await loop._apply_pending_cloud_set_price()
    assert not loop._cloud_set_price_awaiting_dc3.get("corr-dc3-timeout")
    assert loop._cloud_set_price_fail_count["corr-dc3-timeout:1"] == 1
    assert loop._cloud_set_price_fail_count["corr-dc3-timeout:2"] == 1
    assert read_set_price_request() is not None
    assert list_set_price_outcomes() == []

    for addr in (1, 2):
        key = f"corr-dc3-timeout:{addr}"
        loop._cloud_set_price_fail_count[key] = 4
        loop._cloud_set_price_next_try.pop(key, None)
    await loop._apply_pending_cloud_set_price()
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
    _write_price(tmp_path, monkeypatch, "corr-mismatch", 1700, pump="pump-5")
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 1000
        loop.sessions[addr].state.unit_price_obs_gen = 2

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

    for addr in (1, 2):
        loop.sessions[addr].state.unit_price_obs_gen = 3
        loop.sessions[addr].state.unit_price_raw = 1699
    await loop._apply_pending_cloud_set_price()

    assert not loop._cloud_set_price_awaiting_dc3.get("corr-mismatch")
    assert not loop._cloud_set_price_applied.get("corr-mismatch")
    assert loop._cloud_set_price_fail_count["corr-mismatch:1"] == 1
    assert read_set_price_request() is not None
    assert list_set_price_outcomes() == []


@pytest.mark.asyncio
async def test_idle_filling_completed_fresh_in_resets_then_confirms_without_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Production deadlock: FILLING_COMPLETED + saleDisplayHeld=False + IN."""
    _write_price(
        tmp_path, monkeypatch, "e3b51024-7154-4e28-acbc-23b29bb7101b", 1355
    )
    loop = _dual_addr_loop()
    _completed_sale(loop.sessions[1])
    assert 1 not in loop._sale_display_held
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)
    loop._last_completed_sale[1] = {
        "volume_raw": 2500,
        "amount_raw": 33875,
        "unit_price_raw": 1355,
    }

    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()

    labels = _command_labels(ok)
    sessions = [c.args[0].address for c in ok.await_args_list]
    pairs = list(zip(labels, sessions))
    assert ("CD1_RESET_BEFORE_CLOUD_PRICE", 1) in pairs
    assert ("CD5_SET_PRICE_CLOUD", 1) in pairs
    assert ("CD5_SET_PRICE_CLOUD", 2) in pairs
    assert not any("AUTHORIZE" in lab for lab in labels)
    assert read_set_price_request() is None
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.execution_status == "PRICE_CONFIRMED"
    assert outcome.correlation_id == "e3b51024-7154-4e28-acbc-23b29bb7101b"
    assert loop._last_completed_sale[1]["volume_raw"] == 2500
    assert loop._last_completed_sale[1]["amount_raw"] == 33875
    assert loop._last_completed_sale[1]["unit_price_raw"] == 1355


@pytest.mark.asyncio
async def test_nozzle_out_unknown_stale_and_filling_block_reset_cd5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-gates", 1355)
    loop = _dual_addr_loop()
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    _completed_sale(loop.sessions[1], nozzle=NozzlePosition.OUT)
    await loop._apply_pending_cloud_set_price()
    assert not any(
        c.args[0].address == 1
        and (
            "RESET" in (c.kwargs.get("command_label") or "")
            or "CD5" in (c.kwargs.get("command_label") or "")
        )
        for c in ok.await_args_list
    )

    loop.sessions[1].state.nozzle_position = NozzlePosition.UNKNOWN
    ok.reset_mock()
    await loop._apply_pending_cloud_set_price()
    assert not any(
        "RESET" in (c.kwargs.get("command_label") or "")
        or "CD5" in (c.kwargs.get("command_label") or "")
        for c in ok.await_args_list
        if c.args[0].address == 1
    )

    _completed_sale(loop.sessions[1], nozzle=NozzlePosition.IN)
    loop.sessions[1].state.last_nozio_time = time.monotonic() - 60.0
    ok.reset_mock()
    await loop._apply_pending_cloud_set_price()
    assert not any(
        "RESET" in (c.kwargs.get("command_label") or "")
        or "CD5" in (c.kwargs.get("command_label") or "")
        for c in ok.await_args_list
        if c.args[0].address == 1
    )

    loop.sessions[1].state.observed_status = ObservedStatus.FILLING
    loop.sessions[1].state.nozzle_position = NozzlePosition.OUT
    loop.sessions[1].state.last_nozio_time = time.monotonic()
    ok.reset_mock()
    await loop._apply_pending_cloud_set_price()
    assert not any(c.args[0].address == 1 for c in ok.await_args_list)
    assert read_set_price_request() is not None


@pytest.mark.asyncio
async def test_sale_persistence_failure_retains_sale_and_pending_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-persist", 1355)
    loop = _dual_addr_loop()
    _completed_sale(loop.sessions[1], persisted=False)
    loop.sessions[1].state.sale_evidence.sale_published = False
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)

    def _fail_capture(_session) -> bool:
        return False

    loop._capture_completed_sale_snapshot = _fail_capture  # type: ignore[method-assign]
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()

    assert not any(
        "RESET" in (c.kwargs.get("command_label") or "")
        or "CD5" in (c.kwargs.get("command_label") or "")
        for c in ok.await_args_list
        if c.args[0].address == 1
    )
    assert loop.sessions[1].state.filled_volume_raw == 2500
    assert loop.sessions[1].state.filled_amount_raw == 33875
    assert loop.sessions[1].state.unit_price_raw == 1355
    assert 1 not in loop._last_completed_sale
    assert read_set_price_request() is not None
    assert 2 in loop._cloud_set_price_applied["corr-persist"]


@pytest.mark.asyncio
async def test_new_nozzle_lift_during_reset_prevents_cd5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-lift", 1355)
    loop = _dual_addr_loop()
    _completed_sale(loop.sessions[1])
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)

    async def _run(session, *args, **kwargs):
        label = kwargs.get("command_label") or ""
        if "RESET" in label and session.address == 1:
            session.state.nozzle_position = NozzlePosition.OUT
            session.state.observed_status = ObservedStatus.AUTHORIZED
            return type("R", (), {"status": ExchangeResultStatus.APPLICATION_CONFIRMED})()
        return type("R", (), {"status": ExchangeResultStatus.APPLICATION_CONFIRMED})()

    loop._run_owned_command = AsyncMock(side_effect=_run)  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()

    labels_by_addr = [
        (c.args[0].address, c.kwargs.get("command_label"))
        for c in loop._run_owned_command.await_args_list
    ]
    assert (1, "CD1_RESET_BEFORE_CLOUD_PRICE") in labels_by_addr
    assert (1, "CD5_SET_PRICE_CLOUD") not in labels_by_addr
    assert 1 not in loop._cloud_set_price_applied.get("corr-lift", set())
    assert read_set_price_request() is not None


@pytest.mark.asyncio
async def test_reset_link_ack_alone_does_not_advance_to_cd5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-reset-link", 1355)
    loop = _dual_addr_loop()
    _completed_sale(loop.sessions[1])
    loop._sale_display_held.add(1)
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)

    async def _run(session, *args, **kwargs):
        label = kwargs.get("command_label") or ""
        if "RESET" in label:
            return type("R", (), {"status": ExchangeResultStatus.LINK_ACKNOWLEDGED})()
        return type("R", (), {"status": ExchangeResultStatus.APPLICATION_CONFIRMED})()

    loop._run_owned_command = AsyncMock(side_effect=_run)  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()

    labels_by_addr = [
        (c.args[0].address, c.kwargs.get("command_label"))
        for c in loop._run_owned_command.await_args_list
    ]
    assert (1, "CD1_RESET_BEFORE_CLOUD_PRICE") in labels_by_addr
    assert (1, "CD5_SET_PRICE_CLOUD") not in labels_by_addr
    assert loop.sessions[1].state.filled_volume_raw == 2500
    assert 1 in loop._sale_display_held
    assert read_set_price_request() is not None


@pytest.mark.asyncio
async def test_duplicate_retry_does_not_conflict_or_duplicate_sale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-dup", 1355)
    loop = _dual_addr_loop()
    _completed_sale(loop.sessions[1])
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    first_cd5 = sum(1 for lab in _command_labels(ok) if "CD5" in lab)
    assert first_cd5 == 2
    assert read_set_price_request() is None
    sale = dict(loop._last_completed_sale[1])

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-dup",
            command_id="cmd-corr-dup",
            unit_price_raw=1355,
            prices_raw=(1355,),
            pump_id="pump-1",
        )
    )
    ok.reset_mock()
    await loop._apply_pending_cloud_set_price()
    assert ok.await_count == 0
    assert loop._last_completed_sale[1] == sale
    outcomes = list_set_price_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0].correlation_id == "corr-dup"


@pytest.mark.asyncio
async def test_restart_reconnect_preserves_request_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-restart", 1355)
    loop = _dual_addr_loop()
    _completed_sale(loop.sessions[1], nozzle=NozzlePosition.OUT)
    _fresh_in(loop.sessions[2], status=ObservedStatus.FILLING)
    loop._run_owned_command = _confirmed()  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    pending = read_set_price_request()
    assert pending is not None
    assert pending.correlation_id == "corr-restart"

    loop2 = _dual_addr_loop()
    _completed_sale(loop2.sessions[1], nozzle=NozzlePosition.IN)
    _fresh_in(loop2.sessions[2], status=ObservedStatus.RESET)
    loop2._run_owned_command = _confirmed()  # type: ignore[method-assign]
    await loop2._apply_pending_cloud_set_price()
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.correlation_id == "corr-restart"
    assert outcome.execution_status == "PRICE_CONFIRMED"


@pytest.mark.asyncio
async def test_addr1_pending_does_not_command_addr2_while_filling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-iso", 1355)
    loop = _dual_addr_loop()
    _completed_sale(loop.sessions[1], nozzle=NozzlePosition.OUT)
    loop.sessions[2].state.observed_status = ObservedStatus.FILLING
    loop.sessions[2].state.nozzle_position = NozzlePosition.OUT
    loop.sessions[2].state.last_nozio_time = time.monotonic()
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    assert all(c.args[0].address != 2 for c in ok.await_args_list)
    assert not loop._cloud_set_price_applied.get("corr-iso")
    assert read_set_price_request() is not None


@pytest.mark.asyncio
async def test_indefinite_deferral_becomes_bounded_failed_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-ttl", 1355)
    loop = _dual_addr_loop()
    loop._set_price_defer_timeout_s = 0.0
    _completed_sale(loop.sessions[1], nozzle=NozzlePosition.UNKNOWN)
    loop.sessions[2].state.observed_status = ObservedStatus.FILLING
    loop.sessions[2].state.nozzle_position = NozzlePosition.OUT
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    assert not any(
        "CD5" in (c.kwargs.get("command_label") or "")
        or "RESET" in (c.kwargs.get("command_label") or "")
        for c in ok.await_args_list
    )
    assert read_set_price_request() is None
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.execution_status == "PRICE_FAILED"
    assert outcome.accepted is False
    assert outcome.detail == "set_price_deferred_timeout"
    assert set(outcome.gave_up_addresses) == {1, 2}


@pytest.mark.asyncio
async def test_expired_request_does_not_apply_after_later_hangup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-expired", 1355)
    loop = _dual_addr_loop()
    loop._set_price_defer_timeout_s = 0.0
    _completed_sale(loop.sessions[1], nozzle=NozzlePosition.OUT)
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)
    loop._run_owned_command = _confirmed()  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    outcomes = list_set_price_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0].detail == "set_price_deferred_timeout"
    assert read_set_price_request() is None

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-expired",
            command_id="cmd-corr-expired",
            unit_price_raw=1355,
            prices_raw=(1355,),
            pump_id="pump-1",
        )
    )
    loop2 = _dual_addr_loop()
    _completed_sale(loop2.sessions[1], nozzle=NozzlePosition.IN)
    _fresh_in(loop2.sessions[2], status=ObservedStatus.RESET)
    loop2._run_owned_command = _confirmed()  # type: ignore[method-assign]
    await loop2._apply_pending_cloud_set_price()
    assert read_set_price_request() is None
    assert loop2._run_owned_command.await_count == 0
    leftover = list_set_price_outcomes()
    assert len(leftover) == 1
    assert leftover[0].detail == "set_price_deferred_timeout"
