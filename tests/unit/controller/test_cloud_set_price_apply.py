"""Cloud SET_PRICE applies per dart address without blocking on siblings."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from intelipump_fdc.cloud.set_price_request import (
    SetPriceRequest,
    consume_set_price_outcome,
    has_set_price_outcome,
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
    amount: int = 3387500,  # 25.00 L × ₦1355 → 2-dp ledger
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
    s1.state.last_status_time = time.monotonic() - 60.0
    assert loop._set_price_defer_reason(1, s1) == "nozzle_stale"

    s1.state.last_nozio_time = time.monotonic()
    s1.state.last_status_time = time.monotonic()
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
async def test_sibling_filling_defers_cd5_on_idle_address(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not CD5 addr=2 while addr=1 is FILLING — pump often never ACKs."""
    _write_price(tmp_path, monkeypatch, "corr-sibling", 1355, pump="pump-3")
    loop = _dual_addr_loop()
    _fresh_in(loop.sessions[2], status=ObservedStatus.RESET)
    loop.sessions[1].state.observed_status = ObservedStatus.FILLING
    loop.sessions[1].state.nozzle_position = NozzlePosition.OUT
    now = time.monotonic()
    loop.sessions[1].state.last_nozio_time = now
    loop.sessions[1].state.last_status_time = now

    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()

    assert ok.await_count == 0
    assert read_set_price_request() is not None
    assert 2 not in loop._cloud_set_price_applied.get("corr-sibling", set())
    assert loop._set_price_defer_reason(2, loop.sessions[2]) == "sibling_busy"


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
async def test_dc3_timeout_after_link_ack_is_sent_unverified_no_cd5_resend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-dc3-timeout", 1600, pump="pump-3")
    loop = _dual_addr_loop()
    loop._cloud_set_price_dc3_timeout_s = 0.01
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 0
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
    first_cd5 = link.await_count
    assert first_cd5 == 2

    # Idle zero DC3 gen bump must not resend CD5.
    for addr in (1, 2):
        loop.sessions[addr].state.unit_price_obs_gen = 2
        loop.sessions[addr].state.unit_price_raw = 0
    await loop._apply_pending_cloud_set_price()
    assert link.await_count == first_cd5
    assert loop._cloud_set_price_awaiting_dc3.get("corr-dc3-timeout")

    past = time.monotonic() - 1.0
    for addr in (1, 2):
        loop._cloud_set_price_dc3_deadline[f"corr-dc3-timeout:{addr}"] = past
    await loop._apply_pending_cloud_set_price()

    assert read_set_price_request() is None
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.execution_status == "SENT_UNVERIFIED"
    assert outcome.accepted is True
    assert set(outcome.unverified_addresses) == {1, 2}
    assert link.await_count == first_cd5  # never resent CD5


@pytest.mark.asyncio
async def test_idle_zero_dc3_then_later_matching_readback_confirms(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-late-dc3", 1365)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 0
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
    assert loop._cloud_set_price_awaiting_dc3.get("corr-late-dc3")

    # Idle zeros — still awaiting, no CD5 resend.
    for addr in (1, 2):
        loop.sessions[addr].state.unit_price_obs_gen += 1
        loop.sessions[addr].state.unit_price_raw = 0
    await loop._apply_pending_cloud_set_price()
    assert link.await_count == 2
    assert read_set_price_request() is not None

    # Later matching observation confirms.
    for addr in (1, 2):
        loop.sessions[addr].state.unit_price_obs_gen += 1
        loop.sessions[addr].state.unit_price_raw = 1365
    await loop._apply_pending_cloud_set_price()
    assert read_set_price_request() is None
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.execution_status == "PRICE_CONFIRMED"
    assert set(outcome.applied_addresses) == {1, 2}
    assert link.await_count == 2


@pytest.mark.asyncio
async def test_sent_unverified_upgrades_on_late_dc3_without_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import (
        SetPriceOutcome,
        list_set_price_pending_verifies,
        read_persisted_unit_price,
        write_persisted_unit_price,
        write_set_price_outcome,
    )

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1365, (1365,), source="cloud")
    write_set_price_outcome(
        SetPriceOutcome(
            correlation_id="corr-upgrade",
            command_id="cmd-corr-upgrade",
            station_id=None,
            pump_id="pump-1",
            unit_price_raw=1365,
            execution_status="SENT_UNVERIFIED",
            accepted=True,
            applied_addresses=(),
            gave_up_addresses=(),
            deferred_addresses=(),
            unverified_addresses=(1, 2),
            detail="cd5_link_ack_dc3_idle_or_timeout",
        )
    )
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 1365
        loop.sessions[addr].state.unit_price_obs_gen = 5
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    assert ok.await_count == 0  # no CD5 replay
    outcomes = list_set_price_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0].execution_status == "PRICE_CONFIRMED"
    assert set(outcomes[0].applied_addresses) == {1, 2}
    assert list_set_price_pending_verifies() == []
    assert read_persisted_unit_price().unit_price_raw == 1365


@pytest.mark.asyncio
async def test_late_verify_one_target_then_both(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Partial late DC3 upgrades one address; second address confirms overall."""
    from intelipump_fdc.cloud.set_price_request import (
        SetPricePendingVerify,
        ack_set_price_outcome,
        list_set_price_pending_verifies,
        write_set_price_pending_verify,
    )

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_pending_verify(
        SetPricePendingVerify(
            correlation_id="corr-partial",
            command_id="cmd-corr-partial",
            station_id=None,
            pump_id="pump-1",
            unit_price_raw=1370,
            required_addresses=(1, 2),
            verified_addresses=(),
            emitted_verified_addresses=(),
        )
    )
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 0
        loop.sessions[addr].state.unit_price_obs_gen = 1
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    # Address 1 nozzle lift → matching DC3.
    loop.sessions[1].state.unit_price_raw = 1370
    loop.sessions[1].state.unit_price_obs_gen = 2
    await loop._apply_pending_cloud_set_price()
    assert ok.await_count == 0
    partial = list_set_price_outcomes()
    assert len(partial) == 1
    assert partial[0].execution_status == "PRICE_PARTIAL"
    assert set(partial[0].applied_addresses) == {1}
    assert set(partial[0].unverified_addresses) == {2}
    pending = list_set_price_pending_verifies()
    assert len(pending) == 1
    assert set(pending[0].verified_addresses) == {1}

    # Cloud ACK deletes outcome; pending-verify must still upgrade later.
    assert ack_set_price_outcome("corr-partial")
    assert list_set_price_outcomes() == []

    # Duplicate observation of addr 1 must not rewrite or bus-command.
    await loop._apply_pending_cloud_set_price()
    assert list_set_price_outcomes() == []
    assert ok.await_count == 0

    # Address 2 nozzle lift → overall confirm.
    loop.sessions[2].state.unit_price_raw = 1370
    loop.sessions[2].state.unit_price_obs_gen = 3
    await loop._apply_pending_cloud_set_price()
    assert ok.await_count == 0
    final = list_set_price_outcomes()
    assert len(final) == 1
    assert final[0].execution_status == "PRICE_CONFIRMED"
    assert set(final[0].applied_addresses) == {1, 2}
    assert final[0].unverified_addresses == ()
    assert list_set_price_pending_verifies() == []


@pytest.mark.asyncio
async def test_late_verify_survives_restart_without_bus_cmds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import (
        SetPricePendingVerify,
        list_set_price_pending_verifies,
        write_set_price_pending_verify,
    )

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_pending_verify(
        SetPricePendingVerify(
            correlation_id="corr-restart",
            command_id="cmd-corr-restart",
            station_id=None,
            pump_id="pump-1",
            unit_price_raw=1370,
            required_addresses=(1, 2),
            verified_addresses=(1,),
            emitted_verified_addresses=(1,),
            outcome_revision=1,
        )
    )
    # Fresh controller process after restart.
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 1370
        loop.sessions[addr].state.unit_price_obs_gen = 8
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    assert ok.await_count == 0
    assert _command_labels(ok) == []
    outcomes = list_set_price_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0].execution_status == "PRICE_CONFIRMED"
    assert set(outcomes[0].applied_addresses) == {1, 2}
    assert list_set_price_pending_verifies() == []


@pytest.mark.asyncio
async def test_late_verify_cloud_outcome_republish_uses_status_dedupe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SENT_UNVERIFIED then PRICE_CONFIRMED must both publish (distinct dedupe)."""
    from intelipump_fdc.cloud.command_intake import CloudCommandIntake
    from intelipump_fdc.cloud.mqtt.models import MqttPublishResult
    from intelipump_fdc.cloud.set_price_request import (
        SetPriceOutcome,
        write_set_price_outcome,
    )
    from intelipump_fdc.cloud.topics import TopicBuilder
    from unittest.mock import MagicMock

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    mqtt = MagicMock()
    mqtt.is_connected = True
    mqtt.publish = AsyncMock(
        return_value=MqttPublishResult(topic="t", acknowledged=True, mid=1)
    )
    intake = CloudCommandIntake(
        session_factory=MagicMock(),
        mqtt=mqtt,
        topics=TopicBuilder(environment="PRODUCTION"),
        station_id="LAB-1",
        device_id="pi-001",
        environment="PRODUCTION",
        simulated=False,
        allow_lab_simulator_commands=False,
    )
    write_set_price_outcome(
        SetPriceOutcome(
            correlation_id="cca2e75a-5b6f-4085-b72b-2d4066fb73c2",
            command_id="cmd-cca2",
            station_id=None,
            pump_id="pump-1",
            unit_price_raw=1370,
            execution_status="SENT_UNVERIFIED",
            accepted=True,
            applied_addresses=(),
            gave_up_addresses=(),
            deferred_addresses=(),
            unverified_addresses=(1, 2),
            detail="cd5_link_ack_dc3_idle_or_timeout",
        )
    )
    assert await intake.publish_pending_set_price_outcomes() == 1
    first_payload = mqtt.publish.await_args.args[1]
    assert "SENT_UNVERIFIED" in first_payload
    assert "cmd-result-final:cca2e75a-5b6f-4085-b72b-2d4066fb73c2:SENT_UNVERIFIED" in (
        first_payload
    )

    write_set_price_outcome(
        SetPriceOutcome(
            correlation_id="cca2e75a-5b6f-4085-b72b-2d4066fb73c2",
            command_id="cmd-cca2",
            station_id=None,
            pump_id="pump-1",
            unit_price_raw=1370,
            execution_status="PRICE_CONFIRMED",
            accepted=True,
            applied_addresses=(1, 2),
            gave_up_addresses=(),
            deferred_addresses=(),
            unverified_addresses=(),
            detail="dc3_late_match_after_sent_unverified",
        )
    )
    assert await intake.publish_pending_set_price_outcomes() == 1
    second_payload = mqtt.publish.await_args.args[1]
    assert "PRICE_CONFIRMED" in second_payload
    assert "cmd-result-final:cca2e75a-5b6f-4085-b72b-2d4066fb73c2:PRICE_CONFIRMED" in (
        second_payload
    )
    assert mqtt.publish.await_count == 2


@pytest.mark.asyncio
async def test_superseding_price_blocks_late_unverified_upgrade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import (
        SetPriceOutcome,
        SetPricePendingVerify,
        list_set_price_pending_verifies,
        write_set_price_outcome,
        write_set_price_pending_verify,
    )

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_outcome(
        SetPriceOutcome(
            correlation_id="corr-old",
            command_id="cmd-corr-old",
            station_id=None,
            pump_id="pump-1",
            unit_price_raw=1365,
            execution_status="SENT_UNVERIFIED",
            accepted=True,
            applied_addresses=(),
            gave_up_addresses=(),
            deferred_addresses=(),
            unverified_addresses=(1, 2),
            detail="cd5_link_ack_dc3_idle_or_timeout",
        )
    )
    write_set_price_pending_verify(
        SetPricePendingVerify(
            correlation_id="corr-old",
            command_id="cmd-corr-old",
            station_id=None,
            pump_id="pump-1",
            unit_price_raw=1365,
            required_addresses=(1, 2),
        )
    )
    _write_price(tmp_path, monkeypatch, "corr-new", 1400)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        # Face still shows old price from prior command.
        loop.sessions[addr].state.unit_price_raw = 1365
        loop.sessions[addr].state.unit_price_obs_gen = 9
    loop._run_owned_command = _confirmed()  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    # Old SENT_UNVERIFIED must not upgrade while newer request is active.
    old = [o for o in list_set_price_outcomes() if o.correlation_id == "corr-old"]
    assert len(old) == 1
    assert old[0].execution_status == "SENT_UNVERIFIED"
    assert list_set_price_pending_verifies() == []


@pytest.mark.asyncio
async def test_nonmatching_fresh_dc3_does_not_resend_cd5(
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
    first = link.await_count

    for addr in (1, 2):
        loop.sessions[addr].state.unit_price_obs_gen = 3
        loop.sessions[addr].state.unit_price_raw = 1699
    await loop._apply_pending_cloud_set_price()

    assert link.await_count == first
    assert loop._cloud_set_price_awaiting_dc3.get("corr-mismatch")
    assert not loop._cloud_set_price_applied.get("corr-mismatch")
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
        "amount_raw": 3387500,
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
    assert loop._last_completed_sale[1]["amount_raw"] == 3387500
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
    assert loop.sessions[1].state.filled_amount_raw == 3387500
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


def test_unchanged_reset_in_payload_refreshes_observation_age() -> None:
    """Repeated RESET+IN DATA must advance timestamps without a state change."""
    from intelipump_fdc.controller.pump_session import PumpSession
    from intelipump_fdc.controller.session_events import EventBus
    from intelipump_fdc.domain.pump_event import PumpEvent
    from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
    from intelipump_fdc.state_machine.models import ObservationRef
    from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation

    session = PumpSession(address=1, pump_id="pump-1", events=EventBus())
    session.state.observed_status = ObservedStatus.RESET
    session.state.nozzle_position = NozzlePosition.IN
    session.state.last_status_time = 100.0
    session.state.last_nozio_time = 100.0

    mapped = MappedWayneObservation(
        event=PumpEvent.RESET_OBSERVED,
        observation=ObservationRef(raw_wayne_status=int(WaynePumpStatus.RESET)),
        raw_wayne_status=int(WaynePumpStatus.RESET),
        nozzle_out=False,
    )
    session._update_observed_from_mapped(mapped, capture_mono=150.0)
    assert session.state.observed_status is ObservedStatus.RESET
    assert session.state.nozzle_position is NozzlePosition.IN
    assert session.state.last_status_time == 150.0
    assert session.state.last_nozio_time == 150.0

    session._update_observed_from_mapped(mapped, capture_mono=175.0)
    assert session.state.last_status_time == 175.0
    assert session.state.last_nozio_time == 175.0


@pytest.mark.asyncio
async def test_idle_stale_reset_in_refreshes_then_applies_cd5_without_reset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Dashboard price on idle RESET+IN must refresh evidence then CD5 (no RESET)."""
    _write_price(
        tmp_path, monkeypatch, "564f784f-7389-468b-affe-b67eb9b1525d", 1365
    )
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.last_nozio_time = time.monotonic() - 50.0
        loop.sessions[addr].state.last_status_time = time.monotonic() - 50.0
    assert loop._set_price_defer_reason(1, loop.sessions[1]) == "nozzle_stale"

    async def _run(session, *args, **kwargs):
        label = kwargs.get("command_label") or ""
        if "RETURN_STATUS" in label:
            now = time.monotonic()
            session.state.last_status_time = now
            session.state.last_nozio_time = now
            session.state.observed_status = ObservedStatus.RESET
            session.state.nozzle_position = NozzlePosition.IN
            return type(
                "R", (), {"status": ExchangeResultStatus.APPLICATION_CONFIRMED}
            )()
        assert "RESET" not in label
        return type(
            "R", (), {"status": ExchangeResultStatus.APPLICATION_CONFIRMED}
        )()

    loop._run_owned_command = AsyncMock(side_effect=_run)  # type: ignore[method-assign]
    loop._write_frame = AsyncMock(return_value=(time.monotonic(), time.monotonic()))  # type: ignore[method-assign]
    loop._read_poll_session = AsyncMock()  # type: ignore[method-assign]
    loop._drain_pending_data = AsyncMock(return_value=False)  # type: ignore[method-assign]

    await loop._apply_pending_cloud_set_price()

    labels = [
        (c.args[0].address, c.kwargs.get("command_label"))
        for c in loop._run_owned_command.await_args_list
    ]
    assert any("RETURN_STATUS" in (lab or "") for _, lab in labels)
    assert (1, "CD5_SET_PRICE_CLOUD") in labels
    assert (2, "CD5_SET_PRICE_CLOUD") in labels
    assert not any("RESET" in (lab or "") for _, lab in labels)
    assert read_set_price_request() is None
    outcome = consume_set_price_outcome()
    assert outcome is not None
    assert outcome.execution_status == "PRICE_CONFIRMED"
    assert outcome.unit_price_raw == 1365
    assert outcome.correlation_id == "564f784f-7389-468b-affe-b67eb9b1525d"


@pytest.mark.asyncio
async def test_link_ack_alone_does_not_refresh_stale_nozzle_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_price(tmp_path, monkeypatch, "corr-ack-only", 1365)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.last_nozio_time = time.monotonic() - 50.0
        loop.sessions[addr].state.last_status_time = time.monotonic() - 50.0

    async def _run(session, *args, **kwargs):
        # ACK only — no DATA timestamp advancement.
        return type("R", (), {"status": ExchangeResultStatus.LINK_ACKNOWLEDGED})()

    loop._run_owned_command = AsyncMock(side_effect=_run)  # type: ignore[method-assign]
    loop._write_frame = AsyncMock(return_value=(time.monotonic(), time.monotonic()))  # type: ignore[method-assign]
    loop._read_poll_session = AsyncMock()  # type: ignore[method-assign]
    loop._drain_pending_data = AsyncMock(return_value=False)  # type: ignore[method-assign]
    loop._set_price_refresh_min_interval_s = 0.0

    await loop._apply_pending_cloud_set_price()

    assert not any(
        "CD5" in (c.kwargs.get("command_label") or "")
        for c in loop._run_owned_command.await_args_list
    )
    assert loop._set_price_defer_reason(1, loop.sessions[1]) == "nozzle_stale"
    assert read_set_price_request() is not None
    assert list_set_price_outcomes() == []


@pytest.mark.asyncio
async def test_stale_idle_refresh_is_triggered_for_reset_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: RESET+IN stale must call refresh (previously skipped)."""
    _write_price(tmp_path, monkeypatch, "corr-refresh-trigger", 1365)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.last_nozio_time = time.monotonic() - 44.0
        loop.sessions[addr].state.last_status_time = time.monotonic() - 44.0

    refreshed: list[int] = []

    async def _refresh(session):
        refreshed.append(session.address)
        now = time.monotonic()
        session.state.last_status_time = now
        session.state.last_nozio_time = now
        return True

    loop._refresh_set_price_target_evidence = _refresh  # type: ignore[method-assign]
    loop._run_owned_command = _confirmed()  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    assert set(refreshed) == {1, 2}
    assert read_set_price_request() is None


@pytest.mark.asyncio
async def test_owned_lab_startup_price_does_not_override_persisted_dashboard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import write_persisted_unit_price
    from intelipump_fdc.controller.feature_flags import WayneFeatureFlags

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1365, (1365,), source="cloud")
    loop = _dual_addr_loop()
    loop.runtime.feature_flags = WayneFeatureFlags(
        poll_and_observe=False,
        automatic_startup_price_programming=True,
        automatic_reset=False,
        automatic_authorization=False,
    )
    loop.runtime.startup_unit_price = 1300  # stale CLI default
    _fresh_in(loop.sessions[1], status=ObservedStatus.RESET)
    loop.sessions[1].state.unit_price_raw = 1365  # face already matches dashboard

    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._owned_lab_tick(loop.sessions[1])

    assert loop.runtime.startup_unit_price == 1365
    assert 1 in loop._price_programmed
    assert ok.await_count == 0  # no CD5 overwrite


@pytest.mark.asyncio
async def test_owned_lab_startup_uses_persisted_when_face_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import write_persisted_unit_price
    from intelipump_fdc.controller.feature_flags import WayneFeatureFlags

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1365, (1365,), source="cloud")
    loop = _dual_addr_loop()
    loop.runtime.feature_flags = WayneFeatureFlags(
        poll_and_observe=False,
        automatic_startup_price_programming=True,
        automatic_reset=False,
        automatic_authorization=False,
    )
    loop.runtime.startup_unit_price = 1300
    _fresh_in(loop.sessions[1], status=ObservedStatus.RESET)
    loop.sessions[1].state.unit_price_raw = 1300

    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._owned_lab_tick(loop.sessions[1])

    assert loop.runtime.startup_unit_price == 1365
    assert ok.await_count == 1
    # CD5 programmed the persisted dashboard price, not CLI 1300.
    assert 1 in loop._price_programmed



@pytest.mark.asyncio
async def test_unit_price_persisted_once_across_many_polls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: DC3 confirm must not rewrite unit-price.json every poll."""
    from intelipump_fdc.cloud.set_price_request import read_persisted_unit_price

    _write_price(tmp_path, monkeypatch, "882b3b45-c38b-4d51-b810-b0c59542a7f5", 1356)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
        loop.sessions[addr].state.unit_price_raw = 1000
        loop.sessions[addr].state.unit_price_obs_gen = 7

    link = AsyncMock(
        return_value=type(
            "R",
            (),
            {"status": ExchangeResultStatus.LINK_ACKNOWLEDGED},
        )()
    )
    loop._run_owned_command = link  # type: ignore[method-assign]

    writes = {"n": 0}
    from intelipump_fdc.cloud import set_price_request as spr

    real_write = spr.write_persisted_unit_price

    def _counting_write(*args, **kwargs):
        writes["n"] += 1
        return real_write(*args, **kwargs)

    monkeypatch.setattr(spr, "write_persisted_unit_price", _counting_write)

    await loop._apply_pending_cloud_set_price()
    assert loop._cloud_set_price_awaiting_dc3.get(
        "882b3b45-c38b-4d51-b810-b0c59542a7f5"
    )
    assert writes["n"] == 1  # persist on LINK_ACK

    for addr in (1, 2):
        loop.sessions[addr].state.unit_price_obs_gen = 8
        loop.sessions[addr].state.unit_price_raw = 1356
    await loop._apply_pending_cloud_set_price()
    assert writes["n"] == 1  # confirm does not rewrite
    assert read_persisted_unit_price() is not None
    assert read_persisted_unit_price().unit_price_raw == 1356

    # Many subsequent polls / delayed cloud ACK: request may remain if we
    # force applied state with pending request — must not rewrite.
    write_set_price_request(
        SetPriceRequest(
            correlation_id="882b3b45-c38b-4d51-b810-b0c59542a7f5",
            command_id="cmd-882b3b45-c38b-4d51-b810-b0c59542a7f5",
            unit_price_raw=1356,
            prices_raw=(1356,),
            pump_id="pump-1",
        )
    )
    # Simulate partial settle wait: addr1 applied, addr2 deferred hang-up.
    loop._cloud_set_price_applied["882b3b45-c38b-4d51-b810-b0c59542a7f5"] = {1}
    loop._cloud_set_price_unit_persisted["882b3b45-c38b-4d51-b810-b0c59542a7f5"] = 1356
    loop.sessions[2].state.observed_status = ObservedStatus.FILLING_COMPLETED
    loop.sessions[2].state.nozzle_position = NozzlePosition.OUT
    for _ in range(20):
        await loop._apply_pending_cloud_set_price()
    assert writes["n"] == 1

@pytest.mark.asyncio
async def test_delayed_cloud_ack_does_not_rewrite_persisted_price(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import (
        SetPriceOutcome,
        read_persisted_unit_price,
        write_set_price_outcome,
    )

    _write_price(tmp_path, monkeypatch, "corr-ack-hold", 1356)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
    loop._run_owned_command = _confirmed()  # type: ignore[method-assign]

    writes = {"n": 0}
    from intelipump_fdc.cloud import set_price_request as spr

    real_write = spr.write_persisted_unit_price

    def _counting_write(*args, **kwargs):
        writes["n"] += 1
        return real_write(*args, **kwargs)

    monkeypatch.setattr(spr, "write_persisted_unit_price", _counting_write)
    await loop._apply_pending_cloud_set_price()
    assert writes["n"] == 1
    assert read_set_price_request() is None
    first = read_persisted_unit_price()
    assert first is not None
    first_updated = first.updated_at

    # Outcome still waiting for cloud ACK; leftover request must not rewrite.
    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-ack-hold",
            command_id="cmd-corr-ack-hold",
            unit_price_raw=1356,
            prices_raw=(1356,),
            pump_id="pump-1",
        )
    )
    assert has_set_price_outcome("corr-ack-hold") or list_set_price_outcomes()
    for _ in range(10):
        await loop._apply_pending_cloud_set_price()
    assert writes["n"] == 1
    assert read_persisted_unit_price().updated_at == first_updated
    assert read_set_price_request() is None


@pytest.mark.asyncio
async def test_persist_failure_retries_then_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import read_persisted_unit_price

    _write_price(tmp_path, monkeypatch, "corr-persist-fail", 1400)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]

    from intelipump_fdc.cloud import set_price_request as spr

    real_write = spr.write_persisted_unit_price
    calls = {"n": 0}

    def _flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("disk full")
        return real_write(*args, **kwargs)

    monkeypatch.setattr(spr, "write_persisted_unit_price", _flaky)
    await loop._apply_pending_cloud_set_price()
    assert calls["n"] == 1
    assert read_persisted_unit_price() is None
    assert read_set_price_request() is not None
    assert list_set_price_outcomes() == []
    # Applied addresses retained — no CD5 storm while waiting for persist.
    cd5_first = sum(
        1 for c in ok.await_args_list if "CD5" in (c.kwargs.get("command_label") or "")
    )
    ok.reset_mock()
    loop._cloud_set_price_persist_next_try.pop("corr-persist-fail", None)
    await loop._apply_pending_cloud_set_price()
    assert calls["n"] == 2
    assert read_persisted_unit_price() is not None
    assert read_persisted_unit_price().unit_price_raw == 1400
    assert read_set_price_request() is None
    assert consume_set_price_outcome() is not None
    assert not any(
        "CD5" in (c.kwargs.get("command_label") or "") for c in ok.await_args_list
    )
    assert cd5_first >= 1


@pytest.mark.asyncio
async def test_later_price_change_persists_new_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import (
        consume_set_price_outcome,
        read_persisted_unit_price,
    )

    _write_price(tmp_path, monkeypatch, "corr-price-a", 1356)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
    loop._run_owned_command = _confirmed()  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    assert read_persisted_unit_price().unit_price_raw == 1356
    consume_set_price_outcome()

    _write_price(tmp_path, monkeypatch, "corr-price-b", 1410)
    loop2 = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop2.sessions[addr], status=ObservedStatus.RESET)
    loop2._run_owned_command = _confirmed()  # type: ignore[method-assign]
    await loop2._apply_pending_cloud_set_price()
    assert read_persisted_unit_price().unit_price_raw == 1410


@pytest.mark.asyncio
async def test_outcome_retry_does_not_resend_cd5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """While outcome awaits ACK / request leftover, do not re-issue CD5."""
    from intelipump_fdc.cloud.set_price_request import write_set_price_outcome

    _write_price(tmp_path, monkeypatch, "corr-no-recd5", 1356)
    loop = _dual_addr_loop()
    for addr in (1, 2):
        _fresh_in(loop.sessions[addr], status=ObservedStatus.RESET)
    ok = _confirmed()
    loop._run_owned_command = ok  # type: ignore[method-assign]
    await loop._apply_pending_cloud_set_price()
    first_cd5 = sum(
        1 for c in ok.await_args_list if "CD5" in (c.kwargs.get("command_label") or "")
    )
    assert first_cd5 >= 1

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-no-recd5",
            command_id="cmd-corr-no-recd5",
            unit_price_raw=1356,
            prices_raw=(1356,),
            pump_id="pump-1",
        )
    )
    ok.reset_mock()
    for _ in range(5):
        await loop._apply_pending_cloud_set_price()
    assert ok.await_count == 0
    assert not any(
        "CD5" in (c.kwargs.get("command_label") or "") for c in ok.await_args_list
    )


@pytest.mark.asyncio
async def test_restart_between_persist_and_outcome_keeps_price(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import (
        SetPriceOutcome,
        read_persisted_unit_price,
        write_persisted_unit_price,
        write_set_price_outcome,
    )

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_persisted_unit_price(1356, (1356,), source="cloud")
    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-restart-persist",
            command_id="cmd-corr-restart-persist",
            unit_price_raw=1356,
            prices_raw=(1356,),
            pump_id="pump-1",
        )
    )
    write_set_price_outcome(
        SetPriceOutcome(
            correlation_id="corr-restart-persist",
            command_id="cmd-corr-restart-persist",
            station_id=None,
            pump_id="pump-1",
            unit_price_raw=1356,
            execution_status="PRICE_CONFIRMED",
            accepted=True,
            applied_addresses=(1, 2),
            gave_up_addresses=(),
            deferred_addresses=(),
            detail="cd5_application_or_dc3_confirmed",
        )
    )
    loop = _dual_addr_loop()
    writes = {"n": 0}
    from intelipump_fdc.cloud import set_price_request as spr

    real_write = spr.write_persisted_unit_price

    def _counting_write(*args, **kwargs):
        writes["n"] += 1
        return real_write(*args, **kwargs)

    monkeypatch.setattr(spr, "write_persisted_unit_price", _counting_write)
    await loop._apply_pending_cloud_set_price()
    assert writes["n"] == 0
    assert read_persisted_unit_price().unit_price_raw == 1356
    assert read_set_price_request() is None

