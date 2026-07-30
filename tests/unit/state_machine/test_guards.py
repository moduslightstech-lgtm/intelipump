"""Unit tests for command eligibility guards."""

from __future__ import annotations

from decimal import Decimal

from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.guards import evaluate_command_eligibility
from intelipump_fdc.state_machine.models import PumpContext


def _ctx(**kwargs: object) -> PumpContext:
    base = dict(
        pump_id="p1",
        dart_address=1,
        current_state=PumpState.READY,
        communication_healthy=True,
    )
    base.update(kwargs)
    return PumpContext(**base)  # type: ignore[arg-type]


def test_disconnected_authorize_rejected() -> None:
    result = evaluate_command_eligibility(
        PumpCommand.AUTHORIZE,
        _ctx(current_state=PumpState.DISCONNECTED),
    )
    assert result.eligible is False
    assert "state_not_authorize_eligible" in result.blocking_reasons
    assert result.requires_physical_enable is True
    assert result.requires_active_commands_enabled is True


def test_authorize_allowed_from_reset_ready_or_nozzle_up() -> None:
    for state in (PumpState.RESET, PumpState.READY, PumpState.NOZZLE_UP):
        result = evaluate_command_eligibility(
            PumpCommand.AUTHORIZE,
            _ctx(
                current_state=state,
                selected_nozzle=1,
                price_verified=True,
            ),
        )
        assert result.eligible is True, state


def test_filling_set_price_rejected() -> None:
    result = evaluate_command_eligibility(
        PumpCommand.SET_PRICE,
        _ctx(current_state=PumpState.FILLING),
    )
    assert result.eligible is False
    assert any("set_price" in r or "transaction" in r for r in result.blocking_reasons)


def test_nozzle_up_authorize_requires_verified_price() -> None:
    blocked = evaluate_command_eligibility(
        PumpCommand.AUTHORIZE,
        _ctx(
            current_state=PumpState.NOZZLE_UP,
            selected_nozzle=1,
            price_verified=False,
        ),
    )
    assert blocked.eligible is False
    assert "price_not_verified" in blocked.blocking_reasons

    ok = evaluate_command_eligibility(
        PumpCommand.AUTHORIZE,
        _ctx(
            current_state=PumpState.NOZZLE_UP,
            selected_nozzle=1,
            price_verified=True,
        ),
    )
    assert ok.eligible is True
    assert ok.requires_physical_enable is True
    assert ok.requires_active_commands_enabled is True


def test_filling_complete_reset_eligibility() -> None:
    result = evaluate_command_eligibility(
        PumpCommand.RESET,
        _ctx(current_state=PumpState.FILLING_COMPLETE),
    )
    assert result.eligible is True

    filling = evaluate_command_eligibility(
        PumpCommand.RESET,
        _ctx(current_state=PumpState.FILLING),
    )
    assert filling.eligible is False
    assert "reset_not_allowed_during_filling" in filling.blocking_reasons


def test_physical_enable_and_active_command_flags_on_active_commands() -> None:
    result = evaluate_command_eligibility(
        PumpCommand.STOP,
        _ctx(current_state=PumpState.FILLING),
        physical_enable_present=False,
        active_commands_enabled=False,
    )
    assert result.requires_physical_enable is True
    assert result.requires_active_commands_enabled is True
    assert any("physical_enable" in w for w in result.warnings)
    assert any("active_commands_enabled" in w for w in result.warnings)


def test_read_status_always_eligible() -> None:
    for state in PumpState:
        result = evaluate_command_eligibility(
            PumpCommand.READ_STATUS,
            _ctx(current_state=state, communication_healthy=False),
        )
        assert result.eligible is True
        assert result.requires_physical_enable is False
        assert result.requires_active_commands_enabled is False


def test_suspend_resume_stop_states() -> None:
    assert evaluate_command_eligibility(
        PumpCommand.SUSPEND, _ctx(current_state=PumpState.FILLING)
    ).eligible
    assert not evaluate_command_eligibility(
        PumpCommand.SUSPEND, _ctx(current_state=PumpState.READY)
    ).eligible
    assert evaluate_command_eligibility(
        PumpCommand.RESUME, _ctx(current_state=PumpState.SUSPENDED)
    ).eligible
    assert evaluate_command_eligibility(
        PumpCommand.STOP, _ctx(current_state=PumpState.AUTHORIZED)
    ).eligible


def test_preset_requires_positive_value_nozzle_and_price() -> None:
    result = evaluate_command_eligibility(
        PumpCommand.PRESET_AMOUNT,
        _ctx(
            current_state=PumpState.READY,
            selected_nozzle=1,
            price_verified=True,
        ),
        preset_value=Decimal("10.00"),
    )
    assert result.eligible is True

    bad = evaluate_command_eligibility(
        PumpCommand.PRESET_VOLUME,
        _ctx(current_state=PumpState.READY, selected_nozzle=1, price_verified=True),
        preset_value=0,
    )
    assert bad.eligible is False
    assert "preset_value_not_positive" in bad.blocking_reasons


def test_guards_do_not_mutate_context() -> None:
    ctx = _ctx(current_state=PumpState.NOZZLE_UP, selected_nozzle=1, price_verified=True)
    before = ctx.state_version
    evaluate_command_eligibility(PumpCommand.AUTHORIZE, ctx)
    assert ctx.state_version == before
    assert ctx.current_state is PumpState.NOZZLE_UP
