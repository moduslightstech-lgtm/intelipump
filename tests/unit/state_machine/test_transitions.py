"""Unit tests for normalized pump state transitions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import OPERATIONAL_STATES, PumpState
from intelipump_fdc.state_machine.errors import TransitionSeverity
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext
from intelipump_fdc.state_machine.transitions import TRANSITION_TABLE


def _ctx(state: PumpState = PumpState.DISCONNECTED, **kwargs: object) -> PumpContext:
    # READY contexts need readiness gates so finalize_readiness does not
    # immediately revoke to RESET under default communication_healthy=False.
    if state is PumpState.READY:
        kwargs.setdefault("communication_healthy", True)
        kwargs.setdefault("nozzle_out", False)
        kwargs.setdefault("last_raw_wayne_status", 1)
        kwargs.setdefault("was_ready_derivable", True)
    return PumpContext(pump_id="p1", dart_address=1, current_state=state, **kwargs)  # type: ignore[arg-type]


def _apply(state: PumpState, event: PumpEvent, **kwargs: object):
    machine = PumpStateMachine(_ctx(state))
    return machine, machine.apply(event, **kwargs)  # type: ignore[arg-type]


VALID_CASES: list[tuple[PumpState, PumpEvent, PumpState]] = [
    (PumpState.DISCONNECTED, PumpEvent.COMMUNICATION_STARTED, PumpState.DISCOVERING),
    (PumpState.DISCOVERING, PumpEvent.PUMP_DISCOVERED, PumpState.RESET),
    (PumpState.DISCOVERING, PumpEvent.CONFIGURATION_MISSING, PumpState.NOT_PROGRAMMED),
    (PumpState.DISCOVERING, PumpEvent.RESET_OBSERVED, PumpState.RESET),
    (PumpState.DISCOVERING, PumpEvent.READY_OBSERVED, PumpState.READY),
    (PumpState.NOT_PROGRAMMED, PumpEvent.FILLING_COMPLETED, PumpState.FILLING_COMPLETE),
    (PumpState.RESET, PumpEvent.READY_OBSERVED, PumpState.READY),
    (PumpState.RESET, PumpEvent.AUTHORIZATION_CONFIRMED, PumpState.AUTHORIZED),
    (PumpState.READY, PumpEvent.NOZZLE_LIFTED, PumpState.NOZZLE_UP),
    (PumpState.READY, PumpEvent.AUTHORIZATION_CONFIRMED, PumpState.AUTHORIZED),
    (PumpState.NOZZLE_UP, PumpEvent.AUTHORIZATION_CONFIRMED, PumpState.AUTHORIZED),
    (PumpState.NOZZLE_UP, PumpEvent.NOZZLE_RETURNED, PumpState.RESET),
    (PumpState.AUTHORIZED, PumpEvent.FILLING_STARTED, PumpState.FILLING),
    (PumpState.AUTHORIZED, PumpEvent.NOZZLE_RETURNED, PumpState.RESET),
    (PumpState.FILLING, PumpEvent.FILLING_UPDATED, PumpState.FILLING),
    (PumpState.FILLING, PumpEvent.FILLING_COMPLETED, PumpState.FILLING_COMPLETE),
    (PumpState.FILLING, PumpEvent.NOZZLE_RETURNED, PumpState.FILLING_COMPLETE),
    (PumpState.FILLING, PumpEvent.SUSPENDED_OBSERVED, PumpState.SUSPENDED),
    (PumpState.SUSPENDED, PumpEvent.RESUMED_OBSERVED, PumpState.FILLING),
    (PumpState.SUSPENDED, PumpEvent.NOZZLE_RETURNED, PumpState.FILLING_COMPLETE),
    (PumpState.FILLING, PumpEvent.LIMIT_REACHED, PumpState.LIMIT_REACHED),
    (PumpState.LIMIT_REACHED, PumpEvent.NOZZLE_RETURNED, PumpState.FILLING_COMPLETE),
    (PumpState.FILLING_COMPLETE, PumpEvent.RESET_OBSERVED, PumpState.RESET),
    (PumpState.FAULTED, PumpEvent.FAULT_CLEARED, PumpState.DISCOVERING),
    (PumpState.MAINTENANCE, PumpEvent.MAINTENANCE_EXITED, PumpState.DISCOVERING),
]


@pytest.mark.parametrize(("from_state", "event", "to_state"), VALID_CASES)
def test_valid_transitions(
    from_state: PumpState, event: PumpEvent, to_state: PumpState
) -> None:
    _machine, result = _apply(from_state, event)
    assert result.accepted is True
    assert result.current_state is to_state
    if from_state is to_state:
        # Self-transitions (e.g. FILLING + FILLING_UPDATED) are accepted no-ops.
        assert result.noop is True
    else:
        assert result.noop is False
        assert result.previous_state is from_state
        assert result.context.state_version == 1


def test_nozzle_up_filling_started_rejected_without_implicit_flag() -> None:
    _machine, result = _apply(PumpState.NOZZLE_UP, PumpEvent.FILLING_STARTED)
    assert result.accepted is False
    assert result.reason == "implicit_authorize_not_supported"
    assert result.current_state is PumpState.NOZZLE_UP


def test_nozzle_up_filling_started_allowed_with_implicit_flag() -> None:
    machine = PumpStateMachine(_ctx(PumpState.NOZZLE_UP))
    result = machine.apply(
        PumpEvent.FILLING_STARTED,
        allow_implicit_authorize_to_filling=True,
    )
    assert result.accepted is True
    assert result.current_state is PumpState.FILLING


def test_invalid_transition_returns_typed_result() -> None:
    _machine, result = _apply(PumpState.READY, PumpEvent.FILLING_COMPLETED)
    assert result.accepted is False
    assert result.reason == "invalid_transition"
    assert result.severity is TransitionSeverity.ERROR
    assert result.current_state is PumpState.READY
    assert result.attempted_event is PumpEvent.FILLING_COMPLETED


def test_disconnected_authorize_event_rejected() -> None:
    # AUTHORIZE is a command; AUTHORIZATION_CONFIRMED from DISCONNECTED is invalid.
    _machine, result = _apply(PumpState.DISCONNECTED, PumpEvent.AUTHORIZATION_CONFIRMED)
    assert result.accepted is False
    assert result.current_state is PumpState.DISCONNECTED


@pytest.mark.parametrize("state", [*OPERATIONAL_STATES, PumpState.DISCONNECTED])
def test_communication_loss_from_every_state(state: PumpState) -> None:
    machine = PumpStateMachine(
        _ctx(state, communication_healthy=True, state_version=3)
    )
    result = machine.apply(PumpEvent.COMMUNICATION_LOST)
    assert result.accepted is True
    assert result.current_state is PumpState.DISCONNECTED
    assert result.context.communication_healthy is False


@pytest.mark.parametrize("state", sorted(OPERATIONAL_STATES, key=lambda s: s.value))
def test_fault_from_every_operational_state(state: PumpState) -> None:
    if state is PumpState.FAULTED:
        machine = PumpStateMachine(_ctx(state, fault_code=1))
        result = machine.apply(PumpEvent.FAULT_OBSERVED, fault_code=1)
        assert result.accepted is True
        assert result.noop is True
        return
    _machine, result = _apply(state, PumpEvent.FAULT_OBSERVED, fault_code=42)
    assert result.accepted is True
    assert result.current_state is PumpState.FAULTED


def test_duplicate_ready_is_noop() -> None:
    machine = PumpStateMachine(
        _ctx(
            PumpState.READY,
            state_version=5,
            communication_healthy=True,
            nozzle_out=False,
            last_raw_wayne_status=1,
        )
    )
    result = machine.apply(
        PumpEvent.READY_OBSERVED,
        raw_wayne_status=1,
        nozzle_out=False,
    )
    version_after_first = result.context.state_version
    result2 = machine.apply(
        PumpEvent.READY_OBSERVED,
        raw_wayne_status=1,
        nozzle_out=False,
    )
    assert result2.accepted is True
    assert result2.noop is True
    assert result2.current_state is PumpState.READY
    assert result2.context.state_version == version_after_first


def test_unknown_observation_preserves_state() -> None:
    machine = PumpStateMachine(
        _ctx(
            PumpState.READY,
            state_version=4,
            communication_healthy=True,
            nozzle_out=False,
            last_raw_wayne_status=1,
        )
    )
    result = machine.apply(
        PumpEvent.UNKNOWN_OBSERVATION,
        observation=ObservationRef(source_frame_raw_hex="FRAME"),
    )
    assert result.accepted is True
    assert result.noop is True
    assert result.current_state is PumpState.READY


def test_state_version_changes_only_on_meaningful_changes() -> None:
    machine = PumpStateMachine(
        _ctx(
            PumpState.READY,
            state_version=10,
            communication_healthy=True,
            nozzle_out=False,
            last_raw_wayne_status=1,
        )
    )
    # Same-state READY with no field changes: version stays.
    result = machine.apply(
        PumpEvent.READY_OBSERVED,
        raw_wayne_status=1,
        nozzle_out=False,
    )
    assert result.noop is True
    assert result.context.state_version == 10
    # Meaningful selected_nozzle change bumps version.
    result2 = machine.apply(
        PumpEvent.NOZZLE_STATUS_OBSERVED,
        selected_nozzle=2,
        nozzle_out=False,
        raw_wayne_status=1,
    )
    assert result2.context.state_version == 11
    assert result2.current_state is PumpState.READY

def test_repeated_filling_update_preserves_state() -> None:
    machine = PumpStateMachine(_ctx(PumpState.FILLING, state_version=2))
    r1 = machine.apply(PumpEvent.FILLING_UPDATED)
    r2 = machine.apply(PumpEvent.FILLING_UPDATED)
    assert r1.current_state is PumpState.FILLING
    assert r2.current_state is PumpState.FILLING
    assert r2.noop is True


def test_duplicate_completion_evidence_not_emitted_twice() -> None:
    machine = PumpStateMachine(_ctx(PumpState.FILLING))
    key = "complete:frameA:5"
    r1 = machine.apply(
        PumpEvent.FILLING_COMPLETED,
        completion_evidence_key=key,
        observation=ObservationRef(source_frame_raw_hex="AA"),
    )
    assert r1.accepted is True
    assert r1.current_state is PumpState.FILLING_COMPLETE
    r2 = machine.apply(
        PumpEvent.FILLING_COMPLETED,
        completion_evidence_key=key,
        observation=ObservationRef(source_frame_raw_hex="BB"),
    )
    assert r2.accepted is True
    assert r2.noop is True
    assert r2.reason == "duplicate_completion_evidence"
    assert r2.current_state is PumpState.FILLING_COMPLETE


def test_stale_event_does_not_roll_state_backward() -> None:
    t0 = datetime(2026, 7, 20, 12, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(seconds=10)
    machine = PumpStateMachine(
        _ctx(
            PumpState.FILLING,
            last_observation_at=t1,
            state_version=4,
        )
    )
    result = machine.apply(
        PumpEvent.NOZZLE_LIFTED,
        observed_at=t0,
    )
    assert result.accepted is False
    assert result.reason == "stale_observation"
    assert result.current_state is PumpState.FILLING


def test_fault_cleared_does_not_go_directly_to_filling() -> None:
    machine = PumpStateMachine(_ctx(PumpState.FAULTED))
    result = machine.apply(PumpEvent.FAULT_CLEARED)
    assert result.current_state is PumpState.DISCOVERING
    assert result.current_state is not PumpState.FILLING


def test_transition_table_non_empty() -> None:
    assert len(TRANSITION_TABLE) >= 40
