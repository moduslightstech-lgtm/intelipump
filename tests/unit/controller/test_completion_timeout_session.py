"""Controller session wiring for hang-up completion timeout."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext
from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation


def _session(*, events: EventBus | None = None) -> PumpSession:
    return PumpSession(
        address=1,
        pump_id="p1",
        events=events or EventBus(),
        awaiting_filling_complete_timeout=timedelta(seconds=5),
        dc2_stability_window=timedelta(seconds=0),
    )


def _filling_ctx(**kwargs: object) -> PumpContext:
    base: dict[str, object] = dict(
        pump_id="p1",
        dart_address=1,
        current_state=PumpState.FILLING,
        communication_healthy=True,
        nozzle_out=True,
        selected_nozzle=1,
        active_transaction_id="tx-hang",
        dispensed_volume_raw=2000,
        last_raw_wayne_status=4,
        state_version=3,
    )
    base.update(kwargs)
    return PumpContext(**base)  # type: ignore[arg-type]


def test_nozzle_return_during_filling_starts_timeout() -> None:
    s = _session()
    s.machine = PumpStateMachine(_filling_ctx())
    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.NOZZLE_RETURNED,
            observation=ObservationRef(source_frame_raw_hex="aa"),
            nozzle_out=False,
            awaiting_filling_complete=True,
            completion_evidence_key="hang:frame1",
        )
    )
    assert s.machine.context.awaiting_filling_complete is True
    assert s.machine.context.current_state is PumpState.FILLING_COMPLETE
    assert s._await_started_at is not None


def test_dc1_filling_complete_cancels_timeout() -> None:
    bus = EventBus()
    published: list[str] = []

    def _cap(event: ControllerEvent) -> None:
        if event.type is ControllerEventType.STATE_CHANGED:
            published.append(str((event.payload or {}).get("event")))

    bus.add_subscriber(_cap)
    s = _session(events=bus)
    s.machine = PumpStateMachine(
        _filling_ctx(
            current_state=PumpState.FILLING_COMPLETE,
            previous_state=PumpState.FILLING,
            awaiting_filling_complete=True,
            nozzle_out=False,
        )
    )
    s._await_started_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=ObservationRef(source_frame_raw_hex="bb"),
            raw_wayne_status=5,
            awaiting_filling_complete=False,
            completion_evidence_key="complete:bb:5",
        )
    )
    assert s.machine.context.awaiting_filling_complete is False
    assert s._await_started_at is None
    assert "FILLING_COMPLETED" in published
    s.tick_awaiting_completion(now=datetime(2026, 7, 1, 10, 0, 30, tzinfo=UTC))
    assert s.machine.context.completion_inferred is False


def test_repeated_nozzle_in_does_not_restart_timer() -> None:
    s = _session()
    s.machine = PumpStateMachine(
        _filling_ctx(
            current_state=PumpState.FILLING_COMPLETE,
            awaiting_filling_complete=True,
            nozzle_out=False,
        )
    )
    first = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    s._await_started_at = first
    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.NOZZLE_STATUS_OBSERVED,
            observation=ObservationRef(source_frame_raw_hex="cc"),
            nozzle_out=False,
        )
    )
    assert s._await_started_at == first


def test_final_dc2_after_nozzle_return_is_tracked() -> None:
    s = _session()
    started = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    s.machine = PumpStateMachine(
        _filling_ctx(
            current_state=PumpState.FILLING_COMPLETE,
            awaiting_filling_complete=True,
            nozzle_out=False,
            dispensed_volume_raw=2000,
        )
    )
    s._await_started_at = started
    s._last_dc2_volume = 2000
    s._dc2_last_changed_at = started
    later = started + timedelta(seconds=1)
    s._note_dc2_volume(2500, at=later)
    assert s._last_dc2_volume == 2500
    assert s._dc2_last_changed_at == later
    # Session still awaiting; volume note does not finalize.
    assert s.machine.context.awaiting_filling_complete is True


def test_timeout_stable_dc2_infers_once() -> None:
    bus = EventBus()
    completions: list[dict[str, object]] = []

    def _capture(event: ControllerEvent) -> None:
        if event.type is not ControllerEventType.STATE_CHANGED:
            return
        payload = event.payload or {}
        if payload.get("event") == "FILLING_COMPLETED":
            completions.append(dict(payload))

    bus.add_subscriber(_capture)
    s = _session(events=bus)
    started = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    s.machine = PumpStateMachine(
        _filling_ctx(
            current_state=PumpState.FILLING_COMPLETE,
            previous_state=PumpState.FILLING,
            awaiting_filling_complete=True,
            nozzle_out=False,
            last_observation_at=started,
        )
    )
    s._await_started_at = started
    s._dc2_last_changed_at = started
    s._last_dc2_volume = 2000

    s.tick_awaiting_completion(now=started + timedelta(seconds=6))
    assert s.machine.context.completion_inferred is True
    assert s.machine.context.awaiting_filling_complete is False
    assert len(completions) == 1
    assert completions[0].get("completion_inferred") is True

    s.tick_awaiting_completion(now=started + timedelta(seconds=20))
    assert len(completions) == 1


def test_timeout_insufficient_leaves_unresolved() -> None:
    s = _session()
    started = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    s.machine = PumpStateMachine(
        _filling_ctx(
            current_state=PumpState.FILLING_COMPLETE,
            awaiting_filling_complete=True,
            nozzle_out=False,
            dispensed_volume_raw=None,
            last_observation_at=started,
        )
    )
    s._await_started_at = started
    warnings: list[str] = []

    def _cap(event: ControllerEvent) -> None:
        if event.type is ControllerEventType.STATE_CHANGED:
            payload = event.payload or {}
            if payload.get("event") == "RECONCILIATION_WARNING":
                warnings.extend(str(w) for w in (payload.get("warnings") or []))

    s.events.add_subscriber(_cap)
    s.tick_awaiting_completion(now=started + timedelta(seconds=10))
    assert s.machine.context.awaiting_filling_complete is True
    assert s.machine.context.completion_inferred is False
    assert any("insufficient evidence" in w for w in warnings)
    s.tick_awaiting_completion(now=started + timedelta(seconds=20))
    assert sum(1 for w in warnings if "insufficient evidence" in w) == 1


def test_timeout_after_confirmed_completion_noop() -> None:
    s = _session()
    s.machine = PumpStateMachine(
        _filling_ctx(
            current_state=PumpState.FILLING_COMPLETE,
            awaiting_filling_complete=False,
            completion_inferred=False,
            nozzle_out=False,
        )
    )
    s._await_started_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    before = s.machine.context.state_version
    s.tick_awaiting_completion(now=datetime(2026, 7, 1, 10, 1, tzinfo=UTC))
    assert s.machine.context.state_version == before
