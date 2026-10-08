"""Holster-bounce / micro-fill completion hold."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from intelipump_fdc.controller.pump_session import (
    PumpSession,
    _MICRO_COMPLETE_RELIFT_HOLD,
    _NOZIO_STABLE_IN,
)
from intelipump_fdc.controller.sale_lifecycle import SaleLifecycle
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.controller.session_models import NozzlePosition
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext
from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation


def _session(*, events: EventBus | None = None) -> PumpSession:
    return PumpSession(address=2, pump_id="pump-2", events=events or EventBus())


def _filling_session(*, filling_age: timedelta) -> tuple[PumpSession, datetime]:
    now = datetime.now(UTC)
    s = _session()
    s.machine = PumpStateMachine(
        PumpContext(
            pump_id="pump-2",
            dart_address=2,
            current_state=PumpState.FILLING,
            communication_healthy=True,
            nozzle_out=True,
            selected_nozzle=1,
            active_transaction_id="tx-micro",
            dispensed_volume_raw=9,
        )
    )
    s.state.filled_volume_raw = 9
    s.state.filled_amount_raw = 12200
    s.state.unit_price_raw = 1355
    s.state.nozzle_position = NozzlePosition.OUT
    s._filling_seen_this_boot = True
    s._filling_started_at = now - filling_age
    s.state.sale_evidence.note_nozzle_out()
    s.state.sale_evidence.note_authorized(application_confirmed=True)
    s.state.sale_evidence.note_filling()
    s.state.sale_evidence.note_dc2(volume_raw=9, amount_raw=12200)
    return s, now


def test_micro_fill_completion_held_then_suppressed_on_relift() -> None:
    bus = EventBus()
    published: list[dict] = []

    def _cap(event: ControllerEvent) -> None:
        if event.type is ControllerEventType.STATE_CHANGED:
            published.append(dict(event.payload or {}))

    bus.add_subscriber(_cap)
    s, now = _filling_session(filling_age=timedelta(seconds=1))
    s.events = bus
    s._noz_in_edge_at = now
    s.state.nozzle_position = NozzlePosition.IN
    s.machine = PumpStateMachine(
        s.machine.context.with_updates(nozzle_out=False)
    )

    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=ObservationRef(source_frame_raw_hex="ghost122"),
            raw_wayne_status=5,
            nozzle_out=False,
            completion_evidence_key="complete:ghost122:5",
        )
    )
    assert s._held_completion is not None
    assert s.state.sale_lifecycle is SaleLifecycle.FILLING
    assert not any(
        p.get("event") == PumpEvent.FILLING_COMPLETED.value
        and p.get("may_publish_sale") is True
        for p in published
    )

    # Re-lift within hold window → suppress ghost COMPLETED.
    s.state.nozzle_position = NozzlePosition.OUT
    s._tick_completion_hold(now=now + timedelta(seconds=2))
    assert s._held_completion is None
    assert s.state.sale_lifecycle is SaleLifecycle.FILLING
    assert not any(p.get("may_publish_sale") is True for p in published)


def test_micro_fill_completion_released_after_stable_hold() -> None:
    bus = EventBus()
    published: list[dict] = []

    def _cap(event: ControllerEvent) -> None:
        if event.type is ControllerEventType.STATE_CHANGED:
            published.append(dict(event.payload or {}))

    bus.add_subscriber(_cap)
    s, now = _filling_session(filling_age=timedelta(seconds=1))
    s.events = bus
    s._noz_in_edge_at = now
    s.state.nozzle_position = NozzlePosition.IN
    s.machine = PumpStateMachine(
        s.machine.context.with_updates(nozzle_out=False)
    )

    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=ObservationRef(source_frame_raw_hex="short122"),
            raw_wayne_status=5,
            nozzle_out=False,
            completion_evidence_key="complete:short122:5",
        )
    )
    assert s._held_completion is not None
    held_at = s._held_completion.held_at

    # Still inside micro hold → no publish.
    s._tick_completion_hold(
        now=held_at + _NOZIO_STABLE_IN + timedelta(seconds=1)
    )
    assert s._held_completion is not None

    # After relift window with nozzle still IN → finalize.
    s._tick_completion_hold(now=held_at + _MICRO_COMPLETE_RELIFT_HOLD)
    assert s._held_completion is None
    assert s.state.sale_lifecycle is SaleLifecycle.FILLING_COMPLETED
    assert any(
        p.get("event") == PumpEvent.FILLING_COMPLETED.value
        and p.get("may_publish_sale") is True
        for p in published
    )


def test_force_complete_when_sm_rejects_ready() -> None:
    """Regression: SALE console without PersistenceBridge finalize."""
    bus = EventBus()
    published: list[dict] = []

    def _cap(event: ControllerEvent) -> None:
        if event.type is ControllerEventType.STATE_CHANGED:
            published.append(dict(event.payload or {}))

    bus.add_subscriber(_cap)
    s = _session(events=bus)
    now = datetime.now(UTC)
    # READY has no FILLING_COMPLETED edge → SM reject → force publish.
    s.machine = PumpStateMachine(
        PumpContext(
            pump_id="pump-2",
            dart_address=2,
            current_state=PumpState.READY,
            communication_healthy=True,
            nozzle_out=False,
            selected_nozzle=1,
            active_transaction_id="tx-400",
            dispensed_volume_raw=30,
            was_ready_derivable=True,
            last_raw_wayne_status=1,
        )
    )
    s.state.filled_volume_raw = 30
    s.state.filled_amount_raw = 40000
    s.state.unit_price_raw = 1355
    s.state.nozzle_position = NozzlePosition.IN
    # Stable hang-up + long fill → must not enter completion hold.
    s._noz_in_edge_at = now - timedelta(seconds=10)
    s._filling_seen_this_boot = True
    s._filling_started_at = now - timedelta(seconds=30)
    s.state.sale_evidence.note_nozzle_out()
    s.state.sale_evidence.note_authorized(application_confirmed=True)
    s.state.sale_evidence.note_filling()
    s.state.sale_evidence.note_dc2(volume_raw=30, amount_raw=40000)

    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=ObservationRef(source_frame_raw_hex="sale400"),
            raw_wayne_status=5,
            nozzle_out=False,
            completion_evidence_key="complete:sale400:5",
        )
    )
    assert s.machine.context.current_state is PumpState.FILLING_COMPLETE
    assert any(
        p.get("event") == PumpEvent.FILLING_COMPLETED.value
        and p.get("may_publish_sale") is True
        and p.get("filled_amount_raw") == 40000
        for p in published
    )
