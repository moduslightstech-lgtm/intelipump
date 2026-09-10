"""Startup with retained FILLING_COMPLETE must not publish a new sale."""

from __future__ import annotations

from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext
from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation


def test_retained_completed_face_publishes_startup_baseline_only() -> None:
    bus = EventBus()
    events: list[ControllerEvent] = []
    bus.add_subscriber(events.append)

    s = PumpSession(address=1, pump_id="pump-1", events=bus)
    # Cold boot: no unresolved sale; pump still shows last completed face.
    s.seed_recovered_context(
        PumpContext(
            pump_id="pump-1",
            dart_address=1,
            current_state=PumpState.READY,
            active_transaction_id=None,
            has_unresolved_transaction=False,
            communication_healthy=False,
            selected_nozzle=1,
            dispensed_volume_raw=170,
            state_version=1,
        )
    )
    s.state.filled_volume_raw = 170
    s.state.filled_amount_raw = 20000

    s._reconcile_then_apply(
        MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=ObservationRef(source_frame_raw_hex="ff99"),
            raw_wayne_status=int(WaynePumpStatus.FILLING_COMPLETED),
            selected_nozzle=1,
            filling_price_raw=117500,
            completion_evidence_key="complete:ff99:5",
        ),
        raw_volume=170,
    )

    baselines = [
        e
        for e in events
        if e.type is ControllerEventType.STATE_CHANGED
        and (e.payload or {}).get("event") == "STARTUP_BASELINE_OBSERVED"
    ]
    assert len(baselines) == 1
    payload = baselines[0].payload or {}
    assert payload.get("may_publish_sale") is False
    assert payload.get("startup_baseline") is True
    assert payload.get("filled_amount_raw") == 20000
    # Must not emit a completion evidence key that would finalize a new sale.
    assert payload.get("completion_evidence_key") is None
    assert s.machine.context.current_state is PumpState.FILLING_COMPLETE


def test_repeated_completed_frames_do_not_rearm_baseline_sale() -> None:
    bus = EventBus()
    events: list[ControllerEvent] = []
    bus.add_subscriber(events.append)
    s = PumpSession(address=1, pump_id="pump-1", events=bus)
    s.seed_recovered_context(
        PumpContext(
            pump_id="pump-1",
            dart_address=1,
            current_state=PumpState.READY,
            has_unresolved_transaction=False,
            communication_healthy=False,
            selected_nozzle=1,
            dispensed_volume_raw=170,
            state_version=1,
        )
    )
    s.state.filled_volume_raw = 170
    s.state.filled_amount_raw = 20000
    mapped = MappedWayneObservation(
        event=PumpEvent.FILLING_COMPLETED,
        observation=ObservationRef(source_frame_raw_hex="ff99"),
        raw_wayne_status=int(WaynePumpStatus.FILLING_COMPLETED),
        selected_nozzle=1,
        completion_evidence_key="complete:ff99:5",
    )
    s._reconcile_then_apply(mapped, raw_volume=170)
    # After sync, repeated COMPLETED frames must not invent filling_observed.
    before = len(events)
    s._apply_mapped(mapped)
    s._apply_mapped(mapped)
    sold = [
        e
        for e in events[before:]
        if (e.payload or {}).get("completion_evidence_key")
        and (e.payload or {}).get("event") != "STARTUP_BASELINE_OBSERVED"
    ]
    assert sold == []
