"""Startup / reconnect must not publish retained completed-sale faces."""

from __future__ import annotations

from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.controller.sale_lifecycle import SaleLifecycle
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext
from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation


def _bus_events() -> tuple[EventBus, list[ControllerEvent]]:
    bus = EventBus()
    captured: list[ControllerEvent] = []

    def _cap(event: ControllerEvent) -> None:
        captured.append(event)

    bus.add_subscriber(_cap)
    return bus, captured


def _completed_obs(*, hex_ref: str = "ffretained") -> MappedWayneObservation:
    return MappedWayneObservation(
        event=PumpEvent.FILLING_COMPLETED,
        observation=ObservationRef(source_frame_raw_hex=hex_ref),
        raw_wayne_status=int(WaynePumpStatus.FILLING_COMPLETED),
        selected_nozzle=1,
        completion_evidence_key=f"complete:{hex_ref}:5",
    )


def test_restart_retained_completed_publishes_startup_baseline_not_sale() -> None:
    bus, events = _bus_events()
    s = PumpSession(address=1, pump_id="pump-1", events=bus)
    s.state.filled_volume_raw = 17
    s.state.filled_amount_raw = 20000
    s.seed_recovered_context(
        PumpContext(
            pump_id="pump-1",
            dart_address=1,
            current_state=PumpState.DISCOVERING,
            communication_healthy=False,
            selected_nozzle=1,
            dispensed_volume_raw=17,
            state_version=1,
        )
    )
    s._reconcile_then_apply(_completed_obs(), raw_volume=17)

    assert s.machine.context.current_state is PumpState.FILLING_COMPLETE
    baseline = [
        e
        for e in events
        if e.type is ControllerEventType.STATE_CHANGED
        and (e.payload or {}).get("event") == "STARTUP_BASELINE_OBSERVED"
    ]
    assert len(baseline) == 1
    assert baseline[0].payload.get("may_publish_sale") is False
    assert baseline[0].payload.get("startup_baseline") is True
    assert s._filling_seen_this_boot is False
    assert s.state.sale_lifecycle is SaleLifecycle.IDLE


def test_repeated_completed_after_baseline_does_not_publish_sale() -> None:
    bus, events = _bus_events()
    s = PumpSession(address=1, pump_id="pump-1", events=bus)
    s.state.filled_volume_raw = 17
    s.state.filled_amount_raw = 20000
    s.seed_recovered_context(
        PumpContext(
            pump_id="pump-1",
            dart_address=1,
            current_state=PumpState.DISCOVERING,
            communication_healthy=False,
            selected_nozzle=1,
            dispensed_volume_raw=17,
            state_version=1,
        )
    )
    s._reconcile_then_apply(_completed_obs(hex_ref="ff1"), raw_volume=17)
    before = len(events)
    s._apply_mapped(_completed_obs(hex_ref="ff2"))
    new_events = events[before:]
    publishable = [
        e
        for e in new_events
        if (e.payload or {}).get("may_publish_sale") is True
        and (e.payload or {}).get("completion_evidence_key")
    ]
    assert publishable == []
    assert s._filling_seen_this_boot is False


def test_cold_start_completed_without_filling_baselines() -> None:
    bus, events = _bus_events()
    s = PumpSession(address=1, pump_id="pump-1", events=bus)
    s.state.filled_volume_raw = 17
    s.state.filled_amount_raw = 20000
    s.machine = PumpStateMachine(
        s.machine.context.with_updates(dispensed_volume_raw=17)
    )
    s._apply_mapped(_completed_obs(hex_ref="ffcold"))
    baseline = [
        e
        for e in events
        if (e.payload or {}).get("event") == "STARTUP_BASELINE_OBSERVED"
    ]
    assert len(baseline) == 1
    assert s._filling_seen_this_boot is False


def test_real_filling_then_complete_may_publish() -> None:
    bus, events = _bus_events()
    s = PumpSession(address=1, pump_id="pump-1", events=bus)
    s.state.filled_volume_raw = 17
    s.state.filled_amount_raw = 20000
    s.machine = PumpStateMachine(
        PumpContext(
            pump_id="pump-1",
            dart_address=1,
            current_state=PumpState.AUTHORIZED,
            communication_healthy=True,
            selected_nozzle=1,
            state_version=1,
        )
    )
    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.FILLING_STARTED,
            observation=ObservationRef(source_frame_raw_hex="fffill"),
            raw_wayne_status=int(WaynePumpStatus.FILLING),
            selected_nozzle=1,
        )
    )
    assert s._filling_seen_this_boot is True
    assert s.machine.context.current_state is PumpState.FILLING
    s.machine = PumpStateMachine(
        s.machine.context.with_updates(dispensed_volume_raw=17)
    )
    s._apply_mapped(_completed_obs(hex_ref="ffdone"))
    finalized = [
        e
        for e in events
        if (e.payload or {}).get("may_publish_sale") is True
        and (e.payload or {}).get("completion_evidence_key")
    ]
    assert finalized
    assert s.state.sale_lifecycle is SaleLifecycle.FILLING_COMPLETED


def test_nozzle_isolation_filling_flag_is_per_session() -> None:
    s1 = PumpSession(address=1, pump_id="pump-1", events=EventBus())
    s2 = PumpSession(address=2, pump_id="pump-1", events=EventBus())
    s1._filling_seen_this_boot = True
    assert s2._filling_seen_this_boot is False
    s2.state.sale_evidence.note_filling()
    assert s2.state.sale_evidence.filling_observed is True
    s1.state.sale_evidence.reset_attempt()
    assert s1.state.sale_evidence.filling_observed is False
    assert s2.state.sale_evidence.filling_observed is True
