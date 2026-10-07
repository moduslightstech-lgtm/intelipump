"""Equal-value sale after restart must not be discarded by baseline suppress."""

from __future__ import annotations

from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.sale_lifecycle import SaleLifecycle
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


def _bus() -> tuple[EventBus, list[ControllerEvent]]:
    bus = EventBus()
    captured: list[ControllerEvent] = []
    bus.add_subscriber(captured.append)
    return bus, captured


def test_equal_value_after_filling_includes_filling_seen_flag() -> None:
    """After a real fill, completion payload carries filling_seen_this_boot."""
    bus, events = _bus()
    s = PumpSession(address=1, pump_id="pump-1", events=bus)
    s.seed_recovered_context(
        PumpContext(
            pump_id="pump-1",
            dart_address=1,
            current_state=PumpState.AUTHORIZED,
            communication_healthy=True,
            selected_nozzle=1,
            dispensed_volume_raw=0,
            state_version=1,
            active_transaction_id="sale-uuid-eq-1",
        )
    )
    # Real filling this boot.
    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.FILLING_STARTED,
            observation=ObservationRef(source_frame_raw_hex="fill"),
            raw_wayne_status=int(WaynePumpStatus.FILLING),
            selected_nozzle=1,
        )
    )
    s.state.filled_volume_raw = 365
    s.state.filled_amount_raw = 500000
    s.state.unit_price_raw = 1370
    s.state.sale_evidence.note_dc2(volume_raw=365, amount_raw=500000)
    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=ObservationRef(source_frame_raw_hex="eq1"),
            raw_wayne_status=int(WaynePumpStatus.FILLING_COMPLETED),
            selected_nozzle=1,
            completion_evidence_key="complete:eq1:5",
        )
    )
    published = [
        e
        for e in events
        if e.type is ControllerEventType.STATE_CHANGED
        and (e.payload or {}).get("may_publish_sale") is True
    ]
    assert published, "equal-value completion after fill must be publishable"
    payload = published[-1].payload or {}
    assert payload.get("filling_seen_this_boot") is True
    assert payload.get("startup_baseline") is not True


def test_retained_display_baseline_does_not_set_filling_seen() -> None:
    bus, events = _bus()
    s = PumpSession(address=1, pump_id="pump-1", events=bus)
    s.state.filled_volume_raw = 365
    s.state.filled_amount_raw = 500000
    s.seed_recovered_context(
        PumpContext(
            pump_id="pump-1",
            dart_address=1,
            current_state=PumpState.DISCOVERING,
            communication_healthy=False,
            selected_nozzle=1,
            dispensed_volume_raw=365,
            state_version=1,
        )
    )
    s._reconcile_then_apply(
        MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=ObservationRef(source_frame_raw_hex="ret"),
            raw_wayne_status=int(WaynePumpStatus.FILLING_COMPLETED),
            selected_nozzle=1,
            completion_evidence_key="complete:ret:5",
        ),
        raw_volume=365,
    )
    baseline = [
        e
        for e in events
        if (e.payload or {}).get("event") == "STARTUP_BASELINE_OBSERVED"
    ]
    assert len(baseline) == 1
    assert baseline[0].payload.get("may_publish_sale") is False
    assert baseline[0].payload.get("startup_baseline") is True
    assert s.state.sale_lifecycle is SaleLifecycle.IDLE
