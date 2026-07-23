"""Integration: Phase-3 decode → Wayne mapper → state machine."""

from __future__ import annotations

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import PumpContext
from intelipump_fdc.state_machine.wayne_mapper import MapperContext, map_wayne_observation


def test_ambiguous_status_does_not_change_machine_state() -> None:
    machine = PumpStateMachine(
        PumpContext(
            pump_id="p1",
            dart_address=1,
            current_state=PumpState.READY,
            communication_healthy=True,
            state_version=3,
        )
    )
    tx = decode_data_payload(
        bytes.fromhex("01 01 04"),
        source_frame_raw_hex="FRAME-AMB",
    ).transactions[0]
    mapped = map_wayne_observation(tx)
    assert mapped.event is PumpEvent.UNKNOWN_OBSERVATION
    result = machine.apply_mapped(mapped)
    assert result.accepted is True
    assert result.noop is True
    assert result.current_state is PumpState.READY
    assert machine.context.state_version in {3, 4}  # may bump on raw status


def test_resolved_dc1_filling_drives_transition() -> None:
    machine = PumpStateMachine(
        PumpContext(
            pump_id="p1",
            dart_address=1,
            current_state=PumpState.AUTHORIZED,
            communication_healthy=True,
        )
    )
    tx = decode_data_payload(
        bytes.fromhex("01 01 04"),
        source_frame_raw_hex="FRAME-FILL",
    ).transactions[0]
    mapped = map_wayne_observation(
        tx,
        context=MapperContext(resolve_as_dc1=True, current_state=PumpState.AUTHORIZED),
    )
    result = machine.apply_mapped(mapped)
    assert mapped.event is PumpEvent.FILLING_STARTED
    assert result.accepted is True
    assert result.current_state is PumpState.FILLING


def test_partial_dc3_does_not_force_nozzle_up() -> None:
    machine = PumpStateMachine(
        PumpContext(
            pump_id="p1",
            dart_address=1,
            current_state=PumpState.READY,
            communication_healthy=True,
        )
    )
    tx = decode_data_payload(
        bytes.fromhex("03 04 00 11 75 11"),
        source_frame_raw_hex="FRAME-DC3",
    ).transactions[0]
    mapped = map_wayne_observation(tx)
    result = machine.apply_mapped(mapped)
    assert mapped.event is PumpEvent.UNKNOWN_OBSERVATION
    assert result.current_state is PumpState.READY


def test_resolved_dc3_then_authorize_path() -> None:
    machine = PumpStateMachine(
        PumpContext(
            pump_id="p1",
            dart_address=1,
            current_state=PumpState.READY,
            communication_healthy=True,
            price_verified=True,
        )
    )
    dc3 = decode_data_payload(bytes.fromhex("03 04 00 11 75 11")).transactions[0]
    mapped_lift = map_wayne_observation(
        dc3,
        context=MapperContext(resolve_as_dc3=True, current_state=PumpState.READY),
    )
    r1 = machine.apply_mapped(mapped_lift)
    assert r1.current_state is PumpState.NOZZLE_UP
    assert machine.context.selected_nozzle == 1

    r2 = machine.apply(PumpEvent.AUTHORIZATION_CONFIRMED, raw_wayne_status=2)
    assert r2.current_state is PumpState.AUTHORIZED
