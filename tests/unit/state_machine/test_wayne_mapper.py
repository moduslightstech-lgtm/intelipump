"""Unit tests for Wayne observation → normalized event mapping."""

from __future__ import annotations

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.constants import (
    DecodeStatus,
    MessageDirection,
    TransactionType,
)
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.application.models import ApplicationTransaction
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.state_machine.wayne_mapper import (
    MapperContext,
    map_wayne_observation,
    map_wayne_status_code,
)


def test_ambiguous_cd1_dc1_does_not_force_state() -> None:
    tx = decode_data_payload(bytes.fromhex("01 01 05")).transactions[0]
    mapped = map_wayne_observation(tx)
    assert mapped.event is PumpEvent.UNKNOWN_OBSERVATION
    assert mapped.raw_wayne_status == 5
    assert any("Ambiguous CD1/DC1" in w for w in mapped.warnings)


def test_ambiguous_cd1_resolved_as_dc1_maps_status() -> None:
    tx = decode_data_payload(bytes.fromhex("01 01 04")).transactions[0]
    mapped = map_wayne_observation(
        tx,
        context=MapperContext(resolve_as_dc1=True),
    )
    assert mapped.event is PumpEvent.FILLING_STARTED
    assert mapped.raw_wayne_status == WaynePumpStatus.FILLING


def test_ambiguous_cd3_dc3_does_not_force_state() -> None:
    tx = decode_data_payload(bytes.fromhex("03 04 00 11 75 11")).transactions[0]
    assert tx.transaction_type is TransactionType.DC3_NOZZLE_STATUS_PRICE
    assert tx.decode_status is DecodeStatus.PARTIAL
    mapped = map_wayne_observation(tx)
    assert mapped.event is PumpEvent.UNKNOWN_OBSERVATION
    assert any("CD3/DC3" in w for w in mapped.warnings)


def test_resolved_dc3_nozzle_out_maps_to_nozzle_lifted() -> None:
    tx = decode_data_payload(bytes.fromhex("03 04 00 11 75 11")).transactions[0]
    mapped = map_wayne_observation(
        tx,
        context=MapperContext(resolve_as_dc3=True),
    )
    assert mapped.event is PumpEvent.NOZZLE_LIFTED
    assert mapped.selected_nozzle == 1


def test_resolved_dc3_nozzle_in_maps_to_ready_observed() -> None:
    tx = decode_data_payload(bytes.fromhex("03 04 00 11 75 01")).transactions[0]
    mapped = map_wayne_observation(
        tx,
        context=MapperContext(resolve_as_dc3=True, current_state=PumpState.RESET),
    )
    assert mapped.event is PumpEvent.READY_OBSERVED


def test_dc2_maps_to_filling_updated() -> None:
    tx = decode_data_payload(bytes.fromhex("02 08 00 00 00 00 00 00 00 00")).transactions[0]
    mapped = map_wayne_observation(tx)
    assert mapped.event is PumpEvent.FILLING_UPDATED


def test_switched_off_is_unknown() -> None:
    mapped = map_wayne_status_code(WaynePumpStatus.SWITCHED_OFF)
    assert mapped.event is PumpEvent.UNKNOWN_OBSERVATION


def test_status_mapping_table() -> None:
    cases = {
        WaynePumpStatus.PUMP_NOT_PROGRAMMED: PumpEvent.CONFIGURATION_MISSING,
        WaynePumpStatus.RESET: PumpEvent.RESET_OBSERVED,
        WaynePumpStatus.AUTHORIZED: PumpEvent.AUTHORIZATION_CONFIRMED,
        WaynePumpStatus.FILLING: PumpEvent.FILLING_STARTED,
        WaynePumpStatus.FILLING_COMPLETED: PumpEvent.FILLING_COMPLETED,
        WaynePumpStatus.MAX_AMOUNT_VOLUME_REACHED: PumpEvent.LIMIT_REACHED,
        WaynePumpStatus.SUSPENDED: PumpEvent.SUSPENDED_OBSERVED,
    }
    for status, event in cases.items():
        mapped = map_wayne_status_code(int(status))
        assert mapped.event is event


def test_resume_inference_from_suspended_to_filling() -> None:
    mapped = map_wayne_status_code(
        WaynePumpStatus.FILLING,
        previous_wayne_status=WaynePumpStatus.SUSPENDED,
    )
    assert mapped.event is PumpEvent.RESUMED_OBSERVED


def test_cd5_does_not_force_lifecycle_state() -> None:
    tx = decode_data_payload(bytes.fromhex("05 03 00 11 75")).transactions[0]
    mapped = map_wayne_observation(tx)
    assert mapped.event is PumpEvent.UNKNOWN_OBSERVATION


def test_outstanding_request_resolves_dc1() -> None:
    tx = decode_data_payload(bytes.fromhex("01 01 01")).transactions[0]
    mapped = map_wayne_observation(
        tx,
        context=MapperContext(outstanding_request_was_cd1_status=True),
    )
    assert mapped.event is PumpEvent.RESET_OBSERVED


def test_transaction_id_collision_not_used_as_direction_proof() -> None:
    """Same TRANS value alone must not resolve CD1/DC1."""
    tx = ApplicationTransaction(
        transaction_id=0x01,
        transaction_type=TransactionType.AMBIGUOUS_CD1_OR_DC1,
        length=1,
        raw_payload=b"\x04",
        raw_transaction=bytes.fromhex("01 01 04"),
        decoded_body={
            "raw_code": 4,
            "dc1_pump_status": {"known": True, "name": "FILLING"},
            "cd1_command": {"known": True, "name": "RETURN_FILLING_INFORMATION"},
        },
        pump_address=1,
        line_sequence=1,
        direction=MessageDirection.UNKNOWN,
        decode_status=DecodeStatus.PARTIAL,
        source_frame_raw_hex="AA",
    )
    mapped = map_wayne_observation(tx)
    assert mapped.event is PumpEvent.UNKNOWN_OBSERVATION
