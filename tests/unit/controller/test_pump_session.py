"""Per-pump session protocol handling tests."""

from __future__ import annotations

from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.constants import SF
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.escaping import escape_dle, unescape_dle
from intelipump_fdc.protocol.dart.line.frame_builder import (
    build_data_frame,
    build_eot,
    build_nak,
)
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.simulator.encoding import encode_dc1_status


def _session() -> PumpSession:
    return PumpSession(address=1, pump_id="p1", events=EventBus())


def _parse(raw: bytes) -> DartLineFrame:
    parsed = parse_frame(raw)
    assert isinstance(parsed, DartLineFrame)
    return parsed


def test_poll_eot_marks_healthy() -> None:
    s = _session()
    s.build_poll()
    s.handle_response_frame(_parse(build_eot(encode_wire_address(1), 0)))
    assert s.state.stats.eot_count == 1
    assert s.machine.context.current_state is not PumpState.DISCONNECTED


def test_poll_data_ack_and_state() -> None:
    s = _session()
    payload = encode_dc1_status(1)  # RESET
    frame = _parse(build_data_frame(encode_wire_address(1), 0, payload))
    ack = s.handle_response_frame(frame)
    assert ack is not None
    assert s.state.stats.data_count == 1
    assert s.state.stats.ack_sent_count == 1
    assert s.state.expected_rx_sequence == 1


def test_crc_error_does_not_mutate_state() -> None:
    s = _session()
    before = s.machine.context.state_version
    good = build_data_frame(encode_wire_address(1), 0, encode_dc1_status(1))
    body = bytearray(unescape_dle(good[:-1]))
    body[-3] ^= 0xFF
    bad = escape_dle(bytes(body)) + bytes((SF,))
    frame = _parse(bad)
    assert frame.crc_valid is False
    assert s.handle_response_frame(frame) is None
    assert s.state.stats.crc_error_count == 1
    assert s.machine.context.state_version == before


def test_duplicate_data_not_applied_twice() -> None:
    s = _session()
    payload = encode_dc1_status(1)
    raw = build_data_frame(encode_wire_address(1), 0, payload)
    frame = _parse(raw)
    s.handle_response_frame(frame)
    version = s.machine.context.state_version
    ack2 = s.handle_response_frame(frame)
    assert ack2 is not None
    assert s.state.stats.duplicate_count == 1
    # State version should not jump from a duplicate apply of RESET.
    assert s.machine.context.state_version == version


def test_sequence_mismatch() -> None:
    s = _session()
    frame = _parse(build_data_frame(encode_wire_address(1), 5, encode_dc1_status(1)))
    assert s.handle_response_frame(frame) is None
    assert s.state.stats.sequence_error_count == 1


def test_nak_handling() -> None:
    s = _session()
    s.handle_response_frame(_parse(build_nak(encode_wire_address(1), 1)))
    assert s.state.stats.nak_count == 1
    parsed = _parse(build_nak(encode_wire_address(1), 1))
    assert parsed.control_type is ControlType.NAK


def test_timeout_then_eot_clears_transient_last_error() -> None:
    from intelipump_fdc.controller.session_models import CommunicationHealth

    s = _session()
    s.on_timeout()
    s.on_timeout()
    s.on_timeout()
    assert s.state.last_error == "response_timeout"
    assert s.state.stats.timeout_count == 3
    assert s.state.communication is CommunicationHealth.DEGRADED

    s.handle_response_frame(_parse(build_eot(encode_wire_address(1), 0)))
    assert s.state.communication is CommunicationHealth.HEALTHY
    assert s.state.last_error is None
    assert s.state.stats.timeout_count == 3  # historical count retained
    assert s.state.consecutive_timeouts == 0


def test_timeout_then_data_clears_transient_last_error() -> None:
    from intelipump_fdc.controller.session_models import CommunicationHealth

    s = _session()
    s.on_timeout()
    assert s.state.last_error == "response_timeout"
    assert s.state.stats.timeout_count == 1

    frame = _parse(build_data_frame(encode_wire_address(1), 0, encode_dc1_status(1)))
    ack = s.handle_response_frame(frame)
    assert ack is not None
    assert s.state.communication is CommunicationHealth.HEALTHY
    assert s.state.last_error is None
    assert s.state.stats.timeout_count == 1
    assert s.state.stats.data_count == 1


def test_successful_eot_does_not_clear_persistent_protocol_fault() -> None:
    from intelipump_fdc.controller.session_models import CommunicationHealth

    s = _session()
    s.state.last_error = "invalid_crc"
    s.state.last_persistent_fault = "invalid_crc"
    s.handle_response_frame(_parse(build_eot(encode_wire_address(1), 0)))
    assert s.state.communication is CommunicationHealth.HEALTHY
    assert s.state.last_persistent_fault == "invalid_crc"
