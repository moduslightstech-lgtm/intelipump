"""Line-level simulator session tests."""

from __future__ import annotations

from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import (
    build_ack,
    build_data_frame,
    build_poll,
)
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.simulator.config import SequencePolicy, SimulatorConfig, next_sequence
from intelipump_fdc.simulator.encoding import encode_cd1_command
from intelipump_fdc.simulator.faults import ProtocolFaultKind
from intelipump_fdc.simulator.session import SimulatorSession


def test_poll_eot_when_idle() -> None:
    session = SimulatorSession(SimulatorConfig(pumps=SimulatorConfig().pumps[:1]))
    pump = session.get_pump(1)
    pump.enable_communication()
    result = session.receive(build_poll(1))
    parsed = parse_frame(result.responses[0])
    assert isinstance(parsed, DartLineFrame)
    assert parsed.control_type is ControlType.EOT


def test_poll_data_then_ack_advances_tx() -> None:
    session = SimulatorSession(SimulatorConfig(pumps=SimulatorConfig().pumps[:1]))
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    assert pump.has_pending()
    result = session.receive(build_poll(1))
    parsed = parse_frame(result.responses[0])
    assert isinstance(parsed, DartLineFrame)
    assert parsed.control_type is ControlType.DATA
    assert pump.awaiting_ack_sequence == 0
    session.receive(build_ack(1, 0))
    assert pump.tx_sequence == 1
    assert pump.awaiting_ack_sequence is None


def test_invalid_crc_records_fault_no_ack() -> None:
    from intelipump_fdc.protocol.dart.line.constants import SF
    from intelipump_fdc.protocol.dart.line.escaping import escape_dle, unescape_dle

    session = SimulatorSession(SimulatorConfig(pumps=SimulatorConfig().pumps[:1]))
    pump = session.get_pump(1)
    pump.enable_communication()
    good = build_data_frame(1, 0, encode_cd1_command(PumpControlCommand.RETURN_STATUS))
    body = bytearray(unescape_dle(good[:-1]))
    body[-3] ^= 0xFF  # corrupt CRC low byte
    bad = escape_dle(bytes(body)) + bytes((SF,))
    result = session.receive(bad)
    assert result.responses == ()
    assert any(f.kind is ProtocolFaultKind.INVALID_CRC for f in result.faults)


def test_unexpected_sequence_returns_nak() -> None:
    session = SimulatorSession(SimulatorConfig(pumps=SimulatorConfig().pumps[:1]))
    pump = session.get_pump(1)
    pump.enable_communication()
    wire = build_data_frame(1, 5, encode_cd1_command(PumpControlCommand.RETURN_STATUS))
    result = session.receive(wire)
    parsed = parse_frame(result.responses[0])
    assert isinstance(parsed, DartLineFrame)
    assert parsed.control_type is ControlType.NAK
    assert any(f.kind is ProtocolFaultKind.UNEXPECTED_SEQUENCE for f in result.faults)


def test_duplicate_data_acked_without_reprocess() -> None:
    session = SimulatorSession(SimulatorConfig(pumps=SimulatorConfig().pumps[:1]))
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    payload = encode_cd1_command(PumpControlCommand.RETURN_STATUS)
    wire = build_data_frame(1, 0, payload)
    session.receive(wire)
    pending_before = len(pump.pending_outbound)
    result = session.receive(wire)
    parsed = parse_frame(result.responses[0])
    assert isinstance(parsed, DartLineFrame)
    assert parsed.control_type is ControlType.ACK
    assert any(f.kind is ProtocolFaultKind.DUPLICATE_DATA for f in result.faults)
    assert len(pump.pending_outbound) == pending_before


def test_sequence_policies() -> None:
    assert next_sequence(0x0E, SequencePolicy.SPEC_F_TO_1) == 0x0F
    assert next_sequence(0x0F, SequencePolicy.SPEC_F_TO_1) == 0x01
    assert next_sequence(0x0F, SequencePolicy.OBSERVED_F_TO_0) == 0x00


def test_response_timeout_fault() -> None:
    session = SimulatorSession(
        SimulatorConfig(response_timeout_ms=25, pumps=SimulatorConfig().pumps[:1])
    )
    pump = session.get_pump(1)
    pump.cold_start_to_ready()
    session.receive(build_poll(1))
    assert pump.awaiting_ack_sequence is not None
    session.advance(25)
    assert any(f.kind is ProtocolFaultKind.RESPONSE_TIMEOUT for f in session.faults)


def test_session_has_no_tty_paths() -> None:
    from pathlib import Path

    import intelipump_fdc.simulator.session as session_mod

    source = Path(session_mod.__file__).read_text(encoding="utf-8")
    assert "/dev/tty" not in source
    assert "/tmp/dart-" not in source
    assert "pyserial" not in source
    assert "serial.Serial" not in source
