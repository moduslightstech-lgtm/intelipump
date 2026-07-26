from __future__ import annotations

import pytest

from intelipump_fdc.protocol.dart.line.constants import DLE, ETX, MAX_BUFFER_SIZE, SF
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.crc import (
    CrcCandidate,
    dart_crc16,
)
from intelipump_fdc.protocol.dart.line.frame_builder import (
    FrameBuildError,
    build_ack,
    build_ackpoll,
    build_data_frame,
    build_eot,
    build_iap,
    build_nak,
    build_poll,
)
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame, ParseError, ParseErrorCode


def _assert_frame(result: DartLineFrame | ParseError) -> DartLineFrame:
    assert isinstance(result, DartLineFrame)
    return result


def _assert_error(result: DartLineFrame | ParseError) -> ParseError:
    assert isinstance(result, ParseError)
    return result


@pytest.mark.parametrize(
    ("builder", "control_type", "sequence"),
    [
        (lambda: build_poll(2), ControlType.POLL, 0),
        (lambda: build_ack(0x51, 0xA), ControlType.ACK, 0xA),
        (lambda: build_nak(0x50, 0x0), ControlType.NAK, 0),
        (lambda: build_eot(0x51, 0x0), ControlType.EOT, 0),
        (lambda: build_ackpoll(0x51, 0x3), ControlType.ACKPOLL, 0x3),
        (lambda: build_iap(0x51), ControlType.IAP, 0),
    ],
)
def test_control_frame_roundtrip(builder, control_type: ControlType, sequence: int) -> None:
    raw = builder()
    assert raw[-1] == SF
    frame = _assert_frame(parse_frame(raw))
    assert frame.control_type is control_type
    assert frame.sequence == sequence
    assert frame.payload == b""
    assert frame.received_crc is None
    assert frame.raw_frame == raw


def test_empty_payload_data_roundtrip() -> None:
    raw = build_data_frame(0x51, 0x0, b"")
    frame = _assert_frame(parse_frame(raw))
    assert frame.control_type is ControlType.DATA
    assert frame.payload == b""
    assert frame.crc_valid is True
    assert frame.received_crc == dart_crc16(b"\x51\x30")


def test_normal_payload_data_roundtrip() -> None:
    payload = b"\x65\x01\x01"
    raw = build_data_frame(0x51, 0x0, payload)
    frame = _assert_frame(parse_frame(raw))
    assert frame.address == 0x51
    assert frame.sequence == 0
    assert frame.payload == payload
    assert frame.crc_valid is True


def test_payload_containing_sf_is_escaped() -> None:
    payload = bytes([0x01, SF, 0x02])
    raw = build_data_frame(0x10, 0x1, payload)
    assert bytes([DLE, SF]) in raw
    assert raw[-1] == SF
    assert raw[-2] != DLE
    frame = _assert_frame(parse_frame(raw))
    assert frame.payload == payload
    assert frame.crc_valid is True


def test_crc_byte_containing_sf_is_escaped() -> None:
    # Force CRC low byte to SF by injecting a custom crc_fn.
    def fake_crc(_data: bytes) -> int:
        return 0x12FA  # CRC1=FA, CRC2=12

    raw = build_data_frame(0x51, 0x2, b"\x01", crc_fn=fake_crc)
    # Escaped CRC1 SF appears as DLE SF before CRC2.
    assert bytes([DLE, SF, 0x12, ETX, SF]) == raw[-5:]
    frame = _assert_frame(parse_frame(raw, crc_fn=fake_crc))
    assert frame.received_crc == 0x12FA
    assert frame.crc_valid is True


def test_malformed_dle_truncated() -> None:
    raw = bytes([0x51, 0x30, DLE, SF])  # DLE before final SF -> invalid terminator
    err = _assert_error(parse_frame(raw))
    assert err.code is ParseErrorCode.INVALID_TERMINATOR


def test_malformed_dle_inside_body() -> None:
    # Unescaped SF in body (no DLE) before terminator.
    raw = bytes([0x51, 0x30, SF, ETX, SF])
    err = _assert_error(parse_frame(raw))
    assert err.code is ParseErrorCode.MALFORMED_DLE


def test_invalid_terminators() -> None:
    err = _assert_error(parse_frame(b"\x51\x20\x00"))
    assert err.code is ParseErrorCode.MISSING_SF
    err = _assert_error(parse_frame(b"\x51\x20"))
    assert err.code is ParseErrorCode.TOO_SHORT


def test_too_short_data_frames() -> None:
    # DATA ctrl but only ADR CTRL SF
    err = _assert_error(parse_frame(bytes([0x51, 0x30, SF])))
    assert err.code is ParseErrorCode.TOO_SHORT_DATA
    # ADR CTRL CRC1 SF  — missing CRC2+ETX after unescape
    err = _assert_error(parse_frame(bytes([0x51, 0x30, 0x00, SF])))
    assert err.code is ParseErrorCode.TOO_SHORT_DATA


def test_missing_etx() -> None:
    # Structurally long enough for DATA, but last buffer byte is not ETX.
    body = bytes([0x51, 0x30, 0x01, 0x02, 0x04])
    raw = body + bytes([SF])
    err = _assert_error(parse_frame(raw))
    assert err.code is ParseErrorCode.MISSING_ETX


def test_invalid_crc_detected() -> None:
    raw = build_data_frame(0x51, 0x0, b"\x01\x02")
    mutated = bytearray(raw)
    # Flip a payload byte before CRC region: find ETX and corrupt earlier.
    etx_at = mutated.index(ETX)
    mutated[2] ^= 0xFF
    frame = _assert_frame(parse_frame(bytes(mutated)))
    assert frame.crc_valid is False
    assert etx_at > 0


def test_maximum_supported_frame_size() -> None:
    # Unescaped buffer max 256: ADR+CTRL+payload+CRC1+CRC2+ETX
    # overhead = 2 + 2 + 1 = 5
    max_payload = MAX_BUFFER_SIZE - 5
    raw = build_data_frame(0x01, 0x0, b"\x00" * max_payload)
    frame = _assert_frame(parse_frame(raw))
    assert len(frame.payload) == max_payload
    with pytest.raises(FrameBuildError):
        build_data_frame(0x01, 0x0, b"\x00" * (max_payload + 1))


def test_unknown_control_byte_preserved() -> None:
    raw = bytes([0x51, 0x00, SF])
    frame = _assert_frame(parse_frame(raw))
    assert frame.control_type is ControlType.UNKNOWN
    assert frame.unknown_control is True
    assert frame.raw_frame == raw


def test_parse_preserves_raw_on_error() -> None:
    raw = b"\x01\x02"
    err = _assert_error(parse_frame(raw))
    assert err.raw == raw


def test_alternate_crc_candidate_roundtrip() -> None:
    raw = build_data_frame(
        0x51,
        0x4,
        b"\x10\x01",
        crc_candidate=CrcCandidate.CCITT_TRUE_INIT_0000,
    )
    frame = _assert_frame(
        parse_frame(raw, crc_candidate=CrcCandidate.CCITT_TRUE_INIT_0000)
    )
    assert frame.crc_valid is True
