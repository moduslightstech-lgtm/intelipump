"""Parse complete DART line frames (no stream reassembly / no serial I/O).

Source: DART Serial Communication / Line-Level Specification, pages 2-3, 4.

This parser accepts one complete wire frame ending in an unescaped SF.
It does not implement the interrupt-driven stream assembler.
"""

from __future__ import annotations

from intelipump_fdc.protocol.dart.line.constants import (
    ETX,
    MAX_BUFFER_SIZE,
    MIN_DATA_FRAME_SIZE,
    SF,
)
from intelipump_fdc.protocol.dart.line.control import (
    ControlType,
    classify_control,
    extract_sequence,
)
from intelipump_fdc.protocol.dart.line.crc import (
    DEFAULT_CRC_CANDIDATE,
    CrcCandidate,
    CrcFn,
    crc_from_le_bytes,
    get_crc_fn,
)
from intelipump_fdc.protocol.dart.line.escaping import DleEscapeError, unescape_dle
from intelipump_fdc.protocol.dart.line.models import DartLineFrame, ParseError, ParseErrorCode


def parse_frame(
    raw: bytes,
    *,
    crc_fn: CrcFn | None = None,
    crc_candidate: CrcCandidate = DEFAULT_CRC_CANDIDATE,
) -> DartLineFrame | ParseError:
    """Parse a single complete frame (must end with unescaped SF)."""
    if len(raw) < 3:
        return ParseError(ParseErrorCode.TOO_SHORT, "frame shorter than ADR+CTRL+SF", raw)
    if raw[-1] != SF:
        return ParseError(
            ParseErrorCode.MISSING_SF,
            "frame must end with SF (0xFA)",
            raw,
        )
    if len(raw) >= 2 and raw[-2] == 0x10:
        # DLE immediately before final SF means SF was escaped, not a terminator.
        return ParseError(
            ParseErrorCode.INVALID_TERMINATOR,
            "final SF is DLE-escaped; not a frame terminator",
            raw,
        )

    wire_body = raw[:-1]
    try:
        buffer = unescape_dle(wire_body)
    except DleEscapeError as exc:
        return ParseError(ParseErrorCode.MALFORMED_DLE, str(exc), raw)

    if len(buffer) > MAX_BUFFER_SIZE:
        return ParseError(
            ParseErrorCode.BUFFER_TOO_LARGE,
            f"unescaped buffer length {len(buffer)} exceeds MAX_BUFFER_SIZE",
            raw,
        )
    if len(buffer) < 2:
        return ParseError(ParseErrorCode.TOO_SHORT, "buffer shorter than ADR+CTRL", raw)

    address = buffer[0]
    control = buffer[1]
    control_type = classify_control(control)
    sequence = extract_sequence(control)
    unknown = control_type is ControlType.UNKNOWN

    # Control frames: ADR + CTRL only in buffer.
    if len(buffer) == 2:
        if control_type is ControlType.DATA:
            return ParseError(
                ParseErrorCode.TOO_SHORT_DATA,
                "DATA control byte with no CRC/ETX",
                raw,
            )
        return DartLineFrame(
            address=address,
            control=control,
            control_type=control_type,
            sequence=sequence,
            payload=b"",
            received_crc=None,
            computed_crc=None,
            crc_valid=None,
            raw_frame=bytes(raw),
            unknown_control=unknown,
        )

    # Longer buffers are parsed as DATA structure (ADR CTRL payload CRC CRC ETX).
    if len(buffer) < MIN_DATA_FRAME_SIZE:
        return ParseError(
            ParseErrorCode.TOO_SHORT_DATA,
            "DATA frame shorter than ADR+CTRL+CRC1+CRC2+ETX",
            raw,
        )
    if buffer[-1] != ETX:
        return ParseError(
            ParseErrorCode.MISSING_ETX,
            "DATA frame must end with ETX before SF",
            raw,
        )

    crc1 = buffer[-3]
    crc2 = buffer[-2]
    payload = buffer[2:-3]
    received_crc = crc_from_le_bytes(crc1, crc2)
    fn = crc_fn if crc_fn is not None else get_crc_fn(crc_candidate)
    computed_crc = fn(bytes((address, control)) + payload)
    crc_valid = computed_crc == received_crc

    return DartLineFrame(
        address=address,
        control=control,
        control_type=control_type,
        sequence=sequence,
        payload=bytes(payload),
        received_crc=received_crc,
        computed_crc=computed_crc,
        crc_valid=crc_valid,
        raw_frame=bytes(raw),
        unknown_control=unknown,
    )
