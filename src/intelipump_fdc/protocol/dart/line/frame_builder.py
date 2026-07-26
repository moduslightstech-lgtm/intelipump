"""Build DART line frames (bytes only; no serial I/O).

Source: DART Serial Communication / Line-Level Specification, page 3.

Master/slave control frames: ADR + CTRL + SF
DATA frames: ADR + CTRL + DATA... + CRC-1 + CRC-2 + ETX + SF
DLE escaping applied to the buffer contents before the trailing SF.
"""

from __future__ import annotations

from intelipump_fdc.protocol.dart.line.addressing import (
    AddressMappingError,
    encode_wire_address,
)
from intelipump_fdc.protocol.dart.line.constants import ETX, MAX_BUFFER_SIZE, SF
from intelipump_fdc.protocol.dart.line.control import ControlType, compose_control
from intelipump_fdc.protocol.dart.line.crc import (
    DEFAULT_CRC_CANDIDATE,
    CrcCandidate,
    CrcFn,
    crc_bytes_le,
    get_crc_fn,
)
from intelipump_fdc.protocol.dart.line.escaping import escape_dle


class FrameBuildError(ValueError):
    """Invalid arguments or size limits while building a frame."""


def _validate_wire_address(address: int) -> None:
    if not 0 <= address <= 0xFF:
        raise FrameBuildError(f"wire address out of range 0x00-0xFF: {address}")


def build_control_frame(
    wire_address: int,
    control_type: ControlType,
    sequence: int = 0,
) -> bytes:
    """Build POLL / ACK / NAK / EOT / IAP / ACKPOLL control frame.

    ``wire_address`` is the on-wire ADR byte (e.g. 0x50), not a logical side.
    Prefer :func:`build_poll` for status polls (logical → wire mapping).
    """
    if control_type is ControlType.DATA:
        raise FrameBuildError("use build_data_frame for DATA")
    if control_type is ControlType.UNKNOWN:
        raise FrameBuildError("cannot build UNKNOWN control type")
    _validate_wire_address(wire_address)
    control = compose_control(control_type, sequence)
    buffer = bytes((wire_address, control))
    if len(buffer) > MAX_BUFFER_SIZE:
        raise FrameBuildError("control buffer exceeds MAX_BUFFER_SIZE")
    return escape_dle(buffer) + bytes((SF,))


def build_poll(logical_address: int) -> bytes:
    """Build a captured-profile status POLL from a logical side (1 or 2).

    Evidence: ``build_poll(1) == 50 20 FA``, ``build_poll(2) == 51 20 FA``.
    Rejects raw wire addresses passed as logical (e.g. 0x50). Exactly three
    bytes; no extra CRC. No raw-hex override.
    """
    try:
        wire = encode_wire_address(logical_address)
    except AddressMappingError as exc:
        raise FrameBuildError(str(exc)) from exc
    frame = build_control_frame(wire, ControlType.POLL, sequence=0)
    if len(frame) != 3:
        raise FrameBuildError(f"status poll must be exactly 3 bytes, got {len(frame)}")
    return frame


def build_ack(address: int, sequence: int) -> bytes:
    return build_control_frame(address, ControlType.ACK, sequence=sequence)


def build_nak(address: int, sequence: int) -> bytes:
    return build_control_frame(address, ControlType.NAK, sequence=sequence)


def build_eot(address: int, sequence: int = 0) -> bytes:
    return build_control_frame(address, ControlType.EOT, sequence=sequence)


def build_ackpoll(address: int, sequence: int) -> bytes:
    return build_control_frame(address, ControlType.ACKPOLL, sequence=sequence)


def build_iap(address: int) -> bytes:
    return build_control_frame(address, ControlType.IAP, sequence=0)


def build_data_frame(
    wire_address: int,
    sequence: int,
    payload: bytes,
    *,
    crc_fn: CrcFn | None = None,
    crc_candidate: CrcCandidate = DEFAULT_CRC_CANDIDATE,
) -> bytes:
    """Build a DATA frame.

    ``wire_address`` is the on-wire ADR byte. CRC is computed with the selected
    candidate over ADR||CTRL||payload (unescaped). Default is the Phase-2
    capture-proven canonical algorithm.
    """
    _validate_wire_address(wire_address)
    if not 0 <= sequence <= 0x0F:
        raise FrameBuildError(f"sequence out of range 0x0-0xF: {sequence}")
    control = compose_control(ControlType.DATA, sequence)
    fn = crc_fn if crc_fn is not None else get_crc_fn(crc_candidate)
    crc_input = bytes((wire_address, control)) + payload
    crc = fn(crc_input)
    # Unescaped buffer: ADR CTRL DATA... CRC1 CRC2 ETX
    buffer = crc_input + crc_bytes_le(crc) + bytes((ETX,))
    if len(buffer) > MAX_BUFFER_SIZE:
        raise FrameBuildError(
            f"DATA buffer length {len(buffer)} exceeds MAX_BUFFER_SIZE {MAX_BUFFER_SIZE}"
        )
    return escape_dle(buffer) + bytes((SF,))
