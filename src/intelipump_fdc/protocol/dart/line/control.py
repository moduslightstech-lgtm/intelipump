"""DART control-byte classification and sequence extraction.

Source: DART Serial Communication / Line-Level Specification, page 3.
"""

from __future__ import annotations

from enum import StrEnum

from intelipump_fdc.protocol.dart.line.constants import (
    ACK_BASE,
    ACKPOLL_BASE,
    CONTROL_TYPE_MASK,
    DATA_BASE,
    EOT_BASE,
    IAP,
    NAK_BASE,
    POLL,
    SEQUENCE_MASK,
)


class ControlType(StrEnum):
    POLL = "POLL"
    DATA = "DATA"
    IAP = "IAP"
    NAK = "NAK"
    EOT = "EOT"
    ACK = "ACK"
    ACKPOLL = "ACKPOLL"
    UNKNOWN = "UNKNOWN"


def control_type_base(control_type: ControlType) -> int:
    """Return the high-nibble base for a known sequenced/fixed control type."""
    mapping: dict[ControlType, int] = {
        ControlType.POLL: POLL,
        ControlType.DATA: DATA_BASE,
        ControlType.IAP: IAP,
        ControlType.NAK: NAK_BASE,
        ControlType.EOT: EOT_BASE,
        ControlType.ACK: ACK_BASE,
        ControlType.ACKPOLL: ACKPOLL_BASE,
    }
    if control_type not in mapping:
        raise ValueError(f"No base defined for control type {control_type}")
    return mapping[control_type]


def classify_control(control: int) -> ControlType:
    """Classify a CTRL byte into a control family.

    POLL and IAP are documented as fixed values (0x20, 0x40).
    Other families use high-nibble ranges with TX# in the low nibble.
    """
    if not 0 <= control <= 0xFF:
        raise ValueError(f"control byte out of range: {control}")

    if control == POLL:
        return ControlType.POLL
    if control == IAP:
        return ControlType.IAP

    high = control & CONTROL_TYPE_MASK
    if high == DATA_BASE:
        return ControlType.DATA
    if high == NAK_BASE:
        return ControlType.NAK
    if high == EOT_BASE:
        return ControlType.EOT
    if high == ACK_BASE:
        return ControlType.ACK
    if high == ACKPOLL_BASE:
        return ControlType.ACKPOLL
    return ControlType.UNKNOWN


def extract_sequence(control: int) -> int:
    """Extract TX# (low nibble) from a CTRL byte.

    DART Serial Communication / Line-Level Specification, page 3:
    bits 3-0 hold the block sequence number (0-Fh).
    """
    if not 0 <= control <= 0xFF:
        raise ValueError(f"control byte out of range: {control}")
    return control & SEQUENCE_MASK


def compose_control(control_type: ControlType, sequence: int = 0) -> int:
    """Compose a CTRL byte from type and sequence.

    For POLL and IAP the specification shows fixed values; sequence must be 0.
    UNKNOWN cannot be composed.
    """
    if not 0 <= sequence <= 0x0F:
        raise ValueError(f"sequence out of range 0x0-0xF: {sequence}")
    if control_type is ControlType.UNKNOWN:
        raise ValueError("cannot compose UNKNOWN control type")
    if control_type in {ControlType.POLL, ControlType.IAP} and sequence != 0:
        raise ValueError(f"{control_type} is a fixed control byte; sequence must be 0")
    return control_type_base(control_type) | sequence
