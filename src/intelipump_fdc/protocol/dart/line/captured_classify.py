"""Cautious classifications for captured Wayne iGEM / ePump line frames.

Direction on the passive merged capture is inferred only, never certain.
C0-CF are not treated as nozzle-lift or sale state.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from intelipump_fdc.protocol.dart.line.addressing import is_legacy_igem_wire_address
from intelipump_fdc.protocol.dart.line.constants import (
    ACK_BASE,
    CONTROL_TYPE_MASK,
    DATA_BASE,
    EOT_BASE,
    POLL,
    SEQUENCE_MASK,
)
from intelipump_fdc.protocol.dart.line.control import ControlType


class CapturedFrameClass(StrEnum):
    POLL = "POLL"
    SHORT_CONTROL_70 = "SHORT_CONTROL_70"
    SEQUENCE_CONTROL_OR_ACK = "SEQUENCE_CONTROL_OR_ACK"
    DATA_FRAME = "DATA_FRAME"
    UNKNOWN_FRAME = "UNKNOWN_FRAME"
    PARTIAL_FRAME = "PARTIAL_FRAME"


class InferredDirection(StrEnum):
    INFERRED_CONTROLLER_TO_PUMP = "INFERRED_CONTROLLER_TO_PUMP"
    INFERRED_PUMP_TO_CONTROLLER = "INFERRED_PUMP_TO_CONTROLLER"
    UNKNOWN_DIRECTION = "UNKNOWN_DIRECTION"


class PayloadSemantics(StrEnum):
    UNKNOWN_PAYLOAD = "UNKNOWN_PAYLOAD"


@dataclass(frozen=True, slots=True)
class CapturedFrameView:
    """Cautious view over a structurally recognized captured frame."""

    classification: CapturedFrameClass
    wire_address: int
    logical_address: int | None
    control: int
    sequence_nibble: int | None
    possible_acknowledgement: bool
    raw_payload_hex: str | None
    payload_length: int
    payload_semantics: PayloadSemantics
    inferred_direction: InferredDirection
    crc_valid: bool | None
    # Legacy ControlType mapping for callers that still need it (DATA/POLL/…).
    dart_control_type: ControlType | None


def classify_captured_control(control: int) -> CapturedFrameClass:
    if control == POLL:
        return CapturedFrameClass.POLL
    high = control & CONTROL_TYPE_MASK
    if high == EOT_BASE:
        return CapturedFrameClass.SHORT_CONTROL_70
    if high == ACK_BASE:
        return CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK
    if high == DATA_BASE:
        return CapturedFrameClass.DATA_FRAME
    return CapturedFrameClass.UNKNOWN_FRAME


def infer_direction(classification: CapturedFrameClass) -> InferredDirection:
    """Shape-based inference only — never treat as measured direction."""
    if classification is CapturedFrameClass.POLL:
        return InferredDirection.INFERRED_CONTROLLER_TO_PUMP
    if classification in {
        CapturedFrameClass.SHORT_CONTROL_70,
        CapturedFrameClass.DATA_FRAME,
        CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK,
    }:
        return InferredDirection.INFERRED_PUMP_TO_CONTROLLER
    return InferredDirection.UNKNOWN_DIRECTION


def sequence_correlation_note(data_ctrl: int, ack_ctrl: int) -> str | None:
    """Record cautious 3N→CN nibble correlation when both are present."""
    if (data_ctrl & CONTROL_TYPE_MASK) != DATA_BASE:
        return None
    if (ack_ctrl & CONTROL_TYPE_MASK) != ACK_BASE:
        return None
    if (data_ctrl & SEQUENCE_MASK) != (ack_ctrl & SEQUENCE_MASK):
        return None
    nibble = data_ctrl & SEQUENCE_MASK
    return (
        f"sequenceNibble={nibble}; possibleAcknowledgement=true; "
        f"observed 0x{data_ctrl:02X}->0x{ack_ctrl:02X} "
        "(not a proven sale or lift event)"
    )


def is_nozzle_lift_classification(classification: CapturedFrameClass | str) -> bool:
    """C0-CF must never be treated as nozzle-lift events."""
    name = (
        classification.value
        if isinstance(classification, CapturedFrameClass)
        else str(classification)
    )
    return "nozzle" in name.lower() and "lift" in name.lower()


def captured_wire_address_ok(address: int) -> bool:
    return is_legacy_igem_wire_address(address)
