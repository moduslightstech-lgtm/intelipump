"""Simulator protocol fault types (virtual bus only)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ProtocolFaultKind(StrEnum):
    INVALID_CRC = "INVALID_CRC"
    UNEXPECTED_SEQUENCE = "UNEXPECTED_SEQUENCE"
    MALFORMED_FRAME = "MALFORMED_FRAME"
    RESPONSE_TIMEOUT = "RESPONSE_TIMEOUT"
    UNKNOWN_ADDRESS = "UNKNOWN_ADDRESS"
    APPLICATION_REJECTED = "APPLICATION_REJECTED"
    INELIGIBLE_COMMAND = "INELIGIBLE_COMMAND"
    DUPLICATE_DATA = "DUPLICATE_DATA"


@dataclass(frozen=True, slots=True)
class ProtocolFault:
    kind: ProtocolFaultKind
    message: str
    address: int | None = None
    sequence: int | None = None
    raw_frame_hex: str | None = None
    at_ms: int | None = None
