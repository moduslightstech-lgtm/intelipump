"""Line-frame models and typed parse errors."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from intelipump_fdc.protocol.dart.line.control import ControlType


class ParseErrorCode(StrEnum):
    TOO_SHORT = "TOO_SHORT"
    MISSING_SF = "MISSING_SF"
    INVALID_TERMINATOR = "INVALID_TERMINATOR"
    MALFORMED_DLE = "MALFORMED_DLE"
    TOO_SHORT_DATA = "TOO_SHORT_DATA"
    MISSING_ETX = "MISSING_ETX"
    BUFFER_TOO_LARGE = "BUFFER_TOO_LARGE"


@dataclass(frozen=True, slots=True)
class ParseError:
    """Typed parse failure that preserves diagnostic context."""

    code: ParseErrorCode
    message: str
    raw: bytes = field(default=b"")


@dataclass(frozen=True, slots=True)
class DartLineFrame:
    """Decoded DART line frame (control or DATA).

    ``raw_frame`` is the complete wire image including the trailing SF.
    CRC fields are populated for DATA frames; for control frames they are None.
    ``crc_valid`` compares received vs computed using the selected candidate
    only and is NOT proof of the correct DART CRC convention.
    """

    address: int
    control: int
    control_type: ControlType
    sequence: int
    payload: bytes
    received_crc: int | None
    computed_crc: int | None
    crc_valid: bool | None
    raw_frame: bytes
    # True when CTRL classified as UNKNOWN but structural parse succeeded.
    unknown_control: bool = False
