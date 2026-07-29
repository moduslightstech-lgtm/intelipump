"""CD101 request total counters (Pump Interface Rev 2.11, page 19).

TRANS=0x65, LNG=1, DATA=counter select (lab default 1, matching ePump captures).
Read-only request — does not authorize delivery.
"""

from __future__ import annotations

from dataclasses import dataclass

from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.crc import dart_crc16
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame
from intelipump_fdc.protocol.sequence import WayneSequenceManager

CD101_TRANS = 0x65
DEFAULT_COUNTER_SELECT = 1


class CD101Error(ValueError):
    """Invalid CD101 construction."""


@dataclass(frozen=True, slots=True)
class CD101Request:
    counter_select: int
    application_payload: bytes
    source_reference: str = "Pump Interface Rev 2.11, page 19, CD101"

    @property
    def payload_hex(self) -> str:
        return self.application_payload.hex(" ")


def build_cd101_request(*, counter_select: int = DEFAULT_COUNTER_SELECT) -> CD101Request:
    if not 0 <= int(counter_select) <= 0xFF:
        raise CD101Error(f"counter_select out of range: {counter_select}")
    sel = int(counter_select) & 0xFF
    payload = bytes((CD101_TRANS, 0x01, sel))
    return CD101Request(counter_select=sel, application_payload=payload)


def build_cd101_candidate_frame(
    *,
    logical_address: int,
    sequence: int,
    cd101: CD101Request,
) -> tuple[bytes, int, bytes]:
    """Build outer DART DATA frame for a single-shot CD101 TX."""
    wire = encode_wire_address(logical_address)
    frame = build_data_frame(wire, sequence, cd101.application_payload)
    control = WayneSequenceManager.message_byte(sequence)
    crc = dart_crc16(bytes((wire, control)) + cd101.application_payload)
    expected_ack = bytes(
        (wire, WayneSequenceManager.expected_ack_byte(sequence), 0xFA)
    )
    return frame, crc, expected_ack


def is_cd101_application_payload(app: bytes) -> bool:
    return len(app) >= 3 and app[0] == CD101_TRANS and app[1] == 0x01
