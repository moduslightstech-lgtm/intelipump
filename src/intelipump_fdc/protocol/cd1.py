"""Documented CD1 pump control commands (Pump Interface Rev 2.11).

TRANS=0x01, LNG=1, DCC=command byte.
RETURN_STATUS=0x00, RESET=0x05, AUTHORIZE=0x06.
"""

from __future__ import annotations

from dataclasses import dataclass

from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.crc import dart_crc16
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame
from intelipump_fdc.protocol.sequence import WayneSequenceManager

CD1_TRANS = 0x01
ALLOWED_ACTIVE_CD1 = frozenset(
    {
        PumpControlCommand.RETURN_STATUS,
        PumpControlCommand.RESET,
        PumpControlCommand.AUTHORIZE,
    }
)


class CD1Error(ValueError):
    """Invalid CD1 construction."""


@dataclass(frozen=True, slots=True)
class CD1Command:
    command: PumpControlCommand
    application_payload: bytes
    source_reference: str = "Pump Interface Rev 2.11, page 13, CD1"

    @property
    def payload_hex(self) -> str:
        return self.application_payload.hex(" ")

    @property
    def dcc(self) -> int:
        return int(self.command)


def build_cd1_command(command: PumpControlCommand) -> CD1Command:
    if command not in ALLOWED_ACTIVE_CD1:
        raise CD1Error(
            f"CD1 command {command.name} is not enabled for real-Wayne active path"
        )
    payload = bytes((CD1_TRANS, 0x01, int(command) & 0xFF))
    return CD1Command(command=command, application_payload=payload)


def build_cd1_candidate_frame(
    *,
    logical_address: int,
    sequence: int,
    cd1: CD1Command,
) -> tuple[bytes, int, bytes]:
    """Build outer DART DATA frame in memory (or for single-shot TX)."""
    wire = encode_wire_address(logical_address)
    frame = build_data_frame(wire, sequence, cd1.application_payload)
    control = WayneSequenceManager.message_byte(sequence)
    crc = dart_crc16(bytes((wire, control)) + cd1.application_payload)
    expected_ack = bytes(
        (wire, WayneSequenceManager.expected_ack_byte(sequence), 0xFA)
    )
    return frame, crc, expected_ack
