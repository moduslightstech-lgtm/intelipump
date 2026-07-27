"""Combined CD2 allowed-nozzles + CD1 RESET block (single DATA frame).

Documented DART flow after price / next-customer: CD2 then CD1 RESET may share
one level-3 block. Lab hypothesis after lone CD1 RESET ACK-without-DC1-change.
"""

from __future__ import annotations

from dataclasses import dataclass

from intelipump_fdc.protocol.cd1 import build_cd1_command
from intelipump_fdc.protocol.cd2 import CD2AllowedNozzles, build_cd2_allowed_nozzles
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.crc import dart_crc16
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame
from intelipump_fdc.protocol.sequence import WayneSequenceManager


@dataclass(frozen=True, slots=True)
class CD2ResetBlock:
    cd2: CD2AllowedNozzles
    application_payload: bytes
    source_reference: str = (
        "Pump Interface Rev 2.11 filling examples (CD2 then CD1 RESET)"
    )

    @property
    def payload_hex(self) -> str:
        return self.application_payload.hex(" ")


def build_cd2_reset_block(
    logical_nozzles: list[int] | tuple[int, ...],
) -> CD2ResetBlock:
    cd2 = build_cd2_allowed_nozzles(logical_nozzles)
    cd1 = build_cd1_command(PumpControlCommand.RESET)
    payload = cd2.application_payload + cd1.application_payload
    return CD2ResetBlock(cd2=cd2, application_payload=payload)


def build_cd2_reset_candidate_frame(
    *,
    logical_address: int,
    sequence: int,
    block: CD2ResetBlock,
) -> tuple[bytes, int, bytes]:
    wire = encode_wire_address(logical_address)
    frame = build_data_frame(wire, sequence, block.application_payload)
    control = WayneSequenceManager.message_byte(sequence)
    crc = dart_crc16(bytes((wire, control)) + block.application_payload)
    expected_ack = bytes(
        (wire, WayneSequenceManager.expected_ack_byte(sequence), 0xFA)
    )
    return frame, crc, expected_ack


def is_cd2_reset_application_payload(payload: bytes) -> bool:
    """True if payload is exactly CD2(allowed nozzles) + CD1 RESET."""
    if len(payload) < 6:
        return False
    if payload[0] != 0x02:
        return False
    lng = payload[1]
    if lng < 1 or len(payload) < 2 + lng + 3:
        return False
    nozzles = payload[2 : 2 + lng]
    if any(not 1 <= n <= 0x0F for n in nozzles):
        return False
    if len(set(nozzles)) != len(nozzles):
        return False
    rest = payload[2 + lng :]
    return rest == bytes((0x01, 0x01, int(PumpControlCommand.RESET)))
