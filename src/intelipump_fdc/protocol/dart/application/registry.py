"""Transaction registry / documentation map.

Wire TRANS IDs collide between CD and DC families. This registry records
documented names and does not by itself select a decoder — see ``decoder.py``
for structural dispatch.
"""

from __future__ import annotations

from dataclasses import dataclass

from intelipump_fdc.protocol.dart.application.constants import (
    MessageDirection,
    TransactionType,
)


@dataclass(frozen=True, slots=True)
class TransactionRegistration:
    wire_id: int
    logical_type: TransactionType
    documented_direction: MessageDirection
    title: str
    spec_pages: str
    typical_data_length: int | None


REGISTRY: tuple[TransactionRegistration, ...] = (
    TransactionRegistration(
        0x01,
        TransactionType.CD1_COMMAND,
        MessageDirection.MASTER_TO_SLAVE,
        "Command to pump",
        "13",
        1,
    ),
    TransactionRegistration(
        0x01,
        TransactionType.DC1_PUMP_STATUS,
        MessageDirection.SLAVE_TO_MASTER,
        "Pump status",
        "20",
        1,
    ),
    TransactionRegistration(
        0x02,
        TransactionType.CD2_ALLOWED_NOZZLES,
        MessageDirection.MASTER_TO_SLAVE,
        "Allowed nozzle numbers",
        "14",
        None,
    ),
    TransactionRegistration(
        0x02,
        TransactionType.DC2_FILLED_VOLUME_AMOUNT,
        MessageDirection.SLAVE_TO_MASTER,
        "Filled volume and amount",
        "20",
        8,
    ),
    TransactionRegistration(
        0x03,
        TransactionType.CD3_PRESET_VOLUME,
        MessageDirection.MASTER_TO_SLAVE,
        "Preset volume",
        "14",
        4,
    ),
    TransactionRegistration(
        0x03,
        TransactionType.DC3_NOZZLE_STATUS_PRICE,
        MessageDirection.SLAVE_TO_MASTER,
        "Nozzle status and filling price",
        "21",
        4,
    ),
    TransactionRegistration(
        0x05,
        TransactionType.CD5_PRICE_UPDATE,
        MessageDirection.MASTER_TO_SLAVE,
        "Price update",
        "15",
        None,
    ),
    TransactionRegistration(
        0x05,
        TransactionType.DC5_ALARM,
        MessageDirection.SLAVE_TO_MASTER,
        "Alarm code",
        "22",
        1,
    ),
    TransactionRegistration(
        0x65,
        TransactionType.CD101_REQUEST_TOTALS,
        MessageDirection.MASTER_TO_SLAVE,
        "Request Total Counters",
        "19",
        1,
    ),
    TransactionRegistration(
        0x65,
        TransactionType.DC101_TOTAL_COUNTERS,
        MessageDirection.SLAVE_TO_MASTER,
        "Total Counters",
        "25",
        16,
    ),
)


def lookup_wire_id(wire_id: int) -> tuple[TransactionRegistration, ...]:
    return tuple(item for item in REGISTRY if item.wire_id == wire_id)
