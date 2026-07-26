"""Logical ↔ wire address mapping for captured Wayne iGEM / ePump profile.

Source of truth: passive ePump capture (merged ONE_PORT bus).
Logical side 1 → wire 0x50; logical side 2 → wire 0x51.
"""

from __future__ import annotations

from types import MappingProxyType

# Immutable table — only explicitly captured values; no arithmetic extension.
LEGACY_IGEM_WIRE_ADDRESSES: MappingProxyType[int, int] = MappingProxyType(
    {
        1: 0x50,
        2: 0x51,
    }
)

LEGACY_IGEM_WIRE_ADDRESS_SET: frozenset[int] = frozenset(
    LEGACY_IGEM_WIRE_ADDRESSES.values()
)

# Historical synthetic ADR=0x01 polls are invalid for captured Wayne iGEM V-11.06.
INVALID_SYNTHETIC_POLL_LOGICAL_AS_WIRE = bytes((0x01, 0x20, 0xFA))


class AddressMappingError(ValueError):
    """Logical or wire address is not in the captured legacy iGEM profile."""


def encode_wire_address(logical_address: int) -> int:
    """Map a logical pump side (1 or 2) to the captured wire ADR byte."""
    if logical_address in LEGACY_IGEM_WIRE_ADDRESS_SET:
        raise AddressMappingError(
            f"value 0x{logical_address:02X} looks like a wire address; "
            "pass logical address 1 or 2, not a raw wire ADR"
        )
    try:
        return LEGACY_IGEM_WIRE_ADDRESSES[logical_address]
    except KeyError as exc:
        raise AddressMappingError(
            f"unsupported logical address {logical_address}; "
            "captured legacy iGEM profile allows only 1 and 2"
        ) from exc


def decode_wire_address(wire_address: int) -> int:
    """Map a captured wire ADR byte back to logical side 1 or 2."""
    for logical, wire in LEGACY_IGEM_WIRE_ADDRESSES.items():
        if wire == wire_address:
            return logical
    raise AddressMappingError(
        f"unsupported wire address 0x{wire_address:02X}; "
        "captured legacy iGEM profile allows only 0x50 and 0x51"
    )


def is_legacy_igem_wire_address(value: int) -> bool:
    return value in LEGACY_IGEM_WIRE_ADDRESS_SET


def is_legacy_igem_logical_address(value: int) -> bool:
    return value in LEGACY_IGEM_WIRE_ADDRESSES
