"""Owned-lab nozzle physical-state helpers for CD1 RESET diagnostics.

Production DC3 decode keeps documented ``nozzle_out = bool(nozio & 0x10)``.
This module adds an optional controller profile that can mark the physical
position UNKNOWN when a lab head does not expose bit 0x10.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class NozzlePhysicalState(StrEnum):
    IN = "IN"
    OUT = "OUT"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class ControllerProfile:
    """Per-controller interpretation of the documented NOZIO out bit."""

    supports_nozio_out_bit: bool = False


def decode_nozzle_state(
    nozio: int, controller_profile: ControllerProfile
) -> NozzlePhysicalState:
    """Decode physical nozzle state under a controller profile.

    When ``supports_nozio_out_bit`` is True, use documented bit 0x10.
    When False (owned lab head that never asserts 0x10), return UNKNOWN
    rather than treating missing bit as definitive IN.
    """
    value = int(nozio) & 0xFF
    if controller_profile.supports_nozio_out_bit:
        return (
            NozzlePhysicalState.OUT
            if value & 0x10
            else NozzlePhysicalState.IN
        )
    return NozzlePhysicalState.UNKNOWN


__all__ = [
    "ControllerProfile",
    "NozzlePhysicalState",
    "decode_nozzle_state",
]
