"""DC3 NOZIO byte decode (Pump Interface Rev 2.11, page 21, §3.2.3).

Wire layout inside DC3 (TRANS=0x03, LNG=4):

    03 04 PP PP PP NOZIO

NOZIO bit masks (documented):

- bits 0..3: selected logical nozzle number (1..15; 0 = none selected)
- bit 4: nozzle in/out (0 = in, 1 = out)
- bits 5..7: reserved/unknown — preserve and warn if nonzero
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

NOZIO_LOGICAL_NOZZLE_MASK = 0x0F
NOZIO_POSITION_MASK = 0x10
NOZIO_RESERVED_MASK = 0xE0

DC3_NOZIO_SPEC_REF = (
    "Pump Interface Rev 2.11, page 21, section 3.2.3 "
    "Nozzle status and filling price (DC3 / NOZIO)"
)


@dataclass(frozen=True, slots=True)
class NozioDecode:
    """Documented NOZIO field decode with evidence fields."""

    nozio_raw: int
    logical_nozzle_raw: int
    selected_logical_nozzle: int | None
    nozzle_out: bool
    reserved_bits: int
    warnings: tuple[str, ...]
    decoder_confidence: str
    documentation_source: str = DC3_NOZIO_SPEC_REF

    @property
    def nozzle_position(self) -> str:
        return "OUT" if self.nozzle_out else "IN"

    @property
    def nozio_raw_hex(self) -> str:
        return f"{self.nozio_raw:02X}"

    @property
    def nozio_binary(self) -> str:
        return format(self.nozio_raw & 0xFF, "08b")

    def to_evidence_dict(self) -> dict[str, Any]:
        return {
            "nozioRawHex": self.nozio_raw_hex,
            "nozioBinary": self.nozio_binary,
            "logicalNozzleMask": f"0x{NOZIO_LOGICAL_NOZZLE_MASK:02X}",
            "positionMask": f"0x{NOZIO_POSITION_MASK:02X}",
            "reservedBits": f"0x{self.reserved_bits:02X}",
            "logicalNozzleRaw": self.logical_nozzle_raw,
            "selectedLogicalNozzle": self.selected_logical_nozzle,
            "nozzlePosition": self.nozzle_position,
            "nozzleOut": self.nozzle_out,
            "documentationSource": self.documentation_source,
            "decoderConfidence": self.decoder_confidence,
            "warnings": list(self.warnings),
        }


def decode_nozio(nozio: int) -> NozioDecode:
    """Decode one NOZIO byte with explicit documented masks.

    Preserves reserved upper bits with a warning; never discards the
    transaction or invents meanings for bits 5..7.
    """
    value = int(nozio) & 0xFF
    logical_raw = value & NOZIO_LOGICAL_NOZZLE_MASK
    selected = logical_raw if 1 <= logical_raw <= 15 else None
    nozzle_out = bool(value & NOZIO_POSITION_MASK)
    reserved = value & NOZIO_RESERVED_MASK
    warnings: list[str] = []
    if reserved:
        warnings.append(
            f"NOZIO reserved bits 5..7 nonzero: 0x{reserved:02X} "
            f"(raw=0x{value:02X}); preserved, not interpreted"
        )
        confidence = "MEDIUM_RESERVED_BITS_SET"
    else:
        confidence = "HIGH_DOCUMENTED_MASKS"
    if logical_raw == 0:
        warnings.append(
            "NOZIO logical nozzle raw=0; treated as no selected logical nozzle"
        )
    return NozioDecode(
        nozio_raw=value,
        logical_nozzle_raw=logical_raw,
        selected_logical_nozzle=selected,
        nozzle_out=nozzle_out,
        reserved_bits=reserved,
        warnings=tuple(warnings),
        decoder_confidence=confidence,
    )


__all__ = [
    "DC3_NOZIO_SPEC_REF",
    "NOZIO_LOGICAL_NOZZLE_MASK",
    "NOZIO_POSITION_MASK",
    "NOZIO_RESERVED_MASK",
    "NozioDecode",
    "decode_nozio",
]
