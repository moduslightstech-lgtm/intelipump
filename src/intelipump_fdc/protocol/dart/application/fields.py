"""Field helpers for application decoding (BCD → scaled integers / Decimal)."""

from __future__ import annotations

from decimal import Decimal

from intelipump_fdc.protocol.dart.application.bcd import BcdError, decode_packed_bcd
from intelipump_fdc.protocol.dart.application.models import ScaledBcdValue


def decode_scaled_bcd(
    raw: bytes,
    *,
    decimals: int | None = None,
) -> ScaledBcdValue:
    """Decode packed BCD to a scaled integer and optional Decimal.

    ``decimals`` must come from documented configuration (e.g. DC7). When
    omitted, ``value`` is left as None.
    """
    scaled = decode_packed_bcd(raw)
    value: Decimal | None = None
    if decimals is not None:
        if decimals < 0:
            raise BcdError(f"decimals must be non-negative, got {decimals}")
        value = Decimal(scaled).scaleb(-decimals)
    return ScaledBcdValue(
        raw_bcd=bytes(raw),
        raw_scaled=scaled,
        decimals=decimals,
        value=value,
    )
