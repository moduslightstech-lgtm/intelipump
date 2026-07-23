"""Packed BCD helpers independent of DART application message decoding.

These utilities encode/decode classic packed BCD (two decimal digits per byte).
They do not interpret pump/application field layouts.
"""

from __future__ import annotations


class BcdError(ValueError):
    """Invalid packed BCD input."""


def decode_packed_bcd(data: bytes) -> int:
    """Decode packed BCD bytes to a non-negative integer.

    Each nibble must be 0-9. Empty input is rejected.
    """
    if not data:
        raise BcdError("empty packed BCD")
    value = 0
    for byte in data:
        high = (byte >> 4) & 0x0F
        low = byte & 0x0F
        if high > 9 or low > 9:
            raise BcdError(f"invalid BCD nibble in byte 0x{byte:02X}")
        value = value * 100 + high * 10 + low
    return value


def encode_packed_bcd(value: int, *, length: int | None = None) -> bytes:
    """Encode a non-negative integer as packed BCD.

    If ``length`` is given, left-pad with 0x00 digits to that many bytes.
    """
    if value < 0:
        raise BcdError("BCD value must be non-negative")
    if length is not None and length < 0:
        raise BcdError("length must be non-negative")

    digits = f"{value:d}"
    if len(digits) % 2 == 1:
        digits = "0" + digits
    raw = bytes(
        (int(digits[i]) << 4) | int(digits[i + 1]) for i in range(0, len(digits), 2)
    )
    if length is None:
        return raw if raw else b"\x00"
    if len(raw) > length:
        raise BcdError(f"value {value} does not fit in {length} packed BCD bytes")
    return (b"\x00" * (length - len(raw))) + raw


def is_valid_packed_bcd(data: bytes) -> bool:
    """Return True iff every nibble is in 0..9 and data is non-empty."""
    try:
        decode_packed_bcd(data)
    except BcdError:
        return False
    return True
