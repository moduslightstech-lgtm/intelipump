"""Wayne price BCD codecs.

Documented CD5 unit prices use three-byte packed BCD (six digits).
The two-byte codec is legacy/hypothesis-only for merged-capture analysis.
"""

from __future__ import annotations

_MIN_3 = 1
_MAX_3 = 999_999
_MIN_2 = 1
_MAX_2 = 9999


class PriceCodecError(ValueError):
    """Invalid price BCD encode/decode input."""


def encode_price_bcd_3(price: int) -> bytes:
    """Encode display/raw integer price as exactly three packed-BCD bytes.

    Examples: 1175 → ``00 11 75``, 850 → ``00 08 50``, 999999 → ``99 99 99``.
    """
    if type(price) is not int:
        raise PriceCodecError("price must be an int (no float)")
    if price < _MIN_3 or price > _MAX_3:
        raise PriceCodecError(f"price out of range {_MIN_3}-{_MAX_3}: {price}")
    digits = f"{price:06d}"
    return bytes(
        (
            (int(digits[0]) << 4) | int(digits[1]),
            (int(digits[2]) << 4) | int(digits[3]),
            (int(digits[4]) << 4) | int(digits[5]),
        )
    )


def decode_price_bcd_3(data: bytes) -> int:
    """Decode exactly three packed-BCD bytes to an integer price."""
    if not isinstance(data, (bytes, bytearray)):
        raise PriceCodecError("data must be bytes")
    if len(data) != 3:
        raise PriceCodecError(f"expected exactly 3 BCD bytes, got {len(data)}")
    value = 0
    for byte in data:
        high = (byte >> 4) & 0x0F
        low = byte & 0x0F
        if high > 9 or low > 9:
            raise PriceCodecError(f"non-decimal BCD nibble in byte 0x{byte:02X}")
        value = value * 100 + high * 10 + low
    if value < _MIN_3 or value > _MAX_3:
        raise PriceCodecError(f"decoded price out of range: {value}")
    return value


def encode_price_bcd_2_legacy(price: int, *, allow_zero: bool = False) -> bytes:
    """Legacy/hypothesis-only two-byte BCD (capture analysis). Not for CD5."""
    if type(price) is not int:
        raise PriceCodecError("price must be an int (no float)")
    if price == 0 and not allow_zero:
        raise PriceCodecError("price 0 rejected unless allow_zero=True")
    if price < _MIN_2 and not (price == 0 and allow_zero):
        raise PriceCodecError(f"price out of range {_MIN_2}-{_MAX_2}: {price}")
    if price > _MAX_2:
        raise PriceCodecError(f"price out of range {_MIN_2}-{_MAX_2}: {price}")
    digits = f"{price:04d}"
    return bytes(
        (
            (int(digits[0]) << 4) | int(digits[1]),
            (int(digits[2]) << 4) | int(digits[3]),
        )
    )


def decode_price_bcd_2_legacy(data: bytes, *, allow_zero: bool = False) -> int:
    """Legacy/hypothesis-only two-byte BCD decode. Not for CD5."""
    if not isinstance(data, (bytes, bytearray)):
        raise PriceCodecError("data must be bytes")
    if len(data) != 2:
        raise PriceCodecError(f"expected exactly 2 BCD bytes, got {len(data)}")
    value = 0
    for byte in data:
        high = (byte >> 4) & 0x0F
        low = byte & 0x0F
        if high > 9 or low > 9:
            raise PriceCodecError(f"non-decimal BCD nibble in byte 0x{byte:02X}")
        value = value * 100 + high * 10 + low
    if value == 0 and not allow_zero:
        raise PriceCodecError("decoded price 0 rejected unless allow_zero=True")
    if value > _MAX_2:
        raise PriceCodecError(f"decoded price out of range: {value}")
    return value


# Back-compat aliases clearly marked legacy.
encode_price_bcd = encode_price_bcd_2_legacy
decode_price_bcd = decode_price_bcd_2_legacy
