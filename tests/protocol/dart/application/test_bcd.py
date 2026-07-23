from __future__ import annotations

import pytest

from intelipump_fdc.protocol.dart.application.bcd import (
    BcdError,
    decode_packed_bcd,
    encode_packed_bcd,
    is_valid_packed_bcd,
)


def test_valid_packed_bcd_roundtrip() -> None:
    assert decode_packed_bcd(b"\x12\x34") == 1234
    assert encode_packed_bcd(1234) == b"\x12\x34"
    assert encode_packed_bcd(5) == b"\x05"
    assert encode_packed_bcd(5, length=2) == b"\x00\x05"
    assert is_valid_packed_bcd(b"\x00") is True


def test_invalid_packed_bcd_nibbles() -> None:
    assert is_valid_packed_bcd(b"\x1A") is False
    with pytest.raises(BcdError, match="invalid BCD"):
        decode_packed_bcd(b"\x1A")
    with pytest.raises(BcdError, match="invalid BCD"):
        decode_packed_bcd(b"\xA0")
    with pytest.raises(BcdError):
        decode_packed_bcd(b"")


def test_encode_rejects_negative_and_overflow() -> None:
    with pytest.raises(BcdError):
        encode_packed_bcd(-1)
    with pytest.raises(BcdError):
        encode_packed_bcd(100, length=1)
