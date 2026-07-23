from __future__ import annotations

import pytest

from intelipump_fdc.protocol.dart.line.constants import DLE, SF
from intelipump_fdc.protocol.dart.line.escaping import DleEscapeError, escape_dle, unescape_dle


def test_escape_no_sf() -> None:
    assert escape_dle(b"\x01\x02\x10") == b"\x01\x02\x10"


def test_escape_payload_containing_sf() -> None:
    assert escape_dle(bytes([SF])) == bytes([DLE, SF])
    assert escape_dle(b"\x01" + bytes([SF]) + b"\x02") == b"\x01" + bytes([DLE, SF]) + b"\x02"


def test_unescape_roundtrip() -> None:
    original = b"\x00\xfa\x10\x01"
    assert unescape_dle(escape_dle(original)) == original


def test_unescape_rejects_truncated_dle() -> None:
    with pytest.raises(DleEscapeError, match="truncated"):
        unescape_dle(bytes([DLE]))


def test_unescape_rejects_unescaped_sf() -> None:
    with pytest.raises(DleEscapeError, match="unescaped SF"):
        unescape_dle(bytes([SF]))


def test_literal_dle_followed_by_non_sf() -> None:
    assert unescape_dle(bytes([DLE, 0x20])) == bytes([DLE, 0x20])
