"""DLE escaping / unescaping for DART line transparency.

Source: DART Serial Communication / Line-Level Specification, page 2.

Rules:
- DLE (0x10) is inserted before any data or CRC byte equal to SF (0xFA).
- Inserted DLE bytes are excluded from CRC calculation.
- On receive, DLE followed by SF yields a single SF in the buffer
  (DLE is overwritten by SF per the Rx interrupt description).
- Only SF is escaped; a lone DLE in the payload is a literal 0x10.
"""

from __future__ import annotations

from intelipump_fdc.protocol.dart.line.constants import DLE, SF


class DleEscapeError(ValueError):
    """Malformed DLE escape sequence."""


def escape_dle(data: bytes) -> bytes:
    """Insert DLE before each SF byte in ``data`` (payload/CRC/ETX region)."""
    out = bytearray()
    for byte in data:
        if byte == SF:
            out.append(DLE)
        out.append(byte)
    return bytes(out)


def unescape_dle(data: bytes) -> bytes:
    """Reverse DLE insertion for a buffer that must not contain a frame SF.

    Rejects malformed sequences:
    - DLE as the final byte (truncated escape)
    - Unescaped SF inside the buffer (would have terminated the frame)
    """
    out = bytearray()
    index = 0
    length = len(data)
    while index < length:
        byte = data[index]
        if byte == DLE:
            if index + 1 >= length:
                raise DleEscapeError("truncated DLE escape at end of buffer")
            nxt = data[index + 1]
            if nxt != SF:
                # Literal DLE followed by a non-SF byte: keep DLE as data.
                out.append(DLE)
                index += 1
                continue
            out.append(SF)
            index += 2
            continue
        if byte == SF:
            raise DleEscapeError("unescaped SF inside frame body")
        out.append(byte)
        index += 1
    return bytes(out)
