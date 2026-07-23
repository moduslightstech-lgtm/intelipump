"""DART line-protocol constants.

Source: DART Serial Communication / Line-Level Specification, page 3
(document page labeled "Page 3"; PDF page 4).
"""

from __future__ import annotations

# Framing / escape bytes
# DART Serial Communication / Line-Level Specification, page 3
ETX: int = 0x03
DLE: int = 0x10
SF: int = 0xFA

# Control-byte base / fixed values (high nibble = type, low nibble = TX#)
# DART Serial Communication / Line-Level Specification, page 3
POLL: int = 0x20
DATA_BASE: int = 0x30  # 0x30-0x3F
IAP: int = 0x40
NAK_BASE: int = 0x50  # 0x50-0x5F
EOT_BASE: int = 0x70  # 0x70-0x7F
ACK_BASE: int = 0xC0  # 0xC0-0xCF
ACKPOLL_BASE: int = 0xE0  # 0xE0-0xEF

SEQUENCE_MASK: int = 0x0F
CONTROL_TYPE_MASK: int = 0xF0

# Max buffer size including all control characters.
# Ambiguity: whether this limit applies before or after DLE insertion on the wire.
# DART Serial Communication / Line-Level Specification, page 3
MAX_BUFFER_SIZE: int = 256

# Minimum / structural sizes (unescaped buffer, excluding trailing SF on the wire)
CONTROL_FRAME_SIZE: int = 2  # ADR + CTRL  (SF appended on wire)
# ADR + CTRL + CRC1 + CRC2 + ETX
MIN_DATA_FRAME_SIZE: int = 5
