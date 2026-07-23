"""CRC-16 functions for DART DATA frames.

Documented requirements (DART Serial Communication / Line-Level Specification,
pages 2-3; docs/protocol-notes/dart-line-summary.md):
- CRC calculated from ADR through the last unescaped data byte
- Inserted DLE bytes excluded
- CRC-1 low byte, CRC-2 high byte
- Initial value 0000h
- Vendor text names "CRC16 (CCITT)"

Phase 2 capture validation (see docs/protocol-notes/capture-validation-report.md):
- None of the CCITT-family candidates matched any complete captured DATA frame.
- The IBM/ANSI/Modbus-style algorithm (reflected poly 0xA001, init 0x0000,
  xorout 0x0000) matched all 2098 assembled DATA candidates across three
  merged-bus captures.
- That algorithm is therefore the canonical DART line CRC for this project,
  while CCITT candidates remain available for diagnostics.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum

CrcFn = Callable[[bytes], int]


class CrcCandidate(StrEnum):
    """Named CRC-16 candidates.

    ``DART_IBM_ANSI_INIT_0000`` is the capture-proven canonical algorithm.
    CCITT_* entries did not match captured traffic in Phase 2.
    """

    # Canonical (Phase 2 proven against captures)
    DART_IBM_ANSI_INIT_0000 = "dart_ibm_ansi_init_0000"
    # Diagnostic CCITT-family candidates (init 0x0000)
    CCITT_FALSE_INIT_0000 = "ccitt_false_init_0000"
    CCITT_FALSE_INIT_0000_XOR_FFFF = "ccitt_false_init_0000_xor_ffff"
    CCITT_TRUE_INIT_0000 = "ccitt_true_init_0000"
    CCITT_TRUE_INIT_0000_XOR_FFFF = "ccitt_true_init_0000_xor_ffff"


def crc16_dart_ibm_ansi_init_0000(data: bytes) -> int:
    """Canonical DART line CRC-16 (capture-proven).

    Algorithm: reflected poly 0xA001, init 0x0000, xorout 0x0000
    (CRC-16-IBM / ANSI / Modbus-style bit processing).

    Despite vendor "CCITT" naming, Phase 2 merged-bus captures matched this
    function on every complete DATA candidate tested (2098 frames).
    """
    return _crc16_lsb_first(data, poly=0xA001, init=0x0000, xorout=0x0000)


# Explicit alias requested for call sites / docs.
dart_crc16 = crc16_dart_ibm_ansi_init_0000


def crc16_ccitt_false_init_0000(data: bytes) -> int:
    """CRC-16-CCITT false/non-reflected, init 0x0000, xorout 0x0000, poly 0x1021."""
    return _crc16_msb_first(data, poly=0x1021, init=0x0000, xorout=0x0000)


def crc16_ccitt_false_init_0000_xor_ffff(data: bytes) -> int:
    """CRC-16-CCITT false/non-reflected, init 0x0000, xorout 0xFFFF, poly 0x1021."""
    return _crc16_msb_first(data, poly=0x1021, init=0x0000, xorout=0xFFFF)


def crc16_ccitt_true_init_0000(data: bytes) -> int:
    """CRC-16-CCITT true/reflected, init 0x0000, xorout 0x0000, poly 0x8408."""
    return _crc16_lsb_first(data, poly=0x8408, init=0x0000, xorout=0x0000)


def crc16_ccitt_true_init_0000_xor_ffff(data: bytes) -> int:
    """CRC-16-CCITT true/reflected, init 0x0000, xorout 0xFFFF, poly 0x8408."""
    return _crc16_lsb_first(data, poly=0x8408, init=0x0000, xorout=0xFFFF)


CRC_CANDIDATES: dict[CrcCandidate, CrcFn] = {
    CrcCandidate.DART_IBM_ANSI_INIT_0000: crc16_dart_ibm_ansi_init_0000,
    CrcCandidate.CCITT_FALSE_INIT_0000: crc16_ccitt_false_init_0000,
    CrcCandidate.CCITT_FALSE_INIT_0000_XOR_FFFF: crc16_ccitt_false_init_0000_xor_ffff,
    CrcCandidate.CCITT_TRUE_INIT_0000: crc16_ccitt_true_init_0000,
    CrcCandidate.CCITT_TRUE_INIT_0000_XOR_FFFF: crc16_ccitt_true_init_0000_xor_ffff,
}

# Capture-proven default for builder/parser computed_crc fields.
DEFAULT_CRC_CANDIDATE = CrcCandidate.DART_IBM_ANSI_INIT_0000
CANONICAL_CRC_CANDIDATE = CrcCandidate.DART_IBM_ANSI_INIT_0000


def get_crc_fn(candidate: CrcCandidate = DEFAULT_CRC_CANDIDATE) -> CrcFn:
    return CRC_CANDIDATES[candidate]


def compute_all_candidates(data: bytes) -> dict[str, int]:
    """Compute every named candidate over ``data`` (ADR..last data byte)."""
    return {name.value: fn(data) for name, fn in CRC_CANDIDATES.items()}


def crc_bytes_le(crc: int) -> bytes:
    """Return CRC-1 (low) then CRC-2 (high).

    DART Serial Communication / Line-Level Specification, page 3.
    """
    if not 0 <= crc <= 0xFFFF:
        raise ValueError(f"CRC out of range: {crc}")
    return bytes((crc & 0xFF, (crc >> 8) & 0xFF))


def crc_from_le_bytes(crc1: int, crc2: int) -> int:
    """Reassemble CRC word from CRC-1 (low) and CRC-2 (high)."""
    if not 0 <= crc1 <= 0xFF or not 0 <= crc2 <= 0xFF:
        raise ValueError("CRC bytes out of range")
    return crc1 | (crc2 << 8)


def _crc16_msb_first(data: bytes, *, poly: int, init: int, xorout: int) -> int:
    crc = init
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ poly) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc ^ xorout


def _crc16_lsb_first(data: bytes, *, poly: int, init: int, xorout: int) -> int:
    crc = init
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ poly
            else:
                crc >>= 1
            crc &= 0xFFFF
    return crc ^ xorout
