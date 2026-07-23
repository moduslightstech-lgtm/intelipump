from __future__ import annotations

from intelipump_fdc.protocol.dart.line.crc import (
    CANONICAL_CRC_CANDIDATE,
    CRC_CANDIDATES,
    CrcCandidate,
    compute_all_candidates,
    crc16_ccitt_false_init_0000,
    crc16_ccitt_true_init_0000,
    crc16_dart_ibm_ansi_init_0000,
    crc_bytes_le,
    crc_from_le_bytes,
    dart_crc16,
)


def test_all_candidates_are_deterministic_and_named() -> None:
    data = b"\x51\x30\x65\x01\x01"
    results = compute_all_candidates(data)
    assert set(results) == {c.value for c in CrcCandidate}
    assert len(results) == len(CRC_CANDIDATES)
    for value in results.values():
        assert 0 <= value <= 0xFFFF


def test_candidates_differ_for_sample_input() -> None:
    data = b"\x51\x30\x65\x01\x01"
    false_v = crc16_ccitt_false_init_0000(data)
    true_v = crc16_ccitt_true_init_0000(data)
    dart_v = crc16_dart_ibm_ansi_init_0000(data)
    assert false_v != true_v
    assert dart_v != false_v
    assert dart_v != true_v


def test_canonical_alias() -> None:
    data = b"\x51\x30\x65\x01\x01"
    assert dart_crc16(data) == crc16_dart_ibm_ansi_init_0000(data)
    assert CANONICAL_CRC_CANDIDATE is CrcCandidate.DART_IBM_ANSI_INIT_0000


def test_crc_byte_order_le() -> None:
    assert crc_bytes_le(0x8322) == b"\x22\x83"
    assert crc_from_le_bytes(0x22, 0x83) == 0x8322


def test_empty_input_stable() -> None:
    assert crc16_ccitt_false_init_0000(b"") == 0x0000
    assert crc16_ccitt_true_init_0000(b"") == 0x0000
    assert crc16_dart_ibm_ansi_init_0000(b"") == 0x0000
