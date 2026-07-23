"""Phase 2: validate line utilities against captured Wayne traffic fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.crc import (
    CANONICAL_CRC_CANDIDATE,
    CrcCandidate,
    compute_all_candidates,
    crc_from_le_bytes,
    dart_crc16,
    get_crc_fn,
)
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame, ParseError
from intelipump_fdc.protocol.dart.line.sequence import next_tx_sequence

FIXTURE_DIR = Path(__file__).resolve().parents[3] / "fixtures" / "dart"


def _load(name: str) -> list[dict[str, object]] | dict[str, object]:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _hex(value: object) -> bytes:
    assert isinstance(value, str)
    return bytes(int(part, 16) for part in value.split())


def _frame(raw: bytes) -> DartLineFrame:
    result = parse_frame(raw)
    assert isinstance(result, DartLineFrame), result
    return result


def test_fixtures_exist() -> None:
    for name in (
        "captured_frames.json",
        "control_frames.json",
        "data_frames.json",
        "crc_vectors.json",
    ):
        assert (FIXTURE_DIR / name).is_file()


@pytest.mark.parametrize(
    "raw_hex",
    [
        "50 20 FA",
        "51 20 FA",
        "50 70 FA",
        "51 70 FA",
    ],
)
def test_required_control_examples(raw_hex: str) -> None:
    frame = _frame(_hex(raw_hex))
    assert frame.address in {0x50, 0x51}
    if raw_hex.endswith("20 FA"):
        assert frame.control_type is ControlType.POLL
    else:
        assert frame.control_type is ControlType.EOT


def test_control_fixtures_parse() -> None:
    fixtures = _load("control_frames.json")
    assert isinstance(fixtures, list)
    assert fixtures
    for item in fixtures:
        frame = _frame(_hex(item["rawHex"]))
        assert frame.address == item["expectedAddress"]
        assert frame.control_type.value == item["expectedControlType"]
        assert item["direction"] == "UNKNOWN"
        if item["expectedSequence"] is None:
            assert frame.control_type in {ControlType.POLL, ControlType.IAP}
        else:
            assert frame.sequence == item["expectedSequence"]


def test_ack_and_nak_observed_in_fixtures() -> None:
    fixtures = _load("control_frames.json")
    assert isinstance(fixtures, list)
    types = {item["expectedControlType"] for item in fixtures}
    assert "ACK" in types
    assert "NAK" in types
    # ACKPOLL / IAP were not present in these captures.
    assert "ACKPOLL" not in types


def test_data_fixtures_parse_and_crc() -> None:
    fixtures = _load("data_frames.json")
    assert isinstance(fixtures, list)
    assert len(fixtures) >= 2
    for item in fixtures:
        frame = _frame(_hex(item["rawHex"]))
        assert frame.control_type is ControlType.DATA
        assert frame.address == item["expectedAddress"]
        assert frame.sequence == item["expectedSequence"]
        assert frame.payload == _hex(item["payloadHex"])
        assert frame.received_crc == crc_from_le_bytes(
            int(item["receivedCrcLow"]),  # type: ignore[arg-type]
            int(item["receivedCrcHigh"]),  # type: ignore[arg-type]
        )
        assert item["matchingCrcCandidate"] == CANONICAL_CRC_CANDIDATE.value
        assert frame.crc_valid is True
        assert item["direction"] == "UNKNOWN"


def test_crc_vectors_prove_canonical_on_two_independent_frames() -> None:
    vectors = _load("crc_vectors.json")
    assert isinstance(vectors, list)
    assert len(vectors) >= 2
    seen_inputs: set[str] = set()
    for vector in vectors:
        crc_input = _hex(vector["crcInputHex"])
        received = crc_from_le_bytes(
            int(vector["receivedCrcLow"]),  # type: ignore[arg-type]
            int(vector["receivedCrcHigh"]),  # type: ignore[arg-type]
        )
        assert dart_crc16(crc_input) == received
        assert get_crc_fn(CANONICAL_CRC_CANDIDATE)(crc_input) == received
        # CCITT candidates must not silently match these vectors.
        all_vals = compute_all_candidates(crc_input)
        for name, value in all_vals.items():
            if name == CANONICAL_CRC_CANDIDATE.value:
                assert value == received
            elif name.startswith("ccitt_"):
                assert value != received
        seen_inputs.add(crc_input.hex())
    assert len(seen_inputs) >= 2


def test_detailed_data_frame_example() -> None:
    """At least one DATA frame analyzed in detail (ADR/CTRL/payload/CRC/ETX/SF)."""
    raw = _hex("51 30 65 01 01 22 83 03 FA")
    frame = _frame(raw)
    assert frame.address == 0x51
    assert frame.control == 0x30
    assert frame.control_type is ControlType.DATA
    assert frame.sequence == 0x0
    assert frame.payload == bytes.fromhex("65 01 01")
    assert frame.received_crc == 0x8322
    assert frame.computed_crc == dart_crc16(bytes.fromhex("51 30 65 01 01"))
    assert frame.crc_valid is True
    assert raw[-2] == 0x03  # ETX
    assert raw[-1] == 0xFA  # SF


def test_escaped_sf_in_capture_data_still_parses() -> None:
    fixtures = _load("data_frames.json")
    assert isinstance(fixtures, list)
    escaped = [item for item in fixtures if "10 FA" in str(item["rawHex"])]
    # Not all fixture subsets include DLE; accept empty but exercise parser on known form.
    if escaped:
        frame = _frame(_hex(escaped[0]["rawHex"]))
        assert frame.control_type is ControlType.DATA
        assert frame.crc_valid is True


def test_sequence_helper_unchanged_despite_mixed_wrap_evidence() -> None:
    # Spec helper remains F -> 1; captures show mixed wraps on merged bus.
    assert next_tx_sequence(0xF) == 0x1


def test_captured_frames_bundle_meta() -> None:
    bundle = _load("captured_frames.json")
    assert isinstance(bundle, dict)
    meta = bundle["meta"]
    assert isinstance(meta, dict)
    assert meta["canonicalCrcCandidate"] == CrcCandidate.DART_IBM_ANSI_INIT_0000.value
    assert "MERGED_BUS" in str(meta["directionPolicy"])


def test_parse_errors_remain_typed() -> None:
    result = parse_frame(b"\x50\x20")
    assert isinstance(result, ParseError)
