"""Offline decode tests for passive captures (no serial I/O)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from intelipump_fdc.capture.decode_offline import decode_capture_file, load_capture_rx_bytes
from intelipump_fdc.capture.report import (
    PassiveConclusion,
    suggest_conclusion,
    write_validation_report,
)
from intelipump_fdc.capture.schema import (
    SCHEMA_VERSION,
    CaptureEvent,
    SerialCaptureState,
    make_event_record,
    make_rx_record,
)
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.frame_builder import (
    build_data_frame,
    build_eot,
    build_poll,
)
from intelipump_fdc.simulator.encoding import encode_dc1_status


def _write_capture(path: Path, chunks: list[bytes]) -> str:
    capture_id = "test-capture-001"
    lines: list[str] = [
        make_event_record(
            capture_id=capture_id,
            port="/dev/fake",
            baud=9600,
            event=CaptureEvent.CAPTURE_STARTED,
            monotonic_ns=1,
            serial_state=SerialCaptureState.OPEN,
            notes="PASSIVE_CAPTURE_ONLY",
        ).to_json_line()
    ]
    for i, raw in enumerate(chunks, start=1):
        lines.append(
            make_rx_record(
                capture_id=capture_id,
                port="/dev/fake",
                baud=9600,
                raw=raw,
                monotonic_ns=i * 1000,
                chunk_sequence=i,
                idle_gap_ms=None,
                serial_state=SerialCaptureState.OPEN,
            ).to_json_line()
        )
    lines.append(
        make_event_record(
            capture_id=capture_id,
            port="/dev/fake",
            baud=9600,
            event=CaptureEvent.CAPTURE_STOPPED,
            monotonic_ns=999999,
            serial_state=SerialCaptureState.CLOSED,
        ).to_json_line()
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return capture_id


def test_offline_decoder_never_opens_serial(tmp_path: Path) -> None:
    path = tmp_path / "c.jsonl"
    _write_capture(path, [build_eot(encode_wire_address(1), 0)])

    with (
        patch(
            "intelipump_fdc.protocol.dart.transport.serial.SerialTransport.open",
            side_effect=AssertionError("must not open serial"),
        ),
        patch(
            "intelipump_fdc.capture.receive_only.ReceiveOnlySerialSource.open",
            side_effect=AssertionError("must not open receive-only serial"),
        ),
    ):
        result = decode_capture_file(path)

    assert result.total_rx_bytes > 0
    assert result.control_frame_count >= 1


def test_undecoded_bytes_retained(tmp_path: Path) -> None:
    path = tmp_path / "noise.jsonl"
    # Trailing incomplete frame bytes after a valid EOT.
    _write_capture(path, [build_eot(encode_wire_address(2), 1), b"\x01\x10"])
    result = decode_capture_file(path)
    assert result.undecoded_trailing_hex == "01 10"
    assert result.total_rx_bytes == len(build_eot(encode_wire_address(2), 1)) + 2


def test_crc_valid_and_invalid_reported(tmp_path: Path) -> None:
    path = tmp_path / "crc.jsonl"
    good = build_data_frame(encode_wire_address(1), 0, encode_dc1_status(1))
    bad = bytearray(good)
    # Flip a payload/crc byte while keeping SF terminator.
    bad[-3] ^= 0xFF
    _write_capture(path, [bytes(bad), good])
    result = decode_capture_file(path)
    assert result.crc_invalid_count >= 1 or result.rejected_count >= 1
    # At least one CRC-valid DATA expected from the good frame.
    assert result.crc_valid_count >= 1
    uncertain = [f for f in result.candidate_frames if f.certainty == "uncertain"]
    assert uncertain  # invalid/uncertain labeled


def test_possible_addresses_and_uncertain_labels(tmp_path: Path) -> None:
    path = tmp_path / "addr.jsonl"
    _write_capture(
        path,
        [
            build_poll(1),
            build_eot(encode_wire_address(1), 0),
            build_eot(encode_wire_address(2), 0),
        ],
    )
    result = decode_capture_file(path)
    assert 1 in result.possible_addresses
    assert 2 in result.possible_addresses
    assert any("candidate" in n for f in result.candidate_frames for n in f.notes)


def test_load_rejects_non_rx_direction(tmp_path: Path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text(
        json.dumps(
            {
                "schemaVersion": SCHEMA_VERSION,
                "captureId": "x",
                "recordType": "rx_chunk",
                "timestampUtc": "2026-01-01T00:00:00+00:00",
                "monotonicNs": 1,
                "port": "/dev/x",
                "baud": 9600,
                "direction": "TX",
                "rawHex": "01",
                "byteCount": 1,
                "idleGapMs": None,
                "chunkSequence": 1,
                "serialState": "open",
                "notes": None,
                "event": None,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="non-RX"):
        load_capture_rx_bytes(path)


def test_validation_report_defaults_inconclusive(tmp_path: Path) -> None:
    path = tmp_path / "rep.jsonl"
    _write_capture(path, [build_eot(encode_wire_address(1), 0)])
    report = tmp_path / "test-capture-001" / "validation-report.md"
    result = write_validation_report(path, report, duration_s=300.0)
    text = report.read_text(encoding="utf-8")
    assert "PASSIVE_CAPTURE_INCONCLUSIVE" in text
    assert "Candidate frames" in text
    assert suggest_conclusion(result) is PassiveConclusion.INCONCLUSIVE
    assert (
        suggest_conclusion(result, operator_interference=True)
        is PassiveConclusion.FAIL
    )


def test_no_authorization_objects_in_decode(tmp_path: Path) -> None:
    path = tmp_path / "auth.jsonl"
    _write_capture(path, [build_data_frame(encode_wire_address(1), 0, encode_dc1_status(1))])
    result = decode_capture_file(path)
    blob = json.dumps(result.to_dict())
    assert "PumpCommand" not in blob
    assert "OutboundDataItem" not in blob
    assert "authorize_pump" not in blob
