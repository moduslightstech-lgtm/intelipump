"""Evidence JSONL records, markers, and timestamp ordering."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from tools.passive_dart_capture.capture import PassiveCaptureSession
from tools.passive_dart_capture.evidence_writer import (
    EvidenceWriter,
    make_serial_chunk_record,
)
from tools.passive_dart_capture.markers import (
    OperatorMarker,
    append_marker,
    make_marker_record,
    parse_marker_name,
)
from tools.passive_dart_capture.serial_reader import SerialChunk


def test_all_operator_markers_parse() -> None:
    for m in OperatorMarker:
        assert parse_marker_name(m.value) is m


def test_serial_chunk_record_fields() -> None:
    rec = make_serial_chunk_record(
        session_id="s1",
        chunk_sequence=0,
        data=bytes.fromhex("50 20 FA"),
        capture_timestamp_utc=datetime(2026, 8, 1, tzinfo=UTC),
        monotonic_timestamp_ns=123,
        serial_device="/dev/ttyUSB0",
        baud=9600,
        parity="ODD",
        stop_bits=1,
    )
    assert rec["recordType"] == "serial_chunk"
    assert rec["direction"] == "MERGED_BUS"
    assert rec["source"] == "EPUMP_PASSIVE_CAPTURE"
    assert rec["byteCount"] == 3
    assert rec["rawHex"] == "50 20 FA"
    assert rec["rawBase64"]


def test_marker_append_and_timestamps(tmp_path: Path) -> None:
    sid = "marker-session"
    r1 = append_marker(sid, "STARTUP", evidence_dir=tmp_path)
    r2 = append_marker(sid, "NOZZLE_LIFTED", evidence_dir=tmp_path, note="side1")
    path = tmp_path / f"{sid}.jsonl"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert r1["marker"] == "STARTUP"
    assert r2["note"] == "side1"
    assert r1["monotonicTimestampNs"] <= r2["monotonicTimestampNs"]


def test_capture_timestamp_ordering(tmp_path: Path) -> None:
    session = PassiveCaptureSession(session_id="ts-order", evidence_dir=tmp_path)

    def chunk(data: bytes, mono: int) -> SerialChunk:
        return SerialChunk(
            data=data,
            capture_timestamp_utc=datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC),
            monotonic_timestamp_ns=mono,
            serial_device="/dev/ttyUSB0",
            baud=9600,
            parity="ODD",
            stop_bits=1,
        )

    session.process_chunk_for_tests(chunk(bytes.fromhex("50 20 FA"), 100))
    session.process_chunk_for_tests(chunk(bytes.fromhex("51 20 FA"), 200))
    session.flush_and_stop_for_tests()
    rows = [
        json.loads(line)
        for line in session.evidence_path.read_text(encoding="utf-8").splitlines()
    ]
    chunks = [r for r in rows if r["recordType"] == "serial_chunk"]
    assert [c["chunkSequence"] for c in chunks] == [0, 1]
    assert chunks[0]["monotonicTimestampNs"] < chunks[1]["monotonicTimestampNs"]
    frames = [r for r in rows if r["recordType"] == "frame"]
    assert [f["frameSequence"] for f in frames] == [0, 1]


def test_evidence_writer_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "e.jsonl"
    with EvidenceWriter(path) as w:
        w.write_record(make_marker_record(session_id="x", marker="STARTUP"))
        w.write_record({"recordType": "frame", "sessionId": "x", "frameSequence": 0})
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["marker"] == "STARTUP"
