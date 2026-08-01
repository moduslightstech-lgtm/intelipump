"""Offline analyzer: dedupe, reports, transitions."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from tools.passive_dart_capture.analyzer import (
    analyze_path,
    compare_sessions,
    dedupe_frames,
    load_session,
)
from tools.passive_dart_capture.capture import PassiveCaptureSession
from tools.passive_dart_capture.evidence_writer import EvidenceWriter
from tools.passive_dart_capture.markers import make_marker_record
from tools.passive_dart_capture.serial_reader import SerialChunk

from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame


def _chunk(data: bytes, mono: int) -> SerialChunk:
    return SerialChunk(
        data=data,
        capture_timestamp_utc=datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC),
        monotonic_timestamp_ns=mono,
        serial_device="/dev/ttyUSB0",
        baud=9600,
        parity="ODD",
        stop_bits=1,
    )


def _build_session(tmp_path: Path, session_id: str) -> Path:
    session = PassiveCaptureSession(session_id=session_id, evidence_dir=tmp_path)
    # Poll then DC1 FILLING then DC3 NOZIO OUT then DC1 FILLING_COMPLETED + NOZIO IN
    mono = 1000
    session.process_chunk_for_tests(_chunk(bytes.fromhex("50 20 FA"), mono))
    mono += 100
    dc1_fill = build_data_frame(0x50, 0x0, bytes.fromhex("01 01 04"))
    session.process_chunk_for_tests(_chunk(dc1_fill, mono))
    mono += 100
    dc3_out = build_data_frame(0x50, 0x1, bytes.fromhex("03 04 00 11 75 11"))
    session.process_chunk_for_tests(_chunk(dc3_out, mono))
    mono += 100
    dc1_done = build_data_frame(0x50, 0x2, bytes.fromhex("01 01 05"))
    dc3_in = build_data_frame(0x50, 0x3, bytes.fromhex("03 04 00 11 75 01"))
    session.process_chunk_for_tests(_chunk(dc1_done + dc3_in, mono))
    # Unknown transaction
    mono += 100
    unknown = build_data_frame(0x50, 0x4, bytes.fromhex("FF 02 AA BB"))
    session.process_chunk_for_tests(_chunk(unknown, mono))
    session.flush_and_stop_for_tests()
    # Operator markers with earlier monotonic for latency tests
    with EvidenceWriter(session.evidence_path) as w:
        w.write_record(
            make_marker_record(
                session_id=session_id,
                marker="FILLING_OBSERVED",
                monotonic_ns=1050,
            )
        )
        w.write_record(
            make_marker_record(
                session_id=session_id,
                marker="NOZZLE_LIFTED",
                monotonic_ns=1150,
            )
        )
    return session.evidence_path


def test_dedupe_by_session_and_frame_sequence() -> None:
    frames = [
        {"sessionId": "a", "frameSequence": 0, "complete": True},
        {"sessionId": "a", "frameSequence": 0, "complete": True},
        {"sessionId": "a", "frameSequence": 1, "complete": True},
        {"sessionId": "b", "frameSequence": 0, "complete": True},
    ]
    out = dedupe_frames(frames)
    assert len(out) == 3
    assert [f["frameSequence"] for f in out] == [0, 1, 0]


def test_analyze_reports(tmp_path: Path) -> None:
    evidence = _build_session(tmp_path, "an-1")
    reports = tmp_path / "reports"
    result = analyze_path(evidence, reports_dir=reports, session_id="an-1")
    assert result.summary["recordCounts"]["completeFramesAnalyzed"] >= 1
    assert result.status_transitions
    assert any(t["toStatus"] == "FILLING_COMPLETED" for t in result.status_transitions)
    assert result.nozio_transitions
    # First NOZIO OUT establishes baseline; transition recorded on return IN.
    assert any(
        t["fromPosition"] == "OUT" and t["toPosition"] == "IN"
        for t in result.nozio_transitions
    )
    assert result.unknown_transactions
    for key in (
        "timeline_csv",
        "timeline_md",
        "status_csv",
        "nozio_csv",
        "unknown_csv",
        "summary_json",
    ):
        assert result.report_paths[key].is_file()
    summary = json.loads(result.report_paths["summary_json"].read_text(encoding="utf-8"))
    assert "50" in summary["perAddress"]
    # Analytics must not use serial_chunks as frames
    loaded = load_session(evidence)
    assert len(loaded.complete_frames) == result.summary["recordCounts"]["completeFramesAnalyzed"]


def test_compare_sessions(tmp_path: Path) -> None:
    a = _build_session(tmp_path, "cmp-a")
    b = _build_session(tmp_path, "cmp-b")
    reports = tmp_path / "reports"
    comparison = compare_sessions(a, b, reports_dir=reports)
    assert comparison["sessionA"] == "cmp-a"
    assert comparison["sessionB"] == "cmp-b"
    assert Path(comparison["reportPath"]).is_file()
