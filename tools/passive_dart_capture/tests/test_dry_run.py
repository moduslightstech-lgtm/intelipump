"""Hardware-free dry-run pipeline tests (no serial devices)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tools.passive_dart_capture.cli import main
from tools.passive_dart_capture.dry_run import (
    DEFAULT_FIXTURE,
    FixtureByteSource,
    load_fixture_chunks,
    parse_hex_fixture,
    run_dry_run,
)
from tools.passive_dart_capture.serial_reader import PassiveSerialReader


def test_default_fixture_exists() -> None:
    assert DEFAULT_FIXTURE.is_file()
    chunks = load_fixture_chunks(DEFAULT_FIXTURE)
    assert len(chunks) >= 8
    assert bytes.fromhex("50 20 FA") in chunks


def test_parse_hex_fixture_comments_and_inline() -> None:
    text = """
    # comment
    50 20 FA  # poll
    50
    20
    FA
    """
    chunks = parse_hex_fixture(text)
    assert chunks == [
        bytes.fromhex("50 20 FA"),
        bytes.fromhex("50"),
        bytes.fromhex("20"),
        bytes.fromhex("FA"),
    ]


def test_fixture_source_refuses_dev_path() -> None:
    src = FixtureByteSource(chunks=[b"\x50"], device_label="/dev/ttyUSB0")
    with pytest.raises(RuntimeError, match="serial path"):
        src.open()


def test_run_dry_run_pipeline(tmp_path: Path) -> None:
    result = run_dry_run(work_dir=tmp_path, session_id="dry-test")
    assert result.evidence_path.is_file()
    assert result.stats_chunks >= 1
    assert result.stats_frames >= 1

    rows = [
        json.loads(line)
        for line in result.evidence_path.read_text(encoding="utf-8").splitlines()
    ]
    types = {r["recordType"] for r in rows}
    assert "serial_chunk" in types
    assert "frame" in types
    assert "operator_marker" in types

    chunks = [r for r in rows if r["recordType"] == "serial_chunk"]
    assert all(c["direction"] == "MERGED_BUS" for c in chunks)
    assert all(c["serialDevice"] == "fixture:dry-run" for c in chunks)
    assert not any("/dev/tty" in str(c["serialDevice"]) for c in chunks)

    frames = [r for r in rows if r["recordType"] == "frame"]
    classes = {f["frameClass"] for f in frames}
    assert "POLL" in classes
    assert "SHORT_ACK" in classes
    assert "DATA" in classes
    assert "DATA_CRC_INVALID" in classes
    assert "PARTIAL_FRAME" in classes

    for fr in frames:
        assert fr["direction"] in {"UNKNOWN", "INFERRED"}
        assert fr["directionInferenceReason"]
        if fr["direction"] == "INFERRED":
            assert fr["directionConfidence"] == "SHAPE_ONLY"

    summary = result.analysis.summary
    assert summary["recordCounts"]["serialChunks"] == len(chunks)
    # serial_chunk must not inflate complete-frame analytics
    assert summary["recordCounts"]["completeFramesAnalyzed"] == len(
        [f for f in frames if f.get("complete") is True]
    )
    assert summary["errors"]["crcInvalidFrames"] >= 1
    assert summary["errors"]["incompleteFrames"] >= 1
    assert result.analysis.report_paths["summary_json"].is_file()


def test_cli_dry_run_no_hardware(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail_open(self: PassiveSerialReader) -> None:
        raise AssertionError("dry-run must not open PassiveSerialReader")

    monkeypatch.setattr(PassiveSerialReader, "open", _fail_open)
    code = main(
        [
            "dry-run",
            "--fixture",
            str(DEFAULT_FIXTURE),
            "--work-dir",
            str(tmp_path / "out"),
            "--session-id",
            "cli-dry",
        ]
    )
    assert code == 0
    assert (tmp_path / "out" / "evidence" / "cli-dry.jsonl").is_file()
    assert (tmp_path / "out" / "reports" / "cli-dry-summary.json").is_file()


def test_module_main_requires_subcommand() -> None:
    # Bare invocation must not start hardware capture (argparse exits).
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code != 0
