"""Tests for offline NOZIO transition analyzer (no serial I/O)."""

from __future__ import annotations

import json
from pathlib import Path

from intelipump_fdc.bench_poll.nozio_transition_cli import (
    CLASS_EXPECTED_N1,
    CLASS_NO_CHANGE,
    classify_transition,
    run,
    summarize_evidence_jsonl,
)
from intelipump_fdc.protocol.dart.line.frame_builder import build_data_frame


def _rx_jsonl(path: Path, nozio_bytes: list[int]) -> None:
    lines: list[str] = []
    for i, nozio in enumerate(nozio_bytes):
        app = bytes((0x03, 0x04, 0x00, 0x11, 0x75, nozio))
        frame = build_data_frame(0x50, i & 0x0F, app)
        lines.append(
            json.dumps(
                {
                    "direction": "RX",
                    "rawHex": frame.hex(" "),
                }
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_expected_nozzle1_transition(tmp_path: Path) -> None:
    inn = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    ret = tmp_path / "ret.jsonl"
    _rx_jsonl(inn, [0x01, 0x01])
    _rx_jsonl(out, [0x11, 0x11])
    _rx_jsonl(ret, [0x01])
    report = classify_transition(
        summarize_evidence_jsonl(inn),
        summarize_evidence_jsonl(out),
        summarize_evidence_jsonl(ret),
    )
    assert report["classification"] is CLASS_EXPECTED_N1
    assert report["captures"]["nozzleHeldOut"]["dominantNozioHex"] == "11"


def test_no_position_change_not_parser_failure(tmp_path: Path) -> None:
    inn = tmp_path / "in.jsonl"
    out = tmp_path / "out.jsonl"
    ret = tmp_path / "ret.jsonl"
    _rx_jsonl(inn, [0x01, 0x01, 0x01])
    _rx_jsonl(out, [0x01, 0x01])
    _rx_jsonl(ret, [0x01])
    report = classify_transition(
        summarize_evidence_jsonl(inn),
        summarize_evidence_jsonl(out),
        summarize_evidence_jsonl(ret),
    )
    assert report["classification"] is CLASS_NO_CHANGE
    assert "not evidence the parser failed" in report["notes"][0]
    assert run(
        [
            "--nozzle-in",
            str(inn),
            "--nozzle-held-out",
            str(out),
            "--nozzle-returned",
            str(ret),
            "--json",
        ]
    ) == 0
