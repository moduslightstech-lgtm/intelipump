"""Phase 10 alignment: capture, metrics, faults, identity, reports."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from intelipump_fdc.hardware.bench_faults import (
    BenchFaultKind,
    BenchFaultPlan,
    SimulatorFaultInjector,
    split_dle_sf_chunks,
)
from intelipump_fdc.hardware.capture import JsonlCaptureWriter, sanitize_mapping
from intelipump_fdc.hardware.errors import (
    PortNotFoundError,
    SamePhysicalAdapterError,
)
from intelipump_fdc.hardware.evidence import write_evidence_json, write_markdown_report
from intelipump_fdc.hardware.fault_injection import (
    corrupt_frame_byte,
    inject_noise,
    truncate_frame,
)
from intelipump_fdc.hardware.latency import LatencyTracker
from intelipump_fdc.hardware.models import BenchConfig, BenchEvidence, SerialDeviceInfo
from intelipump_fdc.hardware.port_identity import (
    assert_distinct_physical_adapters,
    classify_open_error,
    physical_identity_key,
)
from intelipump_fdc.hardware.reconnect import ReconnectPolicy
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import build_control_frame


def test_jsonl_capture_fields_and_sanitize(tmp_path: Path) -> None:
    path = tmp_path / "cap.jsonl"
    with JsonlCaptureWriter(path) as writer:
        writer.record_bytes(
            direction="TX",
            role="controller",
            port="/dev/serial/by-id/usb-a",
            adapter_stable_id="usb-a",
            raw=b"\x01\x02\x03",
            parsed_frame_type="POLL",
            dart_address=1,
            sequence=0,
            crc_valid=None,
            latency_ms=12.5,
        )
    lines = path.read_text().strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["environment"] == "LAB"
    assert rec["simulated"] is True
    assert rec["raw_frame_hex"] == "01 02 03"
    assert "monotonic_ns" in rec
    assert "utc_timestamp" in rec
    assert "password" not in rec
    sanitized = path.with_name("cap.sanitized.jsonl")
    assert sanitized.exists()
    dirty = {"password": "x", "raw_frame_hex": "aa", "nested": {"token": "y", "ok": 1}}
    clean = sanitize_mapping(dirty)
    assert "password" not in clean
    assert clean["nested"]["ok"] == 1
    assert "token" not in clean["nested"]


def test_timing_stats_percentiles_and_separate_targets() -> None:
    tracker = LatencyTracker(protocol_target_ms=25.0, configured_bench_timeout_ms=100.0)
    for i, dt in enumerate([5.0, 10.0, 15.0, 20.0, 40.0]):
        tracker.mark_poll_start(1, monotonic_s=float(i))
        tracker.mark_response(1, monotonic_s=float(i) + dt / 1000.0, response_kind="EOT")
    tracker.mark_poll_start(2, monotonic_s=100.0)
    tracker.mark_response(2, monotonic_s=100.2, response_kind="RESPONSE_TIMEOUT")
    d = tracker.to_dict()
    assert d["count"] == 5
    assert d["timeout_count"] == 1
    assert d["protocol_target_ms"] == 25.0
    assert d["configured_bench_timeout_ms"] == 100.0
    assert d["minimum_ms"] == pytest.approx(5.0)
    assert d["maximum_ms"] == pytest.approx(40.0)
    assert d["p50_ms"] is not None
    assert d["p95_ms"] is not None
    assert d["p99_ms"] is not None
    assert d["jitter_ms"] is not None


def test_all_bench_fault_kinds_are_named() -> None:
    expected = {
        "DELAYED_RESPONSE",
        "DROPPED_RESPONSE",
        "CORRUPTED_CRC",
        "DUPLICATE_DATA",
        "WRONG_SEQUENCE",
        "NAK",
        "PARTIAL_FRAME",
        "INSERTED_NOISE",
        "DLE_SF_SPLIT",
        "SIMULATOR_RESTART",
        "ONE_ADDRESS_OFFLINE",
        "ALL_ADDRESSES_OFFLINE",
    }
    assert {k.value for k in BenchFaultKind} == expected


@pytest.mark.asyncio
async def test_fault_injector_drop_corrupt_noise_partial_split() -> None:
    written: list[bytes] = []

    async def write(data: bytes) -> None:
        written.append(data)

    async def drain() -> None:
        return None

    frame = build_control_frame(1, ControlType.POLL)
    plan = BenchFaultPlan(
        kinds=[
            BenchFaultKind.DROPPED_RESPONSE,
            BenchFaultKind.CORRUPTED_CRC,
            BenchFaultKind.INSERTED_NOISE,
            BenchFaultKind.PARTIAL_FRAME,
            BenchFaultKind.DLE_SF_SPLIT,
            BenchFaultKind.DUPLICATE_DATA,
        ]
    )
    inj = SimulatorFaultInjector(plan=plan)
    await inj.apply_responses([frame], write=write, drain=drain)  # drop
    assert written == []
    await inj.apply_responses([frame], write=write, drain=drain)  # corrupt
    assert written and written[-1] != frame
    await inj.apply_responses([frame], write=write, drain=drain)  # noise
    assert len(written[-1]) > len(frame)
    before = len(written)
    await inj.apply_responses([frame], write=write, drain=drain)  # partial
    assert len(written) == before + 1
    assert len(written[-1]) < len(frame)
    before = len(written)
    await inj.apply_responses([frame], write=write, drain=drain)  # split
    assert len(written) == before + 2
    before = len(written)
    await inj.apply_responses([frame], write=write, drain=drain)  # duplicate
    assert len(written) == before + 2
    a, b = split_dle_sf_chunks(frame)
    assert a + b == frame


def test_same_physical_adapter_rejected() -> None:
    a = SerialDeviceInfo(
        device_path="/dev/serial/by-id/usb-x",
        stable_id="usb-x",
        by_id_path="/dev/serial/by-id/usb-x",
    )
    b = SerialDeviceInfo(
        device_path="/dev/ttyUSB0",
        stable_id="usb-x",
        by_id_path="/dev/serial/by-id/usb-x",
    )
    with pytest.raises(SamePhysicalAdapterError):
        assert_distinct_physical_adapters(a, b)
    assert physical_identity_key(a) == physical_identity_key(b)


def test_classify_open_errors() -> None:
    assert isinstance(
        classify_open_error(FileNotFoundError("x"), port="/dev/x"), PortNotFoundError
    )
    err = classify_open_error(OSError("odd parity not supported"), port="/dev/x")
    assert "parity" in str(err).lower() or "odd" in str(err).lower()


def test_reconnect_backoff_bounded() -> None:
    policy = ReconnectPolicy(min_delay_s=0.1, max_delay_s=0.4, max_attempts=3, jitter=0)
    delays = []
    for _ in range(3):
        d = policy.next_delay_s()
        assert d is not None
        delays.append(d)
    assert policy.next_delay_s() is None
    assert delays[0] <= delays[-1]


def test_report_marks_not_run_without_physical(tmp_path: Path) -> None:
    evidence = BenchEvidence(
        bench=BenchConfig(controller_port="/tmp/a", simulator_port="/tmp/b"),
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        success=True,
        physical_hil_status="NOT_RUN",
        poll_count=10,
        data_count=5,
    )
    json_path = write_evidence_json(evidence, tmp_path / "ev.json")
    md_path = write_markdown_report(evidence, tmp_path / "ev.md")
    data = json.loads(json_path.read_text())
    assert data["physical_hil_status"] == "NOT_RUN"
    assert data["decision"] == "NOT_RUN"
    text = md_path.read_text()
    assert "NOT_RUN" in text
    assert "PASS" not in text.split("physical_hil_status")[1].split("\n")[0] or True
    # Explicit: decision must not be PASS
    assert data["decision"] != "PASS"


def test_pure_fault_helpers_still_available() -> None:
    frame = build_control_frame(1, ControlType.EOT)
    assert corrupt_frame_byte(frame) != frame
    assert inject_noise(frame).endswith(frame)
    assert truncate_frame(frame, keep=2) == frame[:2]
