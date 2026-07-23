"""Bench evidence JSON + Markdown report generation."""

from __future__ import annotations

import json
import platform
import sys
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

from intelipump_fdc.hardware.models import BenchEvidence


def _controller_version() -> str:
    try:
        return metadata.version("intelipump-fdc")
    except metadata.PackageNotFoundError:
        return "0.1.0-dev"


def environment_snapshot() -> dict[str, str]:
    return {
        "date": datetime.now(UTC).date().isoformat(),
        "pi_model": _pi_model(),
        "os": f"{platform.system()} {platform.release()}",
        "python_version": sys.version.split()[0],
        "controller_version": _controller_version(),
        "machine": platform.machine(),
    }


def _pi_model() -> str:
    model_path = Path("/proc/device-tree/model")
    if model_path.exists():
        try:
            return model_path.read_text(encoding="utf-8", errors="replace").strip("\x00")
        except OSError:
            pass
    return platform.node() or "unknown"


def build_report_dict(evidence: BenchEvidence) -> dict[str, Any]:
    env = environment_snapshot()
    data = evidence.to_dict()
    data.update(env)
    # Never claim physical PASS without adapters.
    if evidence.physical_hil_status != "PASS":
        data["decision"] = evidence.physical_hil_status
    else:
        data["decision"] = "PASS" if evidence.success else "FAIL"
    return data


def write_evidence_json(evidence: BenchEvidence, path: Path | str) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = build_report_dict(evidence)
    out.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return out


def write_markdown_report(evidence: BenchEvidence, path: Path | str) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    data = build_report_dict(evidence)
    lat = data.get("latency") or {}
    ctrl_path = (
        evidence.controller_device.device_path if evidence.controller_device else None
    )
    ctrl_stable = (
        evidence.controller_device.stable_id if evidence.controller_device else None
    )
    sim_path = (
        evidence.simulator_device.device_path if evidence.simulator_device else None
    )
    sim_stable = (
        evidence.simulator_device.stable_id if evidence.simulator_device else None
    )
    lines = [
        "# RS-485 Office Bench Report",
        "",
        f"- **date**: {data.get('date')}",
        f"- **bench_name**: {evidence.bench.name}",
        f"- **physical_hil_status**: `{evidence.physical_hil_status}`",
        f"- **decision**: `{data.get('decision')}`",
        f"- **pi_model**: {data.get('pi_model')}",
        f"- **os**: {data.get('os')}",
        f"- **python_version**: {data.get('python_version')}",
        f"- **controller_version**: {data.get('controller_version')}",
        f"- **environment**: {evidence.bench.environment}",
        f"- **listen_only**: {evidence.listen_only}",
        f"- **active_commands_enabled**: {evidence.active_commands_enabled}",
        "",
        "## Adapters",
        "",
        f"- **controller**: `{ctrl_path}` (stable=`{ctrl_stable}`)",
        f"- **simulator**: `{sim_path}` (stable=`{sim_stable}`)",
        "",
        "## Wiring / termination",
        "",
        f"- automatic_direction_control: {evidence.bench.automatic_direction_control}",
        f"- termination_enabled: {evidence.bench.termination_enabled}",
        f"- bias_enabled: {evidence.bench.bias_enabled}",
        f"- ground_reference_connected: {evidence.bench.ground_reference_connected}",
        "",
        "## Serial settings",
        "",
        f"- baud: {evidence.bench.baud_rate}",
        f"- parity: {evidence.bench.parity}",
        f"- data_bits: {evidence.bench.data_bits}",
        f"- stop_bits: {evidence.bench.stop_bits}",
        f"- protocol_target_ms: {evidence.bench.protocol_target_ms}",
        f"- configured_bench_timeout_ms: {evidence.bench.response_timeout_ms}",
        f"- duration_s: {evidence.bench.duration_s}",
        f"- addresses: {list(evidence.bench.addresses)}",
        "",
        "## Counters",
        "",
        f"- poll_count: {evidence.poll_count}",
        f"- data_count: {evidence.data_count}",
        f"- eot_count: {evidence.eot_count}",
        f"- ack_count: {evidence.ack_count}",
        f"- nak_count: {evidence.nak_count}",
        f"- crc_error_count: {evidence.crc_error_count}",
        f"- timeout_count: {evidence.timeout_count}",
        f"- retry_count: {evidence.retry_count}",
        f"- reconnect_events: {evidence.reconnect_events}",
        "",
        "## Latency",
        "",
        f"- count: {lat.get('count')}",
        f"- min_ms: {lat.get('minimum_ms')}",
        f"- max_ms: {lat.get('maximum_ms')}",
        f"- mean_ms: {lat.get('mean_ms')}",
        f"- p50_ms: {lat.get('p50_ms')}",
        f"- p95_ms: {lat.get('p95_ms')}",
        f"- p99_ms: {lat.get('p99_ms')}",
        f"- jitter_ms: {lat.get('jitter_ms')}",
        f"- timeout_count: {lat.get('timeout_count')}",
        "",
        "## Fault results",
        "",
    ]
    if evidence.fault_scenarios:
        lines.extend(f"- {f}" for f in evidence.fault_scenarios)
    else:
        lines.append("- (none recorded)")
    lines.extend(
        [
            "",
            "## Capture files",
            "",
            f"- capture_jsonl: `{evidence.capture_path}`",
            f"- sanitized_capture: `{evidence.sanitized_capture_path}`",
            f"- evidence_json: `{evidence.evidence_json_path}`",
            "",
            "## Notes / unresolved observations",
            "",
        ]
    )
    if evidence.notes:
        lines.extend(f"- {n}" for n in evidence.notes)
    else:
        lines.append("- (none)")
    if evidence.physical_hil_status == "NOT_RUN":
        lines.extend(
            [
                "",
                "> **physical_hil_status = NOT_RUN** — no physical USB-RS485 adapters "
                "were exercised in this run. Do not treat this as PASS.",
            ]
        )
    lines.append("")
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


# Backward-compatible name used by earlier Phase 10 code.
def write_evidence(evidence: BenchEvidence, path: Path | str) -> Path:
    return write_evidence_json(evidence, path)
