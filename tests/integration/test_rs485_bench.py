"""Optional physical two-adapter RS-485 bench tests."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from intelipump_fdc.hardware.bench_runner import run_rs485_bench
from intelipump_fdc.hardware.models import BenchConfig


def _ports_configured() -> tuple[str, str] | None:
    ctrl = os.environ.get("INTELIPUMP_BENCH__CONTROLLER_PORT", "").strip()
    sim = os.environ.get("INTELIPUMP_BENCH__SIMULATOR_PORT", "").strip()
    if not ctrl or not sim:
        return None
    if not Path(ctrl).exists() or not Path(sim).exists():
        return None
    return ctrl, sim


pytestmark = [
    pytest.mark.rs485_bench,
    pytest.mark.skipif(
        _ports_configured() is None,
        reason=(
            "Set INTELIPUMP_BENCH__CONTROLLER_PORT and "
            "INTELIPUMP_BENCH__SIMULATOR_PORT to existing adapters"
        ),
    ),
]


@pytest.mark.asyncio
async def test_physical_two_adapter_bench(tmp_path: Path) -> None:
    ports = _ports_configured()
    assert ports is not None
    ctrl, sim = ports
    evidence = await run_rs485_bench(
        BenchConfig(
            name="pytest-rs485",
            environment="LAB",
            controller_port=ctrl,
            simulator_port=sim,
            response_timeout_ms=100,
            protocol_target_ms=25,
            duration_s=5.0,
            addresses=(1, 2),
        ),
        evidence_path=tmp_path / "evidence.json",
    )
    assert evidence.listen_only is True
    assert evidence.active_commands_enabled is False
    assert evidence.poll_count > 0
    assert evidence.success is True
