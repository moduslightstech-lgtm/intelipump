"""Controller run-duration CLI and loop behavior."""

from __future__ import annotations

import asyncio

import pytest

from intelipump_fdc.controller.cli import build_parser, resolve_duration
from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair


def test_resolve_duration_omit_means_continuous() -> None:
    assert resolve_duration(None) is None


def test_resolve_duration_positive() -> None:
    assert resolve_duration(30.0) == 30.0
    assert resolve_duration(0.5) == 0.5


def test_resolve_duration_rejects_zero() -> None:
    with pytest.raises(ValueError, match="omit --duration"):
        resolve_duration(0.0)


def test_resolve_duration_rejects_negative() -> None:
    with pytest.raises(ValueError, match="negative"):
        resolve_duration(-1.0)


def test_cli_parser_duration_default_none() -> None:
    parser = build_parser()
    args = parser.parse_args([])
    assert args.duration is None
    duration_action = parser._option_string_actions["--duration"]
    assert duration_action.help is not None
    assert "Omit to run continuously" in duration_action.help


def test_cli_parser_duration_positive() -> None:
    parser = build_parser()
    args = parser.parse_args(["--duration", "12.5"])
    assert args.duration == 12.5


def test_cli_parser_log_all_frames_default_off() -> None:
    parser = build_parser()
    args = parser.parse_args([])
    assert args.log_all_frames is False
    verbose = parser.parse_args(["--log-all-frames"])
    assert verbose.log_all_frames is True


@pytest.mark.asyncio
async def test_controller_loop_timed_run_closes_transport() -> None:
    a, _b = create_memory_transport_pair()
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=20,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
        ),
    )
    loop = ControllerLoop(runtime)
    await loop.run(duration_s=0.15)
    assert not a.is_open


@pytest.mark.asyncio
async def test_controller_loop_continuous_stops_on_request_stop() -> None:
    a, _b = create_memory_transport_pair()
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=20,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
        ),
    )
    loop = ControllerLoop(runtime)

    async def stop_soon() -> None:
        await asyncio.sleep(0.05)
        loop.request_stop()

    await asyncio.gather(loop.run(duration_s=None), stop_soon())
    assert not a.is_open


@pytest.mark.asyncio
async def test_controller_loop_rejects_invalid_duration() -> None:
    a, _b = create_memory_transport_pair()
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(addresses=(1,)),
    )
    loop = ControllerLoop(runtime)
    with pytest.raises(ValueError, match="continuously"):
        await loop.run(duration_s=0)
    with pytest.raises(ValueError, match="negative"):
        await loop.run(duration_s=-5)


@pytest.mark.asyncio
async def test_controller_loop_sigterm_style_cancellation_closes_transport() -> None:
    """SIGTERM maps to request_stop in the CLI; ensure that path shuts down cleanly."""
    a, _b = create_memory_transport_pair()
    runtime = ControllerRuntime(
        transport=a,
        safety=default_lab_safety(),
        config=PollSchedulerConfig(
            addresses=(1,),
            response_timeout_ms=20,
            inter_poll_delay_ms=1,
            idle_sleep_ms=1,
            max_retries=0,
        ),
    )
    loop = ControllerLoop(runtime)
    task = asyncio.create_task(loop.run(duration_s=None))
    await asyncio.sleep(0.05)
    loop.request_stop()  # same as SIGTERM handler
    await asyncio.wait_for(task, timeout=2.0)
    assert not a.is_open
    assert runtime.safety.mode.value == "LISTEN_ONLY"
    assert runtime.safety.active_commands_enabled is False
