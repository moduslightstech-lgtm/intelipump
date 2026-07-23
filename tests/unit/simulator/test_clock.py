"""Simulator clock unit tests."""

from __future__ import annotations

from intelipump_fdc.simulator.clock import SimulatedClock


def test_advance_and_now() -> None:
    clock = SimulatedClock()
    assert clock.now() == 0
    clock.advance(150)
    assert clock.now() == 150


def test_schedule_and_run_until_idle() -> None:
    clock = SimulatedClock()
    seen: list[int] = []
    clock.schedule(100, lambda: seen.append(clock.now()))
    clock.schedule(50, lambda: seen.append(clock.now()))
    advanced = clock.run_until_idle()
    assert advanced == 100
    assert seen == [50, 100]
    assert clock.pending_count == 0


def test_advance_fires_due_callbacks() -> None:
    clock = SimulatedClock()
    fired = []
    clock.schedule(10, lambda: fired.append("a"))
    clock.advance(5)
    assert fired == []
    clock.advance(5)
    assert fired == ["a"]
