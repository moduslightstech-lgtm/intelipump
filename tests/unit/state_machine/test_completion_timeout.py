"""Unit tests for awaiting-filling-complete timeout evaluation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.completion_timeout import (
    evaluate_awaiting_filling_complete_timeout,
)
from intelipump_fdc.state_machine.models import PumpContext


def _ctx(**kwargs: object) -> PumpContext:
    base: dict[str, object] = dict(
        pump_id="p1",
        dart_address=1,
        current_state=PumpState.FILLING_COMPLETE,
        previous_state=PumpState.FILLING,
        awaiting_filling_complete=True,
        nozzle_out=False,
        communication_healthy=True,
        dispensed_volume_raw=1500,
        active_transaction_id="tx-1",
        last_observation_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )
    base.update(kwargs)
    return PumpContext(**base)  # type: ignore[arg-type]


def test_timeout_not_due_yet() -> None:
    started = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    decision = evaluate_awaiting_filling_complete_timeout(
        _ctx(),
        now=started + timedelta(seconds=10),
        timeout=timedelta(seconds=30),
        await_started_at=started,
        dc2_last_changed_at=started,
    )
    assert decision.should_infer is False
    assert decision.insufficient_evidence is False


def test_timeout_infers_with_stable_dc2() -> None:
    started = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    decision = evaluate_awaiting_filling_complete_timeout(
        _ctx(),
        now=started + timedelta(seconds=35),
        timeout=timedelta(seconds=30),
        await_started_at=started,
        dc2_last_changed_at=started,
        dc2_stability_window=timedelta(seconds=2),
    )
    assert decision.should_infer is True
    assert decision.event is PumpEvent.FILLING_COMPLETED
    assert any("INFERRED" in w for w in decision.warnings)


def test_timeout_insufficient_without_dc2() -> None:
    started = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    decision = evaluate_awaiting_filling_complete_timeout(
        _ctx(dispensed_volume_raw=None),
        now=started + timedelta(seconds=35),
        timeout=timedelta(seconds=30),
        await_started_at=started,
    )
    assert decision.should_infer is False
    assert decision.insufficient_evidence is True
    assert any("insufficient evidence" in w for w in decision.warnings)


def test_timeout_insufficient_when_nozzle_out() -> None:
    started = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    decision = evaluate_awaiting_filling_complete_timeout(
        _ctx(nozzle_out=True),
        now=started + timedelta(seconds=35),
        timeout=timedelta(seconds=30),
        await_started_at=started,
        dc2_last_changed_at=started,
    )
    assert decision.should_infer is False
    assert decision.insufficient_evidence is True


def test_timeout_noop_when_not_awaiting() -> None:
    decision = evaluate_awaiting_filling_complete_timeout(
        _ctx(awaiting_filling_complete=False),
        now=datetime(2026, 1, 1, 13, 0, tzinfo=UTC),
    )
    assert decision.should_infer is False


def test_timeout_dc2_not_yet_stable() -> None:
    started = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    now = started + timedelta(seconds=35)
    decision = evaluate_awaiting_filling_complete_timeout(
        _ctx(),
        now=now,
        timeout=timedelta(seconds=30),
        await_started_at=started,
        dc2_last_changed_at=now - timedelta(seconds=0.5),
        dc2_stability_window=timedelta(seconds=2),
    )
    assert decision.should_infer is False
    assert decision.insufficient_evidence is True
