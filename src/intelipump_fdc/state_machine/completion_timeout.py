"""Infer FILLING_COMPLETED when DC1 confirmation times out after hang-up."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.models import PumpContext

# Short wait for DC1 STATUS=5 after NOZZLE_RETURNED hang-up.
DEFAULT_AWAITING_FILLING_COMPLETE_TIMEOUT = timedelta(seconds=30)


@dataclass(frozen=True, slots=True)
class CompletionTimeoutDecision:
    should_infer: bool
    event: PumpEvent | None = None
    warnings: tuple[str, ...] = ()
    inferences: tuple[str, ...] = ()


def evaluate_awaiting_filling_complete_timeout(
    context: PumpContext,
    *,
    now: datetime,
    timeout: timedelta = DEFAULT_AWAITING_FILLING_COMPLETE_TIMEOUT,
) -> CompletionTimeoutDecision:
    """Return inferred completion when hang-up await exceeds timeout.

    Pure helper — callers apply FILLING_COMPLETED with completion_inferred.
    Never auto-authorizes.
    """
    if not context.awaiting_filling_complete:
        return CompletionTimeoutDecision(should_infer=False)
    if context.current_state not in {
        PumpState.FILLING_COMPLETE,
        PumpState.FILLING,
        PumpState.SUSPENDED,
        PumpState.LIMIT_REACHED,
    }:
        return CompletionTimeoutDecision(should_infer=False)

    started = context.last_observation_at
    if started is None:
        return CompletionTimeoutDecision(
            should_infer=False,
            warnings=("awaiting_filling_complete without last_observation_at",),
        )
    if now - started < timeout:
        return CompletionTimeoutDecision(should_infer=False)

    return CompletionTimeoutDecision(
        should_infer=True,
        event=PumpEvent.FILLING_COMPLETED,
        warnings=(
            "INFERRED: DC1 FILLING_COMPLETED missing after hang-up timeout; "
            "closing with audit (completion_inferred).",
        ),
        inferences=(
            f"awaiting_filling_complete exceeded {timeout.total_seconds():.0f}s",
        ),
    )


__all__ = [
    "DEFAULT_AWAITING_FILLING_COMPLETE_TIMEOUT",
    "CompletionTimeoutDecision",
    "evaluate_awaiting_filling_complete_timeout",
]
