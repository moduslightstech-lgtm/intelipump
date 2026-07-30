"""Infer FILLING_COMPLETED when DC1 confirmation times out after hang-up."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.models import PumpContext

# Short wait for DC1 STATUS=5 after NOZZLE_RETURNED hang-up.
DEFAULT_AWAITING_FILLING_COMPLETE_TIMEOUT = timedelta(seconds=30)
DEFAULT_DC2_STABILITY_WINDOW = timedelta(seconds=2)


@dataclass(frozen=True, slots=True)
class CompletionTimeoutDecision:
    should_infer: bool
    event: PumpEvent | None = None
    warnings: tuple[str, ...] = ()
    inferences: tuple[str, ...] = ()
    insufficient_evidence: bool = False


def evaluate_awaiting_filling_complete_timeout(
    context: PumpContext,
    *,
    now: datetime,
    timeout: timedelta = DEFAULT_AWAITING_FILLING_COMPLETE_TIMEOUT,
    await_started_at: datetime | None = None,
    dc2_last_changed_at: datetime | None = None,
    dc2_stability_window: timedelta = DEFAULT_DC2_STABILITY_WINDOW,
) -> CompletionTimeoutDecision:
    """Return inferred completion when hang-up await exceeds timeout.

    Pure helper — callers apply FILLING_COMPLETED with completion_inferred.
    Never auto-authorizes or resets.

    Inference requires strong evidence:
    - awaiting_filling_complete
    - nozzle IN
    - prior filling/limit (or hang-up already moved to FILLING_COMPLETE)
    - DC2 volume present and stable
    - communication healthy
    - active or unresolved transaction identity
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

    started = await_started_at or context.last_observation_at
    if started is None:
        return CompletionTimeoutDecision(
            should_infer=False,
            insufficient_evidence=True,
            warnings=("awaiting_filling_complete without await_started_at",),
        )
    if now - started < timeout:
        return CompletionTimeoutDecision(should_infer=False)

    missing = _evidence_gaps(
        context,
        now=now,
        dc2_last_changed_at=dc2_last_changed_at,
        dc2_stability_window=dc2_stability_window,
    )
    if missing:
        return CompletionTimeoutDecision(
            should_infer=False,
            insufficient_evidence=True,
            warnings=(
                "INFERRED completion withheld: insufficient evidence after "
                f"hang-up timeout ({', '.join(missing)}). Transaction left "
                "unresolved; no auto AUTHORIZE/RESET.",
            ),
            inferences=tuple(missing),
        )

    return CompletionTimeoutDecision(
        should_infer=True,
        event=PumpEvent.FILLING_COMPLETED,
        warnings=(
            "INFERRED: DC1 FILLING_COMPLETED missing after hang-up timeout; "
            "closing with audit (completion_inferred).",
        ),
        inferences=(
            f"awaiting_filling_complete exceeded {timeout.total_seconds():.0f}s",
            "nozzle_in",
            "dc2_stable",
            "communication_healthy",
        ),
    )


def _evidence_gaps(
    context: PumpContext,
    *,
    now: datetime,
    dc2_last_changed_at: datetime | None,
    dc2_stability_window: timedelta,
) -> tuple[str, ...]:
    gaps: list[str] = []
    if context.nozzle_out is not False:
        gaps.append("nozzle_not_in")
    prior_ok = (
        context.previous_state
        in {
            PumpState.FILLING,
            PumpState.LIMIT_REACHED,
            PumpState.SUSPENDED,
            PumpState.FILLING_COMPLETE,
        }
        or context.current_state
        in {
            PumpState.FILLING,
            PumpState.LIMIT_REACHED,
            PumpState.SUSPENDED,
            PumpState.FILLING_COMPLETE,
        }
    )
    if not prior_ok:
        gaps.append("not_previously_filling_or_limit")
    if context.dispensed_volume_raw is None:
        gaps.append("dc2_volume_missing")
    elif dc2_last_changed_at is not None and (
        now - dc2_last_changed_at < dc2_stability_window
    ):
        gaps.append("dc2_not_stable")
    if not context.communication_healthy:
        gaps.append("communication_unhealthy")
    if (
        context.active_transaction_id is None
        and not context.has_unresolved_transaction
    ):
        gaps.append("no_transaction_identity")
    return tuple(gaps)


__all__ = [
    "DEFAULT_AWAITING_FILLING_COMPLETE_TIMEOUT",
    "DEFAULT_DC2_STABILITY_WINDOW",
    "CompletionTimeoutDecision",
    "evaluate_awaiting_filling_complete_timeout",
]
