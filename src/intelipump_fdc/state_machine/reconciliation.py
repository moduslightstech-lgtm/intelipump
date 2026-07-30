"""Restart reconciliation (pure; never auto-authorizes or replays commands)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from intelipump_fdc.domain.pump_command import NON_IDEMPOTENT_COMMANDS, PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.state_machine.models import PumpContext


@dataclass(frozen=True, slots=True)
class LiveObservationSummary:
    """First live evidence after restart (already interpreted by caller/mapper)."""

    communication_healthy: bool
    wayne_status: int | None = None
    normalized_hint: PumpState | None = None
    observed_at: datetime | None = None
    selected_nozzle: int | None = None
    nozzle_out: bool | None = None
    dispensed_volume_raw: int | None = None
    raw_source_hex: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    context: PumpContext
    warnings: tuple[str, ...]
    recovered_state: PumpState
    preserved_unresolved_transaction_id: str | None
    discarded_pending_commands: tuple[str, ...]
    auto_authorize_attempted: bool = False
    pending_commands_replayed: bool = False


def reconcile_after_restart(
    persisted: PumpContext,
    live: LiveObservationSummary,
    *,
    had_active_transaction_persisted: bool | None = None,
    pending_commands: tuple[PumpCommand, ...] = (),
) -> ReconciliationResult:
    """Reconcile persisted pump context with first live observations.

    Rules:
    - never automatically authorize
    - never replay pending non-idempotent commands
    - prefer live dispenser state over persisted state
    - preserve unresolved transaction for investigation
    """
    warnings: list[str] = [
        "Restart reconciliation: live dispenser state preferred over persisted.",
        "Never automatically authorize after restart.",
    ]

    discarded: list[str] = []
    for cmd in pending_commands:
        discarded.append(cmd.value)
        if cmd in NON_IDEMPOTENT_COMMANDS:
            warnings.append(
                f"Discarded pending non-idempotent command {cmd.value}; "
                "will not replay."
            )
        else:
            warnings.append(f"Discarded pending command {cmd.value}; not replayed.")

    if PumpCommand.AUTHORIZE in pending_commands:
        warnings.append(
            "Persisted pending AUTHORIZE was discarded; no auto-authorization."
        )

    unresolved = persisted.active_transaction_id
    if had_active_transaction_persisted or unresolved is not None:
        warnings.append(
            "Preserving unresolved transaction id for investigation; "
            "not clearing on restart alone."
        )

    if not live.communication_healthy:
        ctx = persisted.with_updates(
            previous_state=persisted.current_state,
            current_state=PumpState.DISCONNECTED,
            communication_healthy=False,
            state_version=persisted.state_version + 1,
            last_observation_at=live.observed_at,
            last_transition_at=live.observed_at,
            warnings=tuple([*persisted.warnings, *warnings]),
            # Keep unresolved transaction.
            active_transaction_id=unresolved,
        )
        warnings.append("No communication after restart; remain DISCONNECTED.")
        return ReconciliationResult(
            context=ctx,
            warnings=tuple(warnings),
            recovered_state=PumpState.DISCONNECTED,
            preserved_unresolved_transaction_id=unresolved,
            discarded_pending_commands=tuple(discarded),
            auto_authorize_attempted=False,
            pending_commands_replayed=False,
        )

    live_state = _infer_live_state(live)
    if live_state is None:
        ctx = persisted.with_updates(
            previous_state=persisted.current_state,
            current_state=PumpState.DISCOVERING,
            communication_healthy=True,
            state_version=persisted.state_version + 1,
            last_observation_at=live.observed_at,
            last_transition_at=live.observed_at,
            last_raw_wayne_status=live.wayne_status,
            selected_nozzle=live.selected_nozzle or persisted.selected_nozzle,
            nozzle_out=(
                live.nozzle_out
                if live.nozzle_out is not None
                else persisted.nozzle_out
            ),
            dispensed_volume_raw=(
                live.dispensed_volume_raw
                if live.dispensed_volume_raw is not None
                else persisted.dispensed_volume_raw
            ),
            warnings=tuple([*persisted.warnings, *warnings]),
            active_transaction_id=unresolved,
            has_unresolved_transaction=unresolved is not None,
            price_verified=False,  # never assume price after restart
        )
        warnings.append(
            "Live state unknown after restart; remain DISCOVERING "
            "(not AUTHORIZED/FILLING)."
        )
        return ReconciliationResult(
            context=ctx,
            warnings=tuple(warnings),
            recovered_state=PumpState.DISCOVERING,
            preserved_unresolved_transaction_id=unresolved,
            discarded_pending_commands=tuple(discarded),
            auto_authorize_attempted=False,
            pending_commands_replayed=False,
        )

    if live_state is PumpState.FILLING:
        warnings.append(
            "Live state indicates FILLING; recovered into FILLING with warning "
            "(prefer live over persisted; restore existing transaction, "
            "do not create a duplicate)."
        )
    if live_state is PumpState.FILLING_COMPLETE:
        warnings.append(
            "Live state indicates FILLING_COMPLETE; recovered into "
            "FILLING_COMPLETE (finalize existing unresolved transaction once)."
        )
    if persisted.current_state is PumpState.FILLING and live_state is not PumpState.FILLING:
        warnings.append(
            f"Persisted state was FILLING but live indicates {live_state.value}; "
            "live wins."
        )

    # Never land in AUTHORIZED solely from restart reconciliation.
    if live_state is PumpState.AUTHORIZED:
        warnings.append(
            "Live Wayne AUTHORIZED observed; recovering to AUTHORIZED from live "
            "evidence only (not from persisted pending AUTHORIZE)."
        )

    # Case C/D: RESET must not derive READY before transaction reconciliation.
    if live_state is PumpState.RESET and unresolved is not None:
        if live.nozzle_out is False:
            warnings.append(
                "RESET + nozzle IN with unresolved transaction: retain "
                "unresolved/auditable state; do not derive READY until "
                "transaction reconciliation completes."
            )
        elif live.nozzle_out is True:
            warnings.append(
                "RESET + nozzle OUT with unresolved transaction: retain "
                "recovery state; do not derive READY; wait for more DC1/DC2/DC3."
            )
        else:
            warnings.append(
                "RESET with unresolved transaction: retain unresolved state; "
                "do not derive READY."
            )

    keep_unresolved = unresolved is not None and live_state not in {
        PumpState.FILLING_COMPLETE,
        PumpState.LIMIT_REACHED,
    }
    # FILLING_COMPLETE still keeps the id until publish/complete; mark resolved
    # only after the persistence bridge finalizes.
    awaiting = (
        live_state is PumpState.FILLING_COMPLETE
        and unresolved is not None
        and persisted.awaiting_filling_complete
    )

    ctx = persisted.with_updates(
        previous_state=persisted.current_state,
        current_state=live_state,
        communication_healthy=True,
        state_version=persisted.state_version + 1,
        last_observation_at=live.observed_at,
        last_transition_at=live.observed_at,
        last_raw_wayne_status=live.wayne_status,
        selected_nozzle=live.selected_nozzle or persisted.selected_nozzle,
        nozzle_out=(
            live.nozzle_out if live.nozzle_out is not None else persisted.nozzle_out
        ),
        dispensed_volume_raw=(
            live.dispensed_volume_raw
            if live.dispensed_volume_raw is not None
            else persisted.dispensed_volume_raw
        ),
        warnings=tuple([*persisted.warnings, *warnings]),
        active_transaction_id=unresolved,
        has_unresolved_transaction=keep_unresolved or (unresolved is not None),
        awaiting_filling_complete=awaiting,
        price_verified=False,
        fault_code=(
            persisted.fault_code if live_state is PumpState.FAULTED else None
        ),
    )
    return ReconciliationResult(
        context=ctx,
        warnings=tuple(warnings),
        recovered_state=live_state,
        preserved_unresolved_transaction_id=unresolved,
        discarded_pending_commands=tuple(discarded),
        auto_authorize_attempted=False,
        pending_commands_replayed=False,
    )


def _infer_live_state(live: LiveObservationSummary) -> PumpState | None:
    if live.normalized_hint is not None:
        return live.normalized_hint
    if live.wayne_status is None:
        return None
    try:
        status = WaynePumpStatus(live.wayne_status)
    except ValueError:
        return None

    mapping: dict[WaynePumpStatus, PumpState] = {
        WaynePumpStatus.PUMP_NOT_PROGRAMMED: PumpState.NOT_PROGRAMMED,
        WaynePumpStatus.RESET: PumpState.RESET,
        WaynePumpStatus.AUTHORIZED: PumpState.AUTHORIZED,
        WaynePumpStatus.FILLING: PumpState.FILLING,
        WaynePumpStatus.FILLING_COMPLETED: PumpState.FILLING_COMPLETE,
        WaynePumpStatus.MAX_AMOUNT_VOLUME_REACHED: PumpState.LIMIT_REACHED,
        WaynePumpStatus.SUSPENDED: PumpState.SUSPENDED,
    }
    if status is WaynePumpStatus.SWITCHED_OFF:
        return None
    return mapping.get(status)
