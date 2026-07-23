"""Pure command-eligibility evaluation (no transmit, queue, or execute)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.state_machine.models import PumpContext

_ACTIVE_EXECUTION_COMMANDS: frozenset[PumpCommand] = frozenset(
    {
        PumpCommand.SET_PRICE,
        PumpCommand.RESET,
        PumpCommand.AUTHORIZE,
        PumpCommand.STOP,
        PumpCommand.SUSPEND,
        PumpCommand.RESUME,
        PumpCommand.PRESET_AMOUNT,
        PumpCommand.PRESET_VOLUME,
    }
)

_SET_PRICE_STATES: frozenset[PumpState] = frozenset(
    {
        PumpState.READY,
        PumpState.RESET,
        PumpState.NOT_PROGRAMMED,
    }
)

_RESET_ELIGIBLE_STATES: frozenset[PumpState] = frozenset(
    {
        PumpState.FILLING_COMPLETE,
        PumpState.LIMIT_REACHED,
        PumpState.READY,
        PumpState.RESET,
        PumpState.NOT_PROGRAMMED,
        PumpState.NOZZLE_UP,
        PumpState.AUTHORIZED,
        PumpState.FAULTED,
        PumpState.DISCOVERING,
    }
)

_PRESET_STATES: frozenset[PumpState] = frozenset(
    {
        PumpState.READY,
        PumpState.RESET,
        PumpState.NOZZLE_UP,
        PumpState.AUTHORIZED,
    }
)

_STOP_STATES: frozenset[PumpState] = frozenset(
    {
        PumpState.AUTHORIZED,
        PumpState.FILLING,
        PumpState.SUSPENDED,
    }
)


@dataclass(frozen=True, slots=True)
class CommandEligibilityResult:
    eligible: bool
    command: PumpCommand
    current_state: PumpState
    blocking_reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    requires_physical_enable: bool
    requires_active_commands_enabled: bool


def evaluate_command_eligibility(
    command: PumpCommand,
    context: PumpContext,
    *,
    preset_value: Decimal | int | float | None = None,
    physical_enable_present: bool = False,
    active_commands_enabled: bool = False,
) -> CommandEligibilityResult:
    """Evaluate whether a command is eligible. Does not transmit or queue."""
    blocking: list[str] = []
    warnings: list[str] = []
    requires_physical = command in _ACTIVE_EXECUTION_COMMANDS
    requires_active = command in _ACTIVE_EXECUTION_COMMANDS
    if requires_active and not active_commands_enabled:
        warnings.append(
            "active_commands_enabled is false; execution remains disabled"
        )
    if requires_physical and not physical_enable_present:
        warnings.append(
            "physical_enable_present is false; execution remains disabled"
        )

    if command is PumpCommand.READ_STATUS:
        return CommandEligibilityResult(
            eligible=True,
            command=command,
            current_state=context.current_state,
            blocking_reasons=(),
            warnings=("Evaluation only; no status poll is transmitted.",),
            requires_physical_enable=False,
            requires_active_commands_enabled=False,
        )

    if command is PumpCommand.READ_TOTALS:
        if not context.communication_healthy:
            blocking.append("communication_unhealthy")
        return CommandEligibilityResult(
            eligible=not blocking,
            command=command,
            current_state=context.current_state,
            blocking_reasons=tuple(blocking),
            warnings=("Evaluation only; no totals request is transmitted.",),
            requires_physical_enable=False,
            requires_active_commands_enabled=False,
        )

    if command is PumpCommand.SET_PRICE:
        if not context.communication_healthy:
            blocking.append("communication_unhealthy")
        if context.current_state not in _SET_PRICE_STATES:
            blocking.append(
                f"state_{context.current_state.value}_not_eligible_for_set_price"
            )
        if context.current_state in {
            PumpState.FILLING,
            PumpState.AUTHORIZED,
            PumpState.SUSPENDED,
            PumpState.FILLING_COMPLETE,
        }:
            blocking.append("active_or_completed_transaction_window")
        if context.active_transaction_id is not None:
            blocking.append("active_unresolved_transaction")

    elif command is PumpCommand.AUTHORIZE:
        if context.current_state is not PumpState.NOZZLE_UP:
            blocking.append("state_not_nozzle_up")
        if context.selected_nozzle is None:
            blocking.append("selected_nozzle_unknown")
        if not context.price_verified:
            blocking.append("price_not_verified")
        if not context.communication_healthy:
            blocking.append("communication_unhealthy")
        if context.active_transaction_id is not None:
            blocking.append("active_unresolved_transaction")
        if context.fault_code is not None or context.current_state is PumpState.FAULTED:
            blocking.append("fault_present")
        warnings.append(
            "AUTHORIZE eligibility only; active command execution remains disabled."
        )

    elif command is PumpCommand.STOP:
        if context.current_state not in _STOP_STATES:
            blocking.append("state_not_eligible_for_stop")
        if not context.communication_healthy:
            blocking.append("communication_unhealthy")

    elif command is PumpCommand.SUSPEND:
        if context.current_state is not PumpState.FILLING:
            blocking.append("state_not_filling")
        if not context.communication_healthy:
            blocking.append("communication_unhealthy")

    elif command is PumpCommand.RESUME:
        if context.current_state is not PumpState.SUSPENDED:
            blocking.append("state_not_suspended")
        if not context.communication_healthy:
            blocking.append("communication_unhealthy")

    elif command is PumpCommand.RESET:
        if context.current_state is PumpState.FILLING:
            blocking.append("reset_not_allowed_during_filling")
        elif context.current_state not in _RESET_ELIGIBLE_STATES:
            blocking.append(
                f"state_{context.current_state.value}_not_explicitly_reset_eligible"
            )
        if not context.communication_healthy:
            blocking.append("communication_unhealthy")

    elif command in {PumpCommand.PRESET_AMOUNT, PumpCommand.PRESET_VOLUME}:
        if context.current_state not in _PRESET_STATES:
            blocking.append("preset_only_before_filling")
        if context.current_state is PumpState.FILLING:
            blocking.append("preset_not_during_filling")
        if preset_value is None:
            blocking.append("preset_value_missing")
        else:
            try:
                value = Decimal(str(preset_value))
            except Exception:
                blocking.append("preset_value_invalid")
            else:
                if value <= 0:
                    blocking.append("preset_value_not_positive")
        if context.selected_nozzle is None:
            blocking.append("selected_nozzle_unknown")
        if not context.price_verified:
            blocking.append("price_not_verified")
        if not context.communication_healthy:
            blocking.append("communication_unhealthy")

    else:
        blocking.append(f"unsupported_command_{command.value}")

    if requires_active:
        warnings.append(
            "requires_active_commands_enabled=True for any future execution path"
        )
    if requires_physical:
        warnings.append(
            "requires_physical_enable=True for any future execution path"
        )

    # Deduplicate blocking reasons while preserving order.
    seen: set[str] = set()
    unique_blocking: list[str] = []
    for reason in blocking:
        if reason not in seen:
            seen.add(reason)
            unique_blocking.append(reason)

    return CommandEligibilityResult(
        eligible=len(unique_blocking) == 0,
        command=command,
        current_state=context.current_state,
        blocking_reasons=tuple(unique_blocking),
        warnings=tuple(warnings),
        requires_physical_enable=requires_physical,
        requires_active_commands_enabled=requires_active,
    )
