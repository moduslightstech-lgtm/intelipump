"""Safety gates for controller outbound DATA (Phase 6)."""

from __future__ import annotations

from dataclasses import dataclass

from intelipump_fdc.controller.session_models import OutboundDataItem
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.domain.pump_command import NON_IDEMPOTENT_COMMANDS, PumpCommand

_ACTIVE_COMMANDS: frozenset[PumpCommand] = frozenset(NON_IDEMPOTENT_COMMANDS)
_READ_COMMANDS: frozenset[PumpCommand] = frozenset(
    {PumpCommand.READ_STATUS, PumpCommand.READ_TOTALS}
)


@dataclass(frozen=True, slots=True)
class SafetyDecision:
    allowed: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ControllerSafetyContext:
    environment: str
    mode: ControllerMode
    active_commands_enabled: bool
    require_physical_control_enable: bool
    physical_enable_present: bool = False
    allow_virtual_polling: bool = True
    allow_lab_simulator_commands: bool = False


def evaluate_outbound_safety(
    item: OutboundDataItem,
    ctx: ControllerSafetyContext,
) -> SafetyDecision:
    """Reject production active commands; allow LAB simulator-only reads.

    Phase 8 may additionally allow LAB simulator commands when
    ``allow_lab_simulator_commands`` is set (virtual transport gated at API).
    """
    reasons: list[str] = []
    env = ctx.environment.upper()

    if ctx.mode is ControllerMode.LOCKED_OUT:
        return SafetyDecision(False, ("controller_LOCKED_OUT",))
    if ctx.mode is ControllerMode.FIELD_CONTROL:
        return SafetyDecision(False, ("FIELD_CONTROL_not_enabled_in_phase8",))

    if item.command_type in _READ_COMMANDS:
        if not item.simulator_only:
            reasons.append("read_requests_must_be_simulator_only_in_phase6")
        if env != "LAB":
            reasons.append("simulator_only_requests_require_LAB")
        if ctx.mode not in {ControllerMode.LISTEN_ONLY, ControllerMode.BENCH_CONTROL}:
            reasons.append(f"mode_{ctx.mode.value}_blocks_reads")
        return SafetyDecision(allowed=not reasons, reasons=tuple(reasons))

    # Phase 8 LAB simulator active commands (API-gated virtual transport).
    if (
        ctx.allow_lab_simulator_commands
        and item.simulator_only
        and env == "LAB"
        and ctx.mode in {ControllerMode.LISTEN_ONLY, ControllerMode.BENCH_CONTROL}
    ):
        return SafetyDecision(True, ())

    # Any non-read command is treated as active for Phase 6/8 production path.
    reasons.append(f"active_command_{item.command_type.value}_blocked_in_phase8")
    if not item.simulator_only:
        reasons.append("active_command_missing_simulator_only_flag")
    if env != "LAB":
        reasons.append("simulator_only_requests_require_LAB")
    if ctx.mode is ControllerMode.LISTEN_ONLY:
        reasons.append("LISTEN_ONLY_blocks_active_commands")
    if not ctx.active_commands_enabled:
        reasons.append("active_commands_enabled_is_false")
    if ctx.require_physical_control_enable and not ctx.physical_enable_present:
        reasons.append("physical_enable_required")
    if item.command_type in _ACTIVE_COMMANDS:
        reasons.append("non_idempotent_command")
    return SafetyDecision(allowed=False, reasons=tuple(dict.fromkeys(reasons)))


def evaluate_polling_allowed(ctx: ControllerSafetyContext) -> SafetyDecision:
    if ctx.mode is ControllerMode.LOCKED_OUT:
        return SafetyDecision(False, ("controller_LOCKED_OUT",))
    if ctx.environment.upper() != "LAB" and not ctx.allow_virtual_polling:
        return SafetyDecision(False, ("polling_requires_LAB_or_explicit_virtual_config",))
    if ctx.mode in {
        ControllerMode.LISTEN_ONLY,
        ControllerMode.BENCH_CONTROL,
    } and (ctx.allow_virtual_polling or ctx.environment.upper() == "LAB"):
        return SafetyDecision(True, ())
    return SafetyDecision(False, (f"mode_{ctx.mode.value}_polling_not_permitted",))
