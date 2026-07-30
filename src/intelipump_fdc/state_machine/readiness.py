"""Application READY derivation (Wayne has no native READY status)."""

from __future__ import annotations

from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.state_machine.models import PumpContext


def can_derive_ready(context: PumpContext) -> bool:
    """Return True only when application READY may be derived.

    READY is a normalized InteliPump state — never a Wayne DC1 status.
    """
    return (
        context.last_raw_wayne_status == int(WaynePumpStatus.RESET)
        and context.nozzle_out is False
        and context.communication_healthy
        and context.active_transaction_id is None
        and not context.has_unresolved_transaction
        and context.fault_code is None
    )


def readiness_blockers(context: PumpContext) -> tuple[str, ...]:
    """Human-readable reasons READY cannot be derived."""
    blockers: list[str] = []
    if context.last_raw_wayne_status != int(WaynePumpStatus.RESET):
        blockers.append("wayne_dc1_not_reset")
    if context.nozzle_out is not False:
        blockers.append("nozzle_not_in")
    if not context.communication_healthy:
        blockers.append("communication_unhealthy")
    if context.active_transaction_id is not None:
        blockers.append("active_transaction")
    if context.has_unresolved_transaction:
        blockers.append("unresolved_transaction")
    if context.fault_code is not None:
        blockers.append("fault_present")
    return tuple(blockers)


__all__ = ["can_derive_ready", "readiness_blockers"]
