"""Pure CD1 RESET nozzle-gate decision (unit-testable, no I/O)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from intelipump_fdc.bench_poll.guards import TARGET_OWNED_LAB_WAYNE
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.real_wayne_price.guards import ResetWriteConfirmations, ResetWriteParams
from intelipump_fdc.real_wayne_price.nozzle_physical import (
    ControllerProfile,
    NozzlePhysicalState,
    decode_nozzle_state,
)

OWNED_LAB_NOZIO_UNKNOWN_WARNING = (
    "NOZIO did not expose the physical nozzle-out state. Proceeding with one "
    "owned-lab RESET diagnostic based on explicit operator confirmation. "
    "AUTHORIZE is prohibited."
)


class ResetNozzleDecision(StrEnum):
    """Decision outcome for the pre-TX nozzle gate."""

    ALLOW_NORMAL = "ALLOW_NORMAL"
    ALLOW_OWNED_LAB_UNKNOWN_OVERRIDE = "ALLOW_OWNED_LAB_UNKNOWN_OVERRIDE"
    REFUSE = "REFUSE"


@dataclass(frozen=True, slots=True)
class ResetNozzleGateResult:
    allow: bool
    decision: ResetNozzleDecision
    nozzle_state: NozzlePhysicalState
    refusal_reason: str | None = None
    warning: str | None = None


def controller_profile_for_reset(params: ResetWriteParams) -> ControllerProfile:
    """Normal path trusts bit 0x10; override path treats NOZIO as UNKNOWN."""
    if params.confirmations.allow_nozio_unknown_for_reset:
        return ControllerProfile(supports_nozio_out_bit=False)
    return ControllerProfile(supports_nozio_out_bit=True)


def owned_lab_nozio_unknown_override_confirmed(params: ResetWriteParams) -> bool:
    """True only when every owned-lab override confirmation is present."""
    c = params.confirmations
    return (
        params.target_type == TARGET_OWNED_LAB_WAYNE
        and c.allow_nozio_unknown_for_reset
        and c.confirm_physical_nozzle_out
        and c.confirm_price_visible
        and c.no_product_connected
        and c.authorization_disabled
        and c.technician_present
        and c.emergency_isolation_ready
        and c.confirm_reset_only
        and c.confirm_no_authorize
        and c.owned_lab_pump
    )


def evaluate_reset_nozzle_gate(
    *,
    dc1_code: int | None,
    nozio: int,
    controller_profile: ControllerProfile,
    owned_lab_override_confirmed: bool,
) -> ResetNozzleGateResult:
    """Apply FILLING_COMPLETED + nozzle physical-state gate before RESET TX."""
    nozzle_state = decode_nozzle_state(nozio, controller_profile)
    if dc1_code != int(WaynePumpStatus.FILLING_COMPLETED):
        return ResetNozzleGateResult(
            allow=False,
            decision=ResetNozzleDecision.REFUSE,
            nozzle_state=nozzle_state,
            refusal_reason="status_not_filling_completed",
        )
    if nozzle_state is NozzlePhysicalState.OUT:
        return ResetNozzleGateResult(
            allow=True,
            decision=ResetNozzleDecision.ALLOW_NORMAL,
            nozzle_state=nozzle_state,
        )
    if nozzle_state is NozzlePhysicalState.UNKNOWN:
        if owned_lab_override_confirmed:
            return ResetNozzleGateResult(
                allow=True,
                decision=ResetNozzleDecision.ALLOW_OWNED_LAB_UNKNOWN_OVERRIDE,
                nozzle_state=nozzle_state,
                warning=OWNED_LAB_NOZIO_UNKNOWN_WARNING,
            )
        return ResetNozzleGateResult(
            allow=False,
            decision=ResetNozzleDecision.REFUSE,
            nozzle_state=nozzle_state,
            refusal_reason="physical_nozzle_state_unknown",
        )
    return ResetNozzleGateResult(
        allow=False,
        decision=ResetNozzleDecision.REFUSE,
        nozzle_state=nozzle_state,
        refusal_reason="nozzle_not_out",
    )


def override_confirmation_missing_flags(
    confirms: ResetWriteConfirmations,
) -> list[str]:
    """Flags required only when ``--allow-nozio-unknown-for-reset`` is set."""
    mapping = {
        "confirm_physical_nozzle_out": "--confirm-physical-nozzle-out",
        "confirm_price_visible": "--confirm-price-visible",
        "confirm_reset_only": "--confirm-reset-only",
        "confirm_no_authorize": "--confirm-no-authorize",
        "no_product_connected": (
            "--confirm-no-product-connected/--confirm-no-fuel-test"
        ),
        "authorization_disabled": "--confirm-authorization-disabled",
        "technician_present": "--confirm-technician-present",
        "emergency_isolation_ready": "--confirm-emergency-isolation-ready",
        "owned_lab_pump": "--confirm-owned-lab-pump",
    }
    return [flag for attr, flag in mapping.items() if not getattr(confirms, attr)]


__all__ = [
    "OWNED_LAB_NOZIO_UNKNOWN_WARNING",
    "ResetNozzleDecision",
    "ResetNozzleGateResult",
    "controller_profile_for_reset",
    "evaluate_reset_nozzle_gate",
    "override_confirmation_missing_flags",
    "owned_lab_nozio_unknown_override_confirmed",
]
