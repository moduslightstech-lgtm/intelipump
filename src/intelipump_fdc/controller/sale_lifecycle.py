"""Sale lifecycle tracking and valid-sale evidence gates.

Does **not** copy working-controller false completions. A publishable sale
requires positive delivery evidence; zero-delivery returns abort without sale.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class SaleLifecycle(StrEnum):
    IDLE = "IDLE"
    NOZZLE_LIFTED = "NOZZLE_LIFTED"
    AUTHORIZED = "AUTHORIZED"
    FILLING = "FILLING"
    FILLING_COMPLETED = "FILLING_COMPLETED"
    ABORTED_NO_DELIVERY = "ABORTED_NO_DELIVERY"
    ABORTED = "ABORTED"
    CLOSED = "CLOSED"


@dataclass(slots=True)
class SaleEvidence:
    """Accumulated evidence for one fueling attempt at a pump address."""

    lifecycle: SaleLifecycle = SaleLifecycle.IDLE
    nozzle_lifted: bool = False
    auth_application_confirmed: bool = False
    filling_observed: bool = False
    filling_completed_observed: bool = False
    peak_volume_raw: int = 0
    peak_amount_raw: int = 0
    aborted: bool = False
    sale_published: bool = False

    @property
    def has_positive_delivery(self) -> bool:
        return self.peak_volume_raw > 0 and self.peak_amount_raw > 0

    def note_nozzle_out(self) -> None:
        if self.lifecycle in {
            SaleLifecycle.IDLE,
            SaleLifecycle.CLOSED,
            SaleLifecycle.ABORTED_NO_DELIVERY,
            SaleLifecycle.ABORTED,
            SaleLifecycle.FILLING_COMPLETED,
        }:
            self.reset_attempt()
        self.nozzle_lifted = True
        self.lifecycle = SaleLifecycle.NOZZLE_LIFTED

    def note_authorized(self, *, application_confirmed: bool = False) -> None:
        if application_confirmed:
            self.auth_application_confirmed = True
        if self.lifecycle in {
            SaleLifecycle.IDLE,
            SaleLifecycle.NOZZLE_LIFTED,
            SaleLifecycle.AUTHORIZED,
        }:
            self.lifecycle = SaleLifecycle.AUTHORIZED

    def note_filling(self) -> None:
        self.filling_observed = True
        if self.lifecycle not in {
            SaleLifecycle.ABORTED,
            SaleLifecycle.ABORTED_NO_DELIVERY,
        }:
            self.lifecycle = SaleLifecycle.FILLING

    def note_dc2(self, *, volume_raw: int | None, amount_raw: int | None) -> None:
        if isinstance(volume_raw, int) and volume_raw > self.peak_volume_raw:
            self.peak_volume_raw = volume_raw
        if isinstance(amount_raw, int) and amount_raw > self.peak_amount_raw:
            self.peak_amount_raw = amount_raw

    def note_nozzle_in_zero_delivery(self) -> SaleLifecycle:
        """Zero-delivery lift-return → ABORTED_NO_DELIVERY (no completed sale)."""
        self.aborted = True
        self.lifecycle = SaleLifecycle.ABORTED_NO_DELIVERY
        return self.lifecycle

    def evaluate_filling_completed(self) -> tuple[bool, str]:
        """Return ``(may_finalize_sale, reason)``.

        Valid sale requires: FILLING observed, DC2 vol&amt > 0,
        FILLING_COMPLETED, not already aborted. Zero delivery and
        FILLING_COMPLETED without FILLING never publish a sale.
        """
        self.filling_completed_observed = True
        if self.aborted or self.lifecycle is SaleLifecycle.ABORTED_NO_DELIVERY:
            return False, "already_aborted"
        if self.lifecycle is SaleLifecycle.ABORTED:
            return False, "already_aborted"
        if not self.filling_observed:
            self.aborted = True
            self.lifecycle = SaleLifecycle.ABORTED_NO_DELIVERY
            return False, "filling_completed_without_filling"
        if not self.has_positive_delivery:
            self.aborted = True
            self.lifecycle = SaleLifecycle.ABORTED_NO_DELIVERY
            return False, "zero_delivery"
        # FILLING on Wayne implies prior authorization (local or foreign master).
        if not self.auth_application_confirmed:
            self.auth_application_confirmed = True
        if not self.nozzle_lifted:
            self.nozzle_lifted = True
        self.lifecycle = SaleLifecycle.FILLING_COMPLETED
        return True, "valid_sale_evidence"

    def reset_attempt(self) -> None:
        self.lifecycle = SaleLifecycle.IDLE
        self.nozzle_lifted = False
        self.auth_application_confirmed = False
        self.filling_observed = False
        self.filling_completed_observed = False
        self.peak_volume_raw = 0
        self.peak_amount_raw = 0
        self.aborted = False
        self.sale_published = False
