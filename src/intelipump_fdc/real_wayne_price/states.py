"""Dry-run state values for technician-supervised CD5 price programming."""

from __future__ import annotations

from enum import StrEnum


class PriceDryRunState(StrEnum):
    PUMP_NOT_PROGRAMMED = "PUMP_NOT_PROGRAMMED"
    PRICE_BLOCK_BUILT = "PRICE_BLOCK_BUILT"
    PRICE_BLOCK_REVIEW_PENDING = "PRICE_BLOCK_REVIEW_PENDING"
    PRICE_BLOCK_APPROVED_NOT_TRANSMITTED = "PRICE_BLOCK_APPROVED_NOT_TRANSMITTED"
    FILLING_COMPLETE_EXPECTED = "FILLING_COMPLETE_EXPECTED"
    REFUSED = "REFUSED"
    FAULT = "FAULT"
