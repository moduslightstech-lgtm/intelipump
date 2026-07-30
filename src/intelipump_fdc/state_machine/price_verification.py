"""Explicit DC3 filling-price verification (no silent price_verified=True)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PriceVerificationResult:
    verified: bool
    partial: bool
    expected_raw: int | None
    received_raw: int | None
    selected_nozzle: int | None
    warnings: tuple[str, ...]
    reasons: tuple[str, ...]


def verify_dc3_filling_price(
    *,
    received_price_raw: int | None,
    selected_nozzle: int | None,
    configured_prices_raw: Mapping[int, int],
    price_decimals: int | None,
    price_bcd_valid: bool = True,
) -> PriceVerificationResult:
    """Verify DC3 filling price against configured/accepted nozzle price.

    When unit-price decimals are unknown, preserve raw BCD and mark partial
    (never silently verified).
    """
    warnings: list[str] = []
    reasons: list[str] = []

    if not price_bcd_valid:
        reasons.append("invalid_price_bcd")
        return PriceVerificationResult(
            verified=False,
            partial=False,
            expected_raw=None,
            received_raw=received_price_raw,
            selected_nozzle=selected_nozzle,
            warnings=tuple(warnings),
            reasons=tuple(reasons),
        )

    if selected_nozzle is None:
        reasons.append("selected_nozzle_unknown")
        return PriceVerificationResult(
            verified=False,
            partial=False,
            expected_raw=None,
            received_raw=received_price_raw,
            selected_nozzle=None,
            warnings=tuple(warnings),
            reasons=tuple(reasons),
        )

    if received_price_raw is None:
        reasons.append("received_price_missing")
        return PriceVerificationResult(
            verified=False,
            partial=False,
            expected_raw=configured_prices_raw.get(selected_nozzle),
            received_raw=None,
            selected_nozzle=selected_nozzle,
            warnings=tuple(warnings),
            reasons=tuple(reasons),
        )

    expected = configured_prices_raw.get(selected_nozzle)
    if expected is None:
        reasons.append("configured_price_missing")
        return PriceVerificationResult(
            verified=False,
            partial=False,
            expected_raw=None,
            received_raw=received_price_raw,
            selected_nozzle=selected_nozzle,
            warnings=tuple(warnings),
            reasons=tuple(reasons),
        )

    if price_decimals is None:
        warnings.append(
            "price_decimals unknown; raw BCD compared without Decimal scale"
        )
        if received_price_raw == expected:
            return PriceVerificationResult(
                verified=False,
                partial=True,
                expected_raw=expected,
                received_raw=received_price_raw,
                selected_nozzle=selected_nozzle,
                warnings=tuple(warnings),
                reasons=("decimals_unknown_raw_match",),
            )
        reasons.append("raw_price_mismatch_decimals_unknown")
        return PriceVerificationResult(
            verified=False,
            partial=True,
            expected_raw=expected,
            received_raw=received_price_raw,
            selected_nozzle=selected_nozzle,
            warnings=tuple(warnings),
            reasons=tuple(reasons),
        )

    if received_price_raw != expected:
        reasons.append("price_mismatch")
        return PriceVerificationResult(
            verified=False,
            partial=False,
            expected_raw=expected,
            received_raw=received_price_raw,
            selected_nozzle=selected_nozzle,
            warnings=tuple(warnings),
            reasons=tuple(reasons),
        )

    return PriceVerificationResult(
        verified=True,
        partial=False,
        expected_raw=expected,
        received_raw=received_price_raw,
        selected_nozzle=selected_nozzle,
        warnings=tuple(warnings),
        reasons=(),
    )


__all__ = ["PriceVerificationResult", "verify_dc3_filling_price"]
