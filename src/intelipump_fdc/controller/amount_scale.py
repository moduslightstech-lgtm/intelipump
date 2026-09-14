"""Normalize Wayne DC2 money totals to the 2-decimal ledger scale.

US Lab pumps report amount with 2 money decimals (₦500.00 → raw 50000).
Some field pumps (e.g. SAO RS1) report with 1 money decimal (₦500.0 → raw 5000)
while volume stays at 2 decimals (0.37 L → raw 37).

When unit price is known, volume × price predicts the 2-dp amount. If the wire
amount is one decade short of that prediction, scale it up once so SQLite,
MQTT, and the dashboard stay on the same convention as US Lab.
"""

from __future__ import annotations


def coerce_amount_raw_to_2dp(
    *,
    volume_raw: int,
    amount_raw: int,
    unit_price_raw: int | None,
) -> int:
    """Return amount_raw, scaled ×10 when volume×price proves 1-dp money."""
    if (
        not isinstance(volume_raw, int)
        or not isinstance(amount_raw, int)
        or volume_raw <= 0
        or amount_raw <= 0
    ):
        return amount_raw
    if not isinstance(unit_price_raw, int) or unit_price_raw <= 0:
        return amount_raw

    # Ledger convention: amount_raw ≈ volume_raw * unit_price_raw (both 2 dp).
    expected = volume_raw * unit_price_raw
    tol = max(unit_price_raw, unit_price_raw // 50, 1)
    if abs(amount_raw - expected) <= tol:
        return amount_raw

    scaled = amount_raw * 10
    if abs(scaled - expected) <= tol:
        return scaled
    return amount_raw
