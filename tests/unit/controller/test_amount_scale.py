"""DC2 amount scale coercion (1-dp money → 2-dp ledger)."""

from __future__ import annotations

from intelipump_fdc.controller.amount_scale import coerce_amount_raw_to_2dp


def test_scales_one_dp_amount_to_match_volume_times_price() -> None:
    # 0.36 L × ₦1400 = ₦504 ≈ wire 5000 (1 dp) → ledger 50000 (2 dp)
    assert (
        coerce_amount_raw_to_2dp(
            volume_raw=36, amount_raw=5000, unit_price_raw=1400
        )
        == 50000
    )


def test_leaves_already_2dp_amount_unchanged() -> None:
    assert (
        coerce_amount_raw_to_2dp(
            volume_raw=36, amount_raw=50000, unit_price_raw=1400
        )
        == 50000
    )


def test_leaves_mismatched_amount_unchanged() -> None:
    assert (
        coerce_amount_raw_to_2dp(
            volume_raw=36, amount_raw=1234, unit_price_raw=1400
        )
        == 1234
    )
