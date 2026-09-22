"""DC2 amount scale coercion (1-dp / whole-naira money → 2-dp ledger)."""

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


def test_scales_whole_naira_ago_amount_to_match_volume_times_price() -> None:
    # SAO AGO: 0.53 L × ₦1875 ≈ ₦993.75; wire amount_raw=1000 (whole ₦)
    # → ledger 100000 (2 dp). Attendant enters 100 for a ₦1000 preset.
    assert (
        coerce_amount_raw_to_2dp(
            volume_raw=53, amount_raw=1000, unit_price_raw=1875
        )
        == 100000
    )


def test_scales_ago_live_ticks_whole_naira() -> None:
    # Live DC2 stream from intelipump-8: amount_raw ≈ volume × (1875/100).
    assert (
        coerce_amount_raw_to_2dp(
            volume_raw=25, amount_raw=469, unit_price_raw=1875
        )
        == 46900
    )
    assert (
        coerce_amount_raw_to_2dp(
            volume_raw=40, amount_raw=750, unit_price_raw=1875
        )
        == 75000
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
