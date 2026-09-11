"""Completion volume must use live DC2, not a stale SM dispensed_volume_raw."""

from __future__ import annotations

from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload


def test_completion_frame_decodes_volume_19_not_42() -> None:
    """Frame from lab SALE with amount 223.25 / price 1175 → 0.19 L.

    Full line frame:
    50 31 02 08 00 00 00 19 00 02 23 25 01 01 05 18 4c 03 fa

    Application payload (after ADR/SEQ):
    02 08 00 00 00 19 00 02 23 25   ← DC2 VOL=19 AMO=22325
    01 01 05                         ← separate DC1 status (not volume)
    """
    bundle = decode_data_payload(
        bytes.fromhex("02 08 00 00 00 19 00 02 23 25 01 01 05"),
        source_frame_raw_hex=(
            "50 31 02 08 00 00 00 19 00 02 23 25 01 01 05 18 4c 03 fa"
        ),
    )
    dc2 = next(
        t
        for t in bundle.transactions
        if (t.decoded_body or {}).get("volume") is not None
    )
    volume = (dc2.decoded_body or {})["volume"]
    amount = (dc2.decoded_body or {})["amount"]
    assert volume["raw_scaled"] == 19
    assert amount["raw_scaled"] == 22325
    assert volume["raw_scaled"] != 42


def test_amount_volume_price_consistent_for_22325() -> None:
    amount_raw = 22325
    volume_raw = 19
    price_raw = 1175  # ₦/L as shown on face
    # amount_raw = volume_raw * price_raw  (both money/volume use 2 dp encoding)
    expected = volume_raw * price_raw
    assert expected == 22325
    assert abs(amount_raw - expected) <= max(price_raw // 100, 1)
