"""Stable sale fingerprint / completion key helpers."""

from __future__ import annotations

from intelipump_fdc.domain.sale_fingerprint import (
    sale_fingerprint,
    stable_completion_key,
    startup_baseline_completion_key,
)


def test_fingerprint_stable_for_same_face() -> None:
    a = sale_fingerprint(
        station_id="lab",
        dart_address=1,
        nozzle_id=1,
        raw_volume=170,
        raw_amount=20000,
        raw_price=117500,
    )
    b = sale_fingerprint(
        station_id="lab",
        dart_address=1,
        nozzle_id=1,
        raw_volume=170,
        raw_amount=20000,
        raw_price=117500,
    )
    assert a == b
    assert len(a) == 32


def test_fingerprint_differs_by_nozzle() -> None:
    n1 = sale_fingerprint(
        station_id="lab",
        dart_address=1,
        nozzle_id=1,
        raw_volume=170,
        raw_amount=20000,
    )
    n2 = sale_fingerprint(
        station_id="lab",
        dart_address=1,
        nozzle_id=2,
        raw_volume=170,
        raw_amount=20000,
    )
    assert n1 != n2


def test_stable_completion_key_prefers_uuid() -> None:
    fp = "abc"
    assert stable_completion_key(transaction_uuid="tx-1", fingerprint=fp) == "complete:tx-1"
    assert stable_completion_key(transaction_uuid=None, fingerprint=fp) == "complete-fp:abc"
    assert startup_baseline_completion_key(fp).startswith("startup-baseline:")


def test_equal_value_sales_share_fingerprint_but_distinct_completion_keys() -> None:
    """Two consecutive ₦5000 / same litres share a face fingerprint.

    Identity must still differ via transaction UUID so both sales are kept.
    """
    fp_a = sale_fingerprint(
        station_id="lab",
        dart_address=1,
        nozzle_id=1,
        raw_volume=365,
        raw_amount=500000,
        raw_price=137000,
    )
    fp_b = sale_fingerprint(
        station_id="lab",
        dart_address=1,
        nozzle_id=1,
        raw_volume=365,
        raw_amount=500000,
        raw_price=137000,
    )
    assert fp_a == fp_b
    key_a = stable_completion_key(transaction_uuid="sale-uuid-1", fingerprint=fp_a)
    key_b = stable_completion_key(transaction_uuid="sale-uuid-2", fingerprint=fp_b)
    assert key_a != key_b
    assert key_a.startswith("complete:sale-uuid-1")
    assert key_b.startswith("complete:sale-uuid-2")
