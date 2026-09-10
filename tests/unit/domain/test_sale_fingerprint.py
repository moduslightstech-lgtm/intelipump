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
