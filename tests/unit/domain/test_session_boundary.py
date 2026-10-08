"""Meter RESET face detection — never high-water across physical sessions."""

from __future__ import annotations

from intelipump_fdc.domain.session_boundary import (
    is_new_physical_session,
    session_boundary_reason,
)


def test_growth_within_session_is_not_new() -> None:
    assert not is_new_physical_session(
        prior_volume=111,
        prior_amount=150405,
        new_volume=517,
        new_amount=700535,
    )


def test_meter_reset_1476_to_74_is_new_session() -> None:
    assert is_new_physical_session(
        prior_volume=1476,
        prior_amount=2000000,
        new_volume=74,
        new_amount=100000,
    )
    assert session_boundary_reason(
        prior_volume=1476,
        prior_amount=2000000,
        new_volume=74,
        new_amount=100000,
    )


def test_meter_reset_517_to_near_zero_is_new_session() -> None:
    assert is_new_physical_session(
        prior_volume=517,
        prior_amount=700535,
        new_volume=5,
        new_amount=6775,
    )


def test_tiny_noise_drop_not_new_session() -> None:
    assert not is_new_physical_session(
        prior_volume=100,
        prior_amount=135500,
        new_volume=98,
        new_amount=132790,
    )


def test_empty_prior_not_new() -> None:
    assert not is_new_physical_session(
        prior_volume=0,
        prior_amount=0,
        new_volume=10,
        new_amount=13550,
    )
