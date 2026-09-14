"""Owned-lab / sole-controller dispense CLI gates."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from intelipump_fdc.controller.cli import _owned_lab_dispense_or_exit


def _args(**kwargs):
    base = {
        "confirm_owned_lab_dispense_session": True,
        "confirm_production_sole_controller_dispense": False,
        "enable_active_commands": True,
        "confirm_physical_control_enable": True,
        "mode": "BENCH_CONTROL",
        "price": 1350,
        "confirm_logical_nozzle_mapping": True,
        "confirm_price_scale_raw_bcd": True,
    }
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_lab_owned_session_ok() -> None:
    _owned_lab_dispense_or_exit(_args(), SimpleNamespace(environment="LAB"))


def test_production_refused_without_sole_controller_confirm() -> None:
    with pytest.raises(SystemExit, match="confirm-production-sole-controller-dispense"):
        _owned_lab_dispense_or_exit(
            _args(), SimpleNamespace(environment="PRODUCTION")
        )


def test_production_ok_with_sole_controller_confirm() -> None:
    _owned_lab_dispense_or_exit(
        _args(confirm_production_sole_controller_dispense=True),
        SimpleNamespace(environment="PRODUCTION"),
    )


def test_prod_alias_ok_with_sole_controller_confirm() -> None:
    _owned_lab_dispense_or_exit(
        _args(confirm_production_sole_controller_dispense=True),
        SimpleNamespace(environment="PROD"),
    )
