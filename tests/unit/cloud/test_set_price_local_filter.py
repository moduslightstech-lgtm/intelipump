"""Ownership of SET_PRICE pumpIds on one-Pi-per-pump devices."""

from __future__ import annotations

from types import SimpleNamespace

from intelipump_fdc.cloud.set_price_ownership import (
    local_set_price_pump_ids,
    owned_logical_pump_ids,
    pump_id_allowed_for_device,
)


def test_device_id_maps_to_pump_n() -> None:
    ids = local_set_price_pump_ids(
        device_id="InteliPump-SAO-RS1-pi-008",
        pumps=[],
    )
    assert "pump-8" in ids
    assert owned_logical_pump_ids(device_id="InteliPump-SAO-RS1-pi-008") == {
        "pump-8",
        "pump-08",
        "pump-008",
    }


def test_hostname_style_device_locks_pump() -> None:
    assert owned_logical_pump_ids(device_id="intelipump-8") == {
        "pump-8",
        "pump-08",
        "pump-008",
    }


def test_ago_product_env_locks_pump8(monkeypatch) -> None:
    monkeypatch.setenv("INTELIPUMP_PRODUCT", "AGO")
    monkeypatch.delenv("INTELIPUMP_OWNED_PUMP_IDS", raising=False)
    monkeypatch.delenv("INTELIPUMP_LOGICAL_PUMP_ID", raising=False)
    assert owned_logical_pump_ids(device_id="unknown-device") == {
        "pump-8",
        "pump-08",
        "pump-008",
    }


def test_sqlite_logical_pump_included_when_unlocked() -> None:
    ids = local_set_price_pump_ids(
        device_id="other",
        pumps=[SimpleNamespace(logical_pump_id="pump-3", id="uuid-3", dart_address=1)],
    )
    assert "pump-3" in ids
    assert "uuid-3" in ids
    assert "1" in ids


def test_ago_pump_id_not_local_on_pms_pi() -> None:
    ids = local_set_price_pump_ids(
        device_id="InteliPump-SAO-RS1-pi-001",
        pumps=[SimpleNamespace(logical_pump_id="pump-1", id="u1", dart_address=1)],
    )
    assert "pump-8" not in ids
    assert "pump-1" in ids
    # One-pi-per-pump must not accept bare dart addresses as pumpIds
    assert "1" not in ids


def test_one_pi_per_pump_ignores_other_station_sqlite_rows() -> None:
    """AGO Pi SQLite may list every station pump — only pump-8 is local."""
    ids = local_set_price_pump_ids(
        device_id="InteliPump-SAO-RS1-pi-008",
        pumps=[
            SimpleNamespace(logical_pump_id="pump-1", id="u1", dart_address=1),
            SimpleNamespace(logical_pump_id="pump-8", id="u8", dart_address=2),
        ],
    )
    assert "pump-8" in ids
    assert "u8" in ids
    assert "pump-1" not in ids
    assert "u1" not in ids
    assert "1" not in ids
    assert "2" not in ids


def test_controller_rejects_foreign_pump_id() -> None:
    assert pump_id_allowed_for_device(
        pump_id="pump-1", device_id="InteliPump-SAO-RS1-pi-008"
    ) is False
    assert pump_id_allowed_for_device(
        pump_id="pump-8", device_id="InteliPump-SAO-RS1-pi-008"
    ) is True
