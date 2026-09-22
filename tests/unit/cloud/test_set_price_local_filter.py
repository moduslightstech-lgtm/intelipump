"""SET_PRICE is only queued on the Pi that owns the target pumpId."""

from __future__ import annotations

from types import SimpleNamespace

from intelipump_fdc.cloud.command_intake import _local_set_price_pump_ids


def test_device_id_maps_to_pump_n() -> None:
    ids = _local_set_price_pump_ids(
        device_id="InteliPump-SAO-RS1-pi-008",
        pumps=[],
    )
    assert "pump-8" in ids


def test_sqlite_logical_pump_included() -> None:
    ids = _local_set_price_pump_ids(
        device_id="other",
        pumps=[SimpleNamespace(logical_pump_id="pump-3", id="uuid-3", dart_address=1)],
    )
    assert "pump-3" in ids
    assert "uuid-3" in ids
    assert "1" in ids


def test_ago_pump_id_not_local_on_pms_pi() -> None:
    ids = _local_set_price_pump_ids(
        device_id="InteliPump-SAO-RS1-pi-001",
        pumps=[SimpleNamespace(logical_pump_id="pump-1", id="u1", dart_address=1)],
    )
    assert "pump-8" not in ids
    assert "pump-1" in ids


def test_one_pi_per_pump_ignores_other_station_sqlite_rows() -> None:
    """AGO Pi SQLite may list every station pump — only pump-8 is local."""
    ids = _local_set_price_pump_ids(
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
