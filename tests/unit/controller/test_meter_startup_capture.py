"""Unit tests for once-per-morning OPENING meter capture markers."""

from __future__ import annotations

from pathlib import Path

from intelipump_fdc.controller.meter_startup_capture import (
    business_date_today,
    business_day_rolled,
    captured_addresses_for_today,
    finished_addresses_for_today,
    load_marker,
    mark_address_captured,
    mark_address_failed,
)


def test_mark_failed_stops_retry_but_not_overwrite_captured(tmp_path: Path) -> None:
    path = tmp_path / "meter-startup-capture.json"
    tz = "UTC"

    mark_address_captured(
        address=1,
        correlation_id="ok-1",
        timezone=tz,
        path=path,
    )
    mark_address_failed(
        address=1,
        correlation_id="fail-ignored",
        status="UNSUPPORTED",
        error_code="METER_DC101_TIMEOUT",
        timezone=tz,
        path=path,
    )
    mark_address_failed(
        address=2,
        correlation_id="fail-2",
        status="UNSUPPORTED",
        error_code="METER_DC101_TIMEOUT",
        timezone=tz,
        path=path,
    )

    assert captured_addresses_for_today(timezone=tz, path=path) == {1}
    assert finished_addresses_for_today(timezone=tz, path=path) == {1, 2}

    data = load_marker(path)
    assert "1" in data["captured"]
    assert "1" not in data.get("failed", {})
    assert data["failed"]["2"]["errorCode"] == "METER_DC101_TIMEOUT"


def test_captured_clears_prior_failed(tmp_path: Path) -> None:
    path = tmp_path / "meter-startup-capture.json"
    tz = "UTC"
    mark_address_failed(
        address=1,
        correlation_id="fail-1",
        status="UNSUPPORTED",
        timezone=tz,
        path=path,
    )
    mark_address_captured(
        address=1,
        correlation_id="ok-1",
        timezone=tz,
        path=path,
    )
    assert captured_addresses_for_today(timezone=tz, path=path) == {1}
    assert finished_addresses_for_today(timezone=tz, path=path) == {1}
    data = load_marker(path)
    assert "1" not in data.get("failed", {})


def test_business_day_rolled_opens_new_morning() -> None:
    tz = "UTC"
    today = business_date_today(timezone=tz)
    rolled, got = business_day_rolled(None, timezone=tz)
    assert rolled is True
    assert got == today

    rolled, got = business_day_rolled(today, timezone=tz)
    assert rolled is False
    assert got == today

    rolled, got = business_day_rolled("1999-01-01", timezone=tz)
    assert rolled is True
    assert got == today


def test_finished_addresses_empty_on_stale_marker_date(tmp_path: Path) -> None:
    """Yesterday's captured/failed must not block today's OPENING queue."""
    path = tmp_path / "meter-startup-capture.json"
    path.write_text(
        '{"businessDate":"1999-01-01","captured":{"1":{}},"failed":{"2":{}}}\n',
        encoding="utf-8",
    )
    assert finished_addresses_for_today(timezone="UTC", path=path) == set()
    assert captured_addresses_for_today(timezone="UTC", path=path) == set()
