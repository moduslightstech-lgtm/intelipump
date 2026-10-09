"""Unit tests for once-per-morning OPENING meter capture markers."""

from __future__ import annotations

from pathlib import Path

from intelipump_fdc.controller.meter_startup_capture import (
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
