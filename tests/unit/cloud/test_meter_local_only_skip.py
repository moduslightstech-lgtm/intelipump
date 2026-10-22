"""Local troubleshoot meter reads must not enqueue to cloud."""

from __future__ import annotations

import pytest

from intelipump_fdc.cloud.meter_result_publisher import MeterResultPublisher
from intelipump_fdc.controller.meter_read_request import MeterReadRequest, _parse_request_dict
from intelipump_fdc.core.config import MeterReadingSettings


@pytest.mark.asyncio
async def test_publisher_skips_publish_to_cloud_false(monkeypatch):
    pub = MeterResultPublisher(
        session_factory=None,  # type: ignore[arg-type]
        station_id="SAO",
        device_id="pi-001",
        meter_settings=MeterReadingSettings(),
    )
    called = {"n": 0}

    async def _boom(*_a, **_k):
        called["n"] += 1
        raise AssertionError("must not open unit_of_work for local-only")

    monkeypatch.setattr(
        "intelipump_fdc.cloud.meter_result_publisher.unit_of_work",
        _boom,
    )
    n = await pub._publish_one(
        {
            "correlationId": "local-corr-1",
            "status": "CAPTURED",
            "publishToCloud": False,
            "volumeLiters": 1234.5,
            "dartAddress": 1,
        }
    )
    assert n == 0
    assert called["n"] == 0
    # Second call still skipped via _seen
    n2 = await pub._publish_one(
        {
            "correlationId": "local-corr-1",
            "status": "CAPTURED",
            "publishToCloud": False,
        }
    )
    assert n2 == 0


def test_request_roundtrip_publish_to_cloud_false():
    req = MeterReadRequest(
        correlation_id="c1",
        dart_address=2,
        publish_to_cloud=False,
        notes="troubleshoot",
    )
    parsed = _parse_request_dict(req.to_dict())
    assert parsed is not None
    assert parsed.publish_to_cloud is False
    assert parsed.dart_address == 2


def test_request_defaults_publish_true():
    parsed = _parse_request_dict(
        {"correlationId": "c2", "dartAddress": 1, "counterSelect": 1}
    )
    assert parsed is not None
    assert parsed.publish_to_cloud is True
