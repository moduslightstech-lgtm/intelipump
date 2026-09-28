"""SET_PRICE request / durable outcome bridge tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from intelipump_fdc.cloud.set_price_request import (
    SetPriceOutcome,
    SetPriceRequest,
    ack_set_price_outcome,
    consume_set_price_outcome,
    consume_set_price_request,
    list_set_price_outcomes,
    outcome_path,
    parse_prices_from_payload,
    read_set_price_request,
    write_set_price_outcome,
    write_set_price_request,
)


def test_parse_prices_unit_only() -> None:
    unit, prices = parse_prices_from_payload({"unitPriceRaw": 1400})
    assert unit == 1400
    assert prices == (1400,)


def test_parse_prices_list() -> None:
    unit, prices = parse_prices_from_payload({"pricesRaw": [1400, 1400]})
    assert unit == 1400
    assert prices == (1400, 1400)


def test_write_read_consume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_request(
        SetPriceRequest(
            correlation_id="c1",
            command_id="cmd1",
            unit_price_raw=1400,
            prices_raw=(1400,),
            requested_by="admin@example.com",
        )
    )
    peeked = read_set_price_request()
    assert peeked is not None
    assert peeked.unit_price_raw == 1400
    assert (tmp_path / "set-price-request.json").is_file()
    got = consume_set_price_request()
    assert got is not None
    assert got.correlation_id == "c1"
    assert not (tmp_path / "set-price-request.json").is_file()
    assert consume_set_price_request() is None


def test_persisted_unit_price_roundtrip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import (
        read_persisted_unit_price,
        write_persisted_unit_price,
    )

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    assert read_persisted_unit_price() is None
    write_persisted_unit_price(1450, (1450,), source="cloud")
    got = read_persisted_unit_price()
    assert got is not None
    assert got.unit_price_raw == 1450
    assert got.prices_raw == (1450,)
    assert got.source == "cloud"
    assert (tmp_path / "unit-price.json").is_file()


def _outcome(corr: str, pump: str, status: str = "PRICE_CONFIRMED") -> SetPriceOutcome:
    return SetPriceOutcome(
        correlation_id=corr,
        command_id=f"cmd-{corr}",
        station_id="SAO-1",
        pump_id=pump,
        unit_price_raw=1400,
        execution_status=status,
        accepted=status == "PRICE_CONFIRMED",
        applied_addresses=(1,),
        gave_up_addresses=(),
        deferred_addresses=(),
        detail="test",
    )


def test_outcomes_retain_multiple_correlations_without_overwrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_outcome(_outcome("corr-a", "pump-1"))
    write_set_price_outcome(_outcome("corr-b", "pump-2", status="PRICE_FAILED"))
    pending = list_set_price_outcomes()
    assert {o.correlation_id for o in pending} == {"corr-a", "corr-b"}
    assert {o.pump_id for o in pending} == {"pump-1", "pump-2"}
    # ACK one — the other remains.
    assert ack_set_price_outcome("corr-a") is True
    left = list_set_price_outcomes()
    assert len(left) == 1
    assert left[0].correlation_id == "corr-b"
    assert left[0].execution_status == "PRICE_FAILED"


def test_legacy_single_outcome_file_migrates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    legacy = outcome_path()
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(
        '{"correlationId":"legacy-1","commandId":"c","unitPriceRaw":1400,'
        '"executionStatus":"PRICE_CONFIRMED","accepted":true,'
        '"appliedAddresses":[1],"gaveUpAddresses":[],"deferredAddresses":[]}',
        encoding="utf-8",
    )
    pending = list_set_price_outcomes()
    assert len(pending) == 1
    assert pending[0].correlation_id == "legacy-1"
    assert not legacy.is_file()


def test_set_price_outcome_roundtrip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_outcome(_outcome("c-out", "pump-3"))
    got = consume_set_price_outcome()
    assert got is not None
    assert got.correlation_id == "c-out"
    assert got.pump_id == "pump-3"
    assert got.station_id == "SAO-1"
    assert got.execution_status == "PRICE_CONFIRMED"
    assert got.applied_addresses == (1,)
    assert consume_set_price_outcome() is None


@pytest.mark.asyncio
async def test_publish_retains_outcome_until_mqtt_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    from intelipump_fdc.cloud.command_intake import CloudCommandIntake
    from intelipump_fdc.cloud.mqtt.errors import MqttPublishError
    from intelipump_fdc.cloud.mqtt.models import MqttPublishResult
    from intelipump_fdc.cloud.topics import TopicBuilder

    write_set_price_outcome(_outcome("corr-ack", "pump-1"))
    write_set_price_outcome(_outcome("corr-later", "pump-2"))

    mqtt = MagicMock()
    mqtt.is_connected = True
    mqtt.publish = AsyncMock(side_effect=MqttPublishError("publish ack timeout"))

    intake = CloudCommandIntake(
        session_factory=MagicMock(),
        mqtt=mqtt,
        topics=TopicBuilder(environment="PRODUCTION"),
        station_id="SAO-1",
        device_id="pi-001",
        environment="PRODUCTION",
        simulated=False,
        allow_lab_simulator_commands=False,
    )
    published = await intake.publish_pending_set_price_outcomes()
    assert published == 0
    assert {o.correlation_id for o in list_set_price_outcomes()} == {
        "corr-ack",
        "corr-later",
    }

    mqtt.publish = AsyncMock(
        return_value=MqttPublishResult(topic="t", acknowledged=True, mid=1)
    )
    published = await intake.publish_pending_set_price_outcomes()
    assert published == 2
    assert list_set_price_outcomes() == []


@pytest.mark.asyncio
async def test_publish_ordering_stops_on_first_failure_keeps_rest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    from intelipump_fdc.cloud.command_intake import CloudCommandIntake
    from intelipump_fdc.cloud.mqtt.errors import MqttPublishError
    from intelipump_fdc.cloud.mqtt.models import MqttPublishResult
    from intelipump_fdc.cloud.topics import TopicBuilder

    write_set_price_outcome(_outcome("a-first", "pump-1"))
    write_set_price_outcome(_outcome("b-second", "pump-2"))

    mqtt = MagicMock()
    mqtt.is_connected = True
    calls = {"n": 0}

    async def _publish(*_a, **_k):
        calls["n"] += 1
        if calls["n"] == 1:
            return MqttPublishResult(topic="t", acknowledged=True, mid=1)
        raise MqttPublishError("second failed")

    mqtt.publish = AsyncMock(side_effect=_publish)
    intake = CloudCommandIntake(
        session_factory=MagicMock(),
        mqtt=mqtt,
        topics=TopicBuilder(environment="PRODUCTION"),
        station_id="SAO-1",
        device_id="pi-001",
        environment="PRODUCTION",
        simulated=False,
        allow_lab_simulator_commands=False,
    )
    published = await intake.publish_pending_set_price_outcomes()
    assert published == 1
    left = list_set_price_outcomes()
    assert len(left) == 1
    assert left[0].correlation_id == "b-second"
