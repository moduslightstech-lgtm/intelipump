"""SET_PRICE request / durable outcome bridge tests."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from intelipump_fdc.cloud.set_price_request import (
    SetPriceOutcome,
    SetPriceRequest,
    ack_set_price_outcome,
    consume_set_price_outcome,
    consume_set_price_request,
    has_set_price_outcome,
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


def test_pending_verify_survives_outcome_ack_and_supersede(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from intelipump_fdc.cloud.set_price_request import (
        SetPricePendingVerify,
        clear_set_price_pending_verify,
        list_set_price_pending_verifies,
        read_set_price_pending_verify,
        supersede_set_price_pending_verifies,
        write_set_price_pending_verify,
    )

    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    write_set_price_pending_verify(
        SetPricePendingVerify(
            correlation_id="corr-pv",
            command_id="cmd-pv",
            station_id=None,
            pump_id="pump-1",
            unit_price_raw=1370,
            required_addresses=(1, 2),
            verified_addresses=(1,),
            emitted_verified_addresses=(1,),
            outcome_revision=1,
        )
    )
    got = read_set_price_pending_verify("corr-pv")
    assert got is not None
    assert got.unit_price_raw == 1370
    assert set(got.verified_addresses) == {1}
    write_set_price_outcome(_outcome("corr-pv", "pump-1", status="PRICE_PARTIAL"))
    assert ack_set_price_outcome("corr-pv") is True
    # Outcome gone; pending-verify retained.
    assert list_set_price_outcomes() == []
    assert read_set_price_pending_verify("corr-pv") is not None
    assert supersede_set_price_pending_verifies(except_correlation_id="corr-new") == 1
    assert list_set_price_pending_verifies() == []
    assert clear_set_price_pending_verify("corr-pv") is False


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


@pytest.mark.asyncio
async def test_publish_ack_between_outcome_write_and_request_removal_keeps_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cloud-sync must not remove the outcome while the request can still crash-survive."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    from intelipump_fdc.cloud.command_intake import CloudCommandIntake
    from intelipump_fdc.cloud.mqtt.models import MqttPublishResult
    from intelipump_fdc.cloud.topics import TopicBuilder
    from intelipump_fdc.cloud.set_price_request import (
        consume_set_price_request_durable,
        pending_request_blocks_outcome_ack,
    )

    # Finalize window: durable outcome written, request not yet cleared.
    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-race",
            command_id="cmd-race",
            unit_price_raw=1400,
            prices_raw=(1400,),
            pump_id="pump-1",
        )
    )
    write_set_price_outcome(_outcome("corr-race", "pump-1"))
    assert pending_request_blocks_outcome_ack("corr-race") is True

    mqtt = MagicMock()
    mqtt.is_connected = True
    mqtt.publish = AsyncMock(
        return_value=MqttPublishResult(topic="t", acknowledged=True, mid=1)
    )
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
    mqtt.publish.assert_not_called()
    assert {o.correlation_id for o in list_set_price_outcomes()} == {"corr-race"}
    assert read_set_price_request() is not None

    # Controller finishes: durable request clear after durable outcome.
    assert consume_set_price_request_durable() is not None
    assert pending_request_blocks_outcome_ack("corr-race") is False

    published = await intake.publish_pending_set_price_outcomes()
    assert published == 1
    mqtt.publish.assert_called_once()
    assert list_set_price_outcomes() == []


@pytest.mark.asyncio
async def test_publish_ack_after_mqtt_still_defers_if_request_reappears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the request is still present at ACK time, keep the outcome on disk."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    from intelipump_fdc.cloud.command_intake import CloudCommandIntake
    from intelipump_fdc.cloud.mqtt.models import MqttPublishResult
    from intelipump_fdc.cloud.topics import TopicBuilder

    write_set_price_outcome(_outcome("corr-late", "pump-2"))

    mqtt = MagicMock()
    mqtt.is_connected = True

    async def _publish_then_recreate_request(*_a, **_k):
        # Simulate the race: between publish start and ACK-delete, the
        # controller has written outcome but not yet cleared the request —
        # or a concurrent reader still sees it. Recreate request mid-publish.
        write_set_price_request(
            SetPriceRequest(
                correlation_id="corr-late",
                command_id="cmd-late",
                unit_price_raw=1400,
                prices_raw=(1400,),
                pump_id="pump-2",
            )
        )
        return MqttPublishResult(topic="t", acknowledged=True, mid=7)

    mqtt.publish = AsyncMock(side_effect=_publish_then_recreate_request)
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
    assert {o.correlation_id for o in list_set_price_outcomes()} == {"corr-late"}


def test_outcome_write_sync_failure_raises_and_leaves_no_corrupt_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    import intelipump_fdc.cloud.set_price_request as spr

    real_fsync = os.fsync
    calls = {"n": 0}

    def _fsync_fail(fd: int) -> None:
        calls["n"] += 1
        # Fail the file fsync (first call); dir fsync would be second.
        if calls["n"] == 1:
            raise OSError("simulated fsync failure")
        return real_fsync(fd)

    monkeypatch.setattr(spr.os, "fsync", _fsync_fail)
    with pytest.raises(OSError, match="simulated fsync failure"):
        write_set_price_outcome(_outcome("corr-sync-fail", "pump-1"))
    assert list_set_price_outcomes() == []
    assert has_set_price_outcome("corr-sync-fail") is False
    outcomes = tmp_path / "set-price-outcomes"
    if outcomes.is_dir():
        assert list(outcomes.glob("*.json")) == []


def test_outcome_dir_sync_failure_after_replace_not_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Post-replace directory sync failure must not leave a durable marker."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    import intelipump_fdc.cloud.set_price_request as spr

    real_fsync = os.fsync
    calls = {"n": 0}

    def _fsync_fail_dir(fd: int) -> None:
        calls["n"] += 1
        # 1 = file fsync on tmp, 2 = dir fsync after replace
        if calls["n"] == 2:
            raise OSError("simulated dir fsync failure")
        return real_fsync(fd)

    monkeypatch.setattr(spr.os, "fsync", _fsync_fail_dir)
    with pytest.raises(OSError, match="simulated dir fsync failure"):
        write_set_price_outcome(_outcome("corr-dir-sync", "pump-1"))
    assert has_set_price_outcome("corr-dir-sync") is False
    assert list_set_price_outcomes() == []


def test_request_unlink_dir_sync_failure_restores_request_retains_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    import intelipump_fdc.cloud.set_price_request as spr
    from intelipump_fdc.cloud.set_price_request import (
        consume_set_price_request_durable,
        has_ack_hold,
        pending_request_blocks_outcome_ack,
    )

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-unlink-sync",
            command_id="cmd-unlink-sync",
            unit_price_raw=1400,
            prices_raw=(1400,),
            pump_id="pump-1",
        )
    )
    write_set_price_outcome(_outcome("corr-unlink-sync", "pump-1"))

    real_fsync_dir = spr._fsync_dir
    calls = {"n": 0}

    def _fail_unlink_dir_only(path: Path) -> None:
        calls["n"] += 1
        # First call: post-unlink dir sync. Later calls: durable restore.
        if calls["n"] == 1:
            raise OSError("simulated request dir fsync failure")
        return real_fsync_dir(path)

    monkeypatch.setattr(spr, "_fsync_dir", _fail_unlink_dir_only)
    with pytest.raises(OSError, match="request restored durably"):
        consume_set_price_request_durable()

    assert read_set_price_request() is not None
    assert read_set_price_request().correlation_id == "corr-unlink-sync"
    assert has_set_price_outcome("corr-unlink-sync") is True
    assert has_ack_hold("corr-unlink-sync") is False
    assert pending_request_blocks_outcome_ack("corr-unlink-sync") is True


@pytest.mark.asyncio
async def test_unlink_and_restore_sync_both_fail_retains_durable_ack_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If unlink sync and durable restore both fail, keep a durable ACK-hold."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    import intelipump_fdc.cloud.set_price_request as spr
    from intelipump_fdc.cloud.command_intake import CloudCommandIntake
    from intelipump_fdc.cloud.mqtt.models import MqttPublishResult
    from intelipump_fdc.cloud.topics import TopicBuilder
    from intelipump_fdc.cloud.set_price_request import (
        consume_set_price_request_durable,
        has_ack_hold,
        pending_request_blocks_outcome_ack,
    )

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-both-fail",
            command_id="cmd-both-fail",
            unit_price_raw=1425,
            prices_raw=(1425,),
            pump_id="pump-9",
        )
    )
    write_set_price_outcome(_outcome("corr-both-fail", "pump-9"))

    real_fsync_dir = spr._fsync_dir

    def _fail_request_dir_only(path: Path) -> None:
        # Fail sync of the request directory itself; allow ACK-hold subdir sync.
        if path.parent.resolve() == spr.request_dir().resolve():
            raise OSError("request dir sync failure")
        return real_fsync_dir(path)

    monkeypatch.setattr(spr, "_fsync_dir", _fail_request_dir_only)
    with pytest.raises(OSError, match="ACK-hold retained"):
        consume_set_price_request_durable()

    assert has_ack_hold("corr-both-fail") is True
    assert has_set_price_outcome("corr-both-fail") is True
    assert pending_request_blocks_outcome_ack("corr-both-fail") is True
    # Durable restore rolled back the non-durable request file.
    assert read_set_price_request() is None

    mqtt = MagicMock()
    mqtt.is_connected = True
    mqtt.publish = AsyncMock(
        return_value=MqttPublishResult(topic="t", acknowledged=True, mid=11)
    )
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
    mqtt.publish.assert_not_called()
    assert has_set_price_outcome("corr-both-fail") is True


@pytest.mark.asyncio
async def test_publish_after_durable_unlink_can_ack_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After durable request removal, cloud-sync may publish and ACK-delete."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    from intelipump_fdc.cloud.command_intake import CloudCommandIntake
    from intelipump_fdc.cloud.mqtt.models import MqttPublishResult
    from intelipump_fdc.cloud.topics import TopicBuilder
    from intelipump_fdc.cloud.set_price_request import (
        consume_set_price_request_durable,
        pending_request_blocks_outcome_ack,
    )

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-after-unlink",
            command_id="cmd-after-unlink",
            unit_price_raw=1400,
            prices_raw=(1400,),
            pump_id="pump-3",
        )
    )
    write_set_price_outcome(_outcome("corr-after-unlink", "pump-3"))

    assert consume_set_price_request_durable() is not None
    assert read_set_price_request() is None
    assert pending_request_blocks_outcome_ack("corr-after-unlink") is False

    mqtt = MagicMock()
    mqtt.is_connected = True
    mqtt.publish = AsyncMock(
        return_value=MqttPublishResult(topic="t", acknowledged=True, mid=3)
    )
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
    assert list_set_price_outcomes() == []


@pytest.mark.asyncio
async def test_publish_blocked_when_request_unlink_sync_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Crash window: unlink appeared but dir sync failed → request restored, no ACK."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    import intelipump_fdc.cloud.set_price_request as spr
    from intelipump_fdc.cloud.command_intake import CloudCommandIntake
    from intelipump_fdc.cloud.mqtt.models import MqttPublishResult
    from intelipump_fdc.cloud.topics import TopicBuilder
    from intelipump_fdc.cloud.set_price_request import consume_set_price_request_durable

    write_set_price_request(
        SetPriceRequest(
            correlation_id="corr-crash-window",
            command_id="cmd-crash-window",
            unit_price_raw=1410,
            prices_raw=(1410,),
            pump_id="pump-4",
        )
    )
    write_set_price_outcome(_outcome("corr-crash-window", "pump-4"))

    real_fsync_dir = spr._fsync_dir
    calls = {"n": 0}

    def _fail_first_dir_sync(path: Path) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("dir sync after unlink")
        return real_fsync_dir(path)

    monkeypatch.setattr(spr, "_fsync_dir", _fail_first_dir_sync)
    with pytest.raises(OSError, match="request restored durably"):
        consume_set_price_request_durable()

    mqtt = MagicMock()
    mqtt.is_connected = True
    mqtt.publish = AsyncMock(
        return_value=MqttPublishResult(topic="t", acknowledged=True, mid=9)
    )
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
    mqtt.publish.assert_not_called()
    assert read_set_price_request() is not None
    assert has_set_price_outcome("corr-crash-window") is True
