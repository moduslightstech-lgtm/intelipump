"""Dashboard READ_METER must not require a local SQLite pump catalog row."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.channel_map import ChannelMapping
from intelipump_fdc.cloud.command_intake import CloudCommandIntake
from intelipump_fdc.cloud.schemas import CloudCommandInbound
from intelipump_fdc.cloud.topics import TopicBuilder
from intelipump_fdc.controller.meter_read_request import read_meter_read_request
from intelipump_fdc.core.config import MeterReadingSettings
from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.migrations import init_schema


@pytest.fixture
async def db_factory(tmp_path: Path) -> async_sessionmaker[AsyncSession]:
    engine = create_engine(f"sqlite+aiosqlite:///{tmp_path / 'cloud.db'}")
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    yield factory
    await dispose_engine(engine)


@pytest.mark.asyncio
async def test_production_read_meter_uses_channel_map_without_pump_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, db_factory
) -> None:
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    monkeypatch.setenv("INTELIPUMP_METER_READ_REQUEST_DIR", str(tmp_path))

    mqtt = MagicMock()
    mqtt.is_connected = True
    mqtt.subscribe = AsyncMock()
    mqtt.set_message_handler = MagicMock()
    mqtt.publish = AsyncMock()

    mappings = {
        1: ChannelMapping(
            address=1,
            pump_id="pump-6",
            nozzle_id="nozzle-2",
            side_id=None,
            product="PMS",
            source_identifier="pump-6-n2",
        ),
        2: ChannelMapping(
            address=2,
            pump_id="pump-6",
            nozzle_id="nozzle-1",
            side_id=None,
            product="PMS",
            source_identifier="pump-6-n1",
        ),
    }
    intake = CloudCommandIntake(
        session_factory=db_factory,
        mqtt=mqtt,
        topics=TopicBuilder(environment="PRODUCTION"),
        station_id="SAO-Redeemed-Station-1",
        device_id="InteliPump-SAO-RS1-pi-006",
        environment="PRODUCTION",
        simulated=False,
        allow_lab_simulator_commands=False,
        meter_reading_settings=MeterReadingSettings(
            hardware_cd101=True,
            allowed_device_id="InteliPump-SAO-RS1-pi-006",
            allowed_addresses="1,2",
            counter_select=1,
            volume_decimals=3,
        ),
        channel_mappings=mappings,
    )

    now = datetime.now(UTC)
    cmd = CloudCommandInbound(
        commandId="cmd-meter-1",
        correlationId="corr-meter-nozzle-1",
        stationId="SAO-Redeemed-Station-1",
        pumpId="pump-6",
        commandType="READ_METER",
        payload={"nozzleId": "nozzle-1", "readOnly": True},
        createdAt=now,
        expiresAt=now + timedelta(minutes=2),
        simulatorOnly=False,
        environment="PRODUCTION",
        requestedBy="sao_admin@gmail.com",
    )
    result = await intake.handle_command(cmd)

    assert result["executionStatus"] == "PENDING_CONTROLLER"
    assert "pump_not_found" not in (result.get("blockingReasons") or [])
    req = read_meter_read_request()
    assert req is not None
    assert req.dart_address == 2
    assert req.correlation_id == "corr-meter-nozzle-1"


@pytest.mark.asyncio
async def test_production_read_meter_ignores_other_station_pump(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, db_factory
) -> None:
    """Station topic is shared — pump-6 must not act on pump-3 READ_METER."""
    monkeypatch.setenv("INTELIPUMP_SET_PRICE_REQUEST_DIR", str(tmp_path))
    monkeypatch.setenv("INTELIPUMP_METER_READ_REQUEST_DIR", str(tmp_path))

    mqtt = MagicMock()
    mqtt.is_connected = True
    mqtt.subscribe = AsyncMock()
    mqtt.set_message_handler = MagicMock()
    mqtt.publish = AsyncMock()

    mappings = {
        1: ChannelMapping(
            address=1,
            pump_id="pump-6",
            nozzle_id="nozzle-2",
            side_id=None,
            product="PMS",
            source_identifier="pump-6-n2",
        ),
        2: ChannelMapping(
            address=2,
            pump_id="pump-6",
            nozzle_id="nozzle-1",
            side_id=None,
            product="PMS",
            source_identifier="pump-6-n1",
        ),
    }
    intake = CloudCommandIntake(
        session_factory=db_factory,
        mqtt=mqtt,
        topics=TopicBuilder(environment="PRODUCTION"),
        station_id="SAO-Redeemed-Station-1",
        device_id="InteliPump-SAO-RS1-pi-006",
        environment="PRODUCTION",
        simulated=False,
        allow_lab_simulator_commands=False,
        meter_reading_settings=MeterReadingSettings(
            hardware_cd101=True,
            allowed_device_id="InteliPump-SAO-RS1-pi-006",
            allowed_addresses="1,2",
            counter_select=1,
            volume_decimals=3,
        ),
        channel_mappings=mappings,
    )

    now = datetime.now(UTC)
    cmd = CloudCommandInbound(
        commandId="cmd-meter-foreign",
        correlationId="corr-meter-foreign-pump3",
        stationId="SAO-Redeemed-Station-1",
        pumpId="pump-3",
        commandType="READ_METER",
        payload={"nozzleId": "nozzle-2", "readOnly": True},
        createdAt=now,
        expiresAt=now + timedelta(minutes=2),
        simulatorOnly=False,
        environment="PRODUCTION",
        requestedBy="sao_admin@gmail.com",
    )
    result = await intake.handle_command(cmd)

    assert result["executionStatus"] == "IGNORED_OTHER_PUMP"
    assert read_meter_read_request() is None
    mqtt.publish.assert_not_called()
