"""Database initialization and SQLite pragma tests."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from intelipump_fdc.persistence.migrations import verify_sqlite_pragmas
from intelipump_fdc.persistence.models import SCHEMA_VERSION
from intelipump_fdc.persistence.unit_of_work import unit_of_work

STATION = "InteliPump-US-Lab"


@pytest.mark.asyncio
async def test_database_initialization(engine_factory: tuple) -> None:
    engine, factory = engine_factory
    pragmas = await verify_sqlite_pragmas(engine)
    assert pragmas["journal_mode"].upper() == "WAL"
    assert pragmas["foreign_keys"] in {"1", "ON", "on"}
    async with unit_of_work(factory) as uow:
        from intelipump_fdc.persistence.migrations import get_schema_version

        assert await get_schema_version(uow.session) == SCHEMA_VERSION


@pytest.mark.asyncio
async def test_wal_mode_enabled(engine_factory: tuple) -> None:
    engine, _factory = engine_factory
    pragmas = await verify_sqlite_pragmas(engine)
    assert pragmas["journal_mode"].upper() == "WAL"


@pytest.mark.asyncio
async def test_foreign_keys_enabled(engine_factory: tuple) -> None:
    engine, factory = engine_factory
    pragmas = await verify_sqlite_pragmas(engine)
    assert pragmas["foreign_keys"] in {"1", "ON", "on"}
    async with unit_of_work(factory) as uow:
        with pytest.raises(IntegrityError):
            await uow.states.insert_if_meaningful(
                pump_id="missing-pump",
                normalized_state="READY",
                previous_state=None,
                selected_nozzle=None,
                active_transaction_id=None,
                communication_healthy=True,
                raw_wayne_status=None,
                source_frame_ref=None,
                state_version=1,
                observed_at=None,
            )


@pytest.mark.asyncio
async def test_pump_uniqueness_constraints(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-1", dart_address=1
        )
    async with unit_of_work(factory) as uow:
        with pytest.raises(IntegrityError):
            # Same address, different logical id
            from datetime import UTC, datetime
            from uuid import uuid4

            from intelipump_fdc.persistence.models import PumpRow

            uow.session.add(
                PumpRow(
                    id=str(uuid4()),
                    station_id=STATION,
                    logical_pump_id="pump-other",
                    dart_address=1,
                    enabled=True,
                    created_at=datetime.now(UTC),
                    updated_at=datetime.now(UTC),
                )
            )
            await uow.session.flush()
