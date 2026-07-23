"""State snapshot repository tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from intelipump_fdc.persistence.unit_of_work import unit_of_work

STATION = "InteliPump-US-Lab"


async def _pump(factory) -> str:  # type: ignore[no-untyped-def]
    async with unit_of_work(factory) as uow:
        p = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-1", dart_address=1
        )
        return p.id


@pytest.mark.asyncio
async def test_latest_pump_state_query(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        await uow.states.insert_if_meaningful(
            pump_id=pump_id,
            normalized_state="READY",
            previous_state="DISCONNECTED",
            selected_nozzle=None,
            active_transaction_id=None,
            communication_healthy=True,
            raw_wayne_status=1,
            source_frame_ref="aa",
            state_version=1,
            observed_at=datetime.now(UTC),
        )
        await uow.states.insert_if_meaningful(
            pump_id=pump_id,
            normalized_state="NOZZLE_UP",
            previous_state="READY",
            selected_nozzle=1,
            active_transaction_id=None,
            communication_healthy=True,
            raw_wayne_status=2,
            source_frame_ref="bb",
            state_version=2,
            observed_at=datetime.now(UTC),
        )
        latest = await uow.states.latest(pump_id)
        assert latest is not None
        assert latest.state_version == 2
        assert latest.normalized_state == "NOZZLE_UP"


@pytest.mark.asyncio
async def test_state_version_monotonicity(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        await uow.states.insert_if_meaningful(
            pump_id=pump_id,
            normalized_state="READY",
            previous_state=None,
            selected_nozzle=None,
            active_transaction_id=None,
            communication_healthy=True,
            raw_wayne_status=None,
            source_frame_ref=None,
            state_version=5,
            observed_at=None,
        )
        # Older version rejected
        assert (
            await uow.states.insert_if_meaningful(
                pump_id=pump_id,
                normalized_state="FILLING",
                previous_state="READY",
                selected_nozzle=1,
                active_transaction_id="t1",
                communication_healthy=True,
                raw_wayne_status=3,
                source_frame_ref=None,
                state_version=4,
                observed_at=None,
            )
            is None
        )
        latest = await uow.states.latest(pump_id)
        assert latest is not None
        assert latest.state_version == 5


@pytest.mark.asyncio
async def test_duplicate_snapshot_suppression(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    pump_id = await _pump(factory)
    async with unit_of_work(factory) as uow:
        kwargs = dict(
            pump_id=pump_id,
            normalized_state="READY",
            previous_state=None,
            selected_nozzle=None,
            active_transaction_id=None,
            communication_healthy=True,
            raw_wayne_status=1,
            source_frame_ref="x",
            state_version=1,
            observed_at=None,
        )
        assert await uow.states.insert_if_meaningful(**kwargs) is not None
        assert await uow.states.insert_if_meaningful(**kwargs) is None
        snaps = await uow.states.list_for_pump(pump_id)
        assert len(snaps) == 1
