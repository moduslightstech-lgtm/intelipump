"""Per-nozzle baseline persistence survives across sessions."""

from __future__ import annotations

from pathlib import Path

import pytest

from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.migrations import init_schema
from intelipump_fdc.persistence.unit_of_work import unit_of_work


@pytest.mark.asyncio
async def test_baseline_survives_restart_and_isolates_nozzles(tmp_path: Path) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'baselines.db'}"
    engine = create_engine(db)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)

    async with unit_of_work(factory) as uow:
        await uow.nozzle_baselines.upsert_baseline(
            station_id="lab",
            pump_id="p1",
            dart_address=1,
            nozzle_id=1,
            fingerprint="fp-n1",
            raw_volume=170,
            raw_amount=20000,
            transaction_uuid="tx-n1",
            mark_published=True,
        )
        await uow.nozzle_baselines.upsert_baseline(
            station_id="lab",
            pump_id="p1",
            dart_address=1,
            nozzle_id=2,
            fingerprint="fp-n2",
            raw_volume=170,
            raw_amount=20000,
            mark_published=True,
        )

    await dispose_engine(engine)

    engine2 = create_engine(db)
    factory2 = create_session_factory(engine2)
    async with unit_of_work(factory2) as uow:
        n1 = await uow.nozzle_baselines.get(station_id="lab", dart_address=1, nozzle_id=1)
        n2 = await uow.nozzle_baselines.get(station_id="lab", dart_address=1, nozzle_id=2)
        assert n1 is not None and n2 is not None
        assert n1.last_published_fingerprint == "fp-n1"
        assert n2.last_published_fingerprint == "fp-n2"
        assert n1.last_transaction_uuid == "tx-n1"
        assert uow.nozzle_baselines.is_already_observed(n1, "fp-n1")
        assert not uow.nozzle_baselines.is_already_observed(n1, "fp-n2")
        assert uow.nozzle_baselines.is_already_observed(n2, "fp-n2")
    await dispose_engine(engine2)
