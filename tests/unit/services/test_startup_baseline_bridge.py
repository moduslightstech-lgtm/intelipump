"""Persistence bridge suppresses republish of startup baseline faces."""

from __future__ import annotations

from pathlib import Path

import pytest

from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.domain.sale_fingerprint import sale_fingerprint
from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.migrations import init_schema
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
from intelipump_fdc.services.persistence_worker import PersistenceWorker

STATION = "InteliPump-US-Lab"


@pytest.mark.asyncio
async def test_startup_baseline_then_repeated_complete_does_not_create_tx(
    tmp_path: Path,
) -> None:
    db = f"sqlite+aiosqlite:///{tmp_path / 'baseline-bridge.db'}"
    engine = create_engine(db)
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-1", dart_address=1
        )
        pump_db = pump.id

    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="LAB",
        simulated=True,
        worker=PersistenceWorker(maxsize=8),
        pump_id_by_address={1: pump_db},
        logical_by_address={1: "pump-1"},
        events=EventBus(),
        automatic_transaction_publishing=True,
    )

    fp = sale_fingerprint(
        station_id=STATION,
        dart_address=1,
        nozzle_id=1,
        raw_volume=170,
        raw_amount=20000,
        raw_price=117500,
    )

    await bridge._handle_state_changed(
        {
            "address": 1,
            "detail": "startup_baseline",
            "payload": {
                "event": "STARTUP_BASELINE_OBSERVED",
                "startup_baseline": True,
                "selected_nozzle": 1,
                "filled_volume_raw": 170,
                "filled_amount_raw": 20000,
                "filling_price_raw": 117500,
                "normalized_state": "FILLING_COMPLETE",
                "may_publish_sale": False,
            },
        }
    )

    async with unit_of_work(factory) as uow:
        baseline = await uow.nozzle_baselines.get(
            station_id=STATION, dart_address=1, nozzle_id=1
        )
        assert baseline is not None
        assert uow.nozzle_baselines.is_already_observed(baseline, fp)
        assert await uow.transactions.count_completed(station_id=STATION) == 0

    await bridge._handle_state_changed(
        {
            "address": 1,
            "detail": "repeated_complete",
            "payload": {
                "event": "FILLING_COMPLETED",
                "previous_state": "FILLING_COMPLETE",
                "normalized_state": "FILLING_COMPLETE",
                "selected_nozzle": 1,
                "filled_volume_raw": 170,
                "filled_amount_raw": 20000,
                "filling_price_raw": 117500,
                "awaiting_filling_complete": False,
                "active_transaction_id": None,
                "may_publish_sale": True,
            },
        }
    )

    async with unit_of_work(factory) as uow:
        assert await uow.transactions.count_completed(station_id=STATION) == 0

    await dispose_engine(engine)
