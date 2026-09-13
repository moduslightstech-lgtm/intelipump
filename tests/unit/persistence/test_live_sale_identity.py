"""Channel-map identity must be immutable for live sales."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
from intelipump_fdc.services.persistence_worker import PersistenceWorker
from intelipump_fdc.services.pump_state_service import PumpStateService
from intelipump_fdc.state_machine.models import PumpContext

STATION = "InteliPump-US-Lab"


@pytest.mark.asyncio
async def test_address_2_opens_with_pump1_nozzle2_without_wayne_nozzle(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
        await PumpStateService(uow).persist_context(
            pump_db_id=pump_id,
            context=PumpContext(
                pump_id="pump-2",
                dart_address=2,
                current_state=PumpState.FILLING,
                previous_state=PumpState.AUTHORIZED,
                communication_healthy=True,
                state_version=1,
            ),
            observed_at=datetime.now(UTC),
        )

    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="LAB",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_id},
        logical_by_address={2: "pump-2"},
        events=EventBus(),
        mqtt_pump_by_address={2: "pump-1"},
        mqtt_nozzle_by_address={2: "nozzle-2"},
        mqtt_source_by_address={2: "pump-2"},
    )
    bridge._verified.note_nozzle_lifted(
        pump_id="pump-1", nozzle_id="nozzle-2", dart_address=2
    )
    bridge._verified.note_authorized(
        pump_id="pump-1",
        nozzle_id="nozzle-2",
        dart_address=2,
        baseline_volume_raw=0,
    )
    bridge._verified.note_dc1_state(
        pump_id="pump-1", nozzle_id="nozzle-2", dc1_state="FILLING", dart_address=2
    )

    await bridge._handle_app_decoded(
        {
            "address": 2,
            "is_dc2": True,
            "payload": {
                "raw_volume": 8,
                "raw_amount": 9400,
                "volume_decimals": 2,
                "amount_decimals": 2,
                "raw_price": 1175,
                "price_decimals": 2,
                # Wayne selected_nozzle often missing on DC2
            },
        }
    )

    async with unit_of_work(factory) as uow:
        open_rows = await uow.transactions.list_unresolved(station_id=STATION)
        assert len(open_rows) == 1
        row = open_rows[0]
        assert row.canonical_pump_id == "pump-1"
        assert row.canonical_nozzle_id == "nozzle-2"
        assert row.source_identifier == "pump-2"
        assert row.nozzle_id == 2
        snap = await uow.states.latest(pump_id)
        assert snap is not None
        assert snap.normalized_state == PumpState.FILLING.value


@pytest.mark.asyncio
async def test_filling_state_resolves_nozzle_from_channel_map(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id

    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="LAB",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_id},
        logical_by_address={2: "pump-2"},
        events=EventBus(),
        mqtt_pump_by_address={2: "pump-1"},
        mqtt_nozzle_by_address={2: "nozzle-2"},
        mqtt_source_by_address={2: "pump-2"},
    )
    bridge._verified.note_nozzle_lifted(
        pump_id="pump-1", nozzle_id="nozzle-2", dart_address=2
    )
    bridge._verified.note_authorized(
        pump_id="pump-1",
        nozzle_id="nozzle-2",
        dart_address=2,
        baseline_volume_raw=0,
    )

    await bridge._handle_state_changed(
        {
            "address": 2,
            "detail": "AUTHORIZED->FILLING",
            "payload": {
                "normalized_state": PumpState.FILLING.value,
                "previous_state": PumpState.AUTHORIZED.value,
                "communication_healthy": True,
                "state_version": 3,
            },
        }
    )
    await bridge._handle_app_decoded(
        {
            "address": 2,
            "is_dc2": True,
            "payload": {
                "raw_volume": 8,
                "raw_amount": 9400,
                "volume_decimals": 2,
                "amount_decimals": 2,
                "raw_price": 1175,
                "price_decimals": 2,
            },
        }
    )

    async with unit_of_work(factory) as uow:
        open_rows = await uow.transactions.list_unresolved(station_id=STATION)
        assert len(open_rows) == 1
        assert open_rows[0].canonical_pump_id == "pump-1"
        assert open_rows[0].canonical_nozzle_id == "nozzle-2"
        assert open_rows[0].nozzle_id == 2
