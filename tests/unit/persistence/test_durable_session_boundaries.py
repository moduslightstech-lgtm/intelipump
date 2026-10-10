"""Physical session identities: no high-water glue across meter RESET.

Asserts the Oct-8 pump-3 glue cases as regression targets:
- 1.11 + 5.17 + 0.74 L → three identities, total 7.02 L
- 14.76 + 0.74 L → two identities, total 15.50 L
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from intelipump_fdc.controller.session_events import EventBus
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
from intelipump_fdc.services.persistence_worker import PersistenceWorker
from intelipump_fdc.services.pump_state_service import PumpStateService
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
    FillingUpdateRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService
from intelipump_fdc.state_machine.models import PumpContext

STATION = "InteliPump-US-Lab"
PRICE = 1355


def _seed_verified(
    bridge: PersistenceBridge,
    *,
    address: int = 2,
    pump_id: str = "pump-3",
    nozzle_id: str = "nozzle-2",
    baseline: int = 0,
) -> None:
    can_pump = bridge._mqtt_pump_by_address.get(
        address, bridge._logical_by_address.get(address, pump_id)
    )
    can_nozzle = bridge._mqtt_nozzle_by_address.get(address) or nozzle_id
    # New lift resets the verified book (clears prior transaction_id).
    bridge._verified.note_nozzle_lifted(
        pump_id=can_pump, nozzle_id=can_nozzle, dart_address=address
    )
    bridge._verified.note_authorized(
        pump_id=can_pump,
        nozzle_id=can_nozzle,
        dart_address=address,
        baseline_volume_raw=baseline,
    )


async def _begin_filling(
    bridge: PersistenceBridge,
    *,
    address: int = 2,
    tx: str | None = None,
    version: int = 1,
) -> None:
    await bridge._handle_state_changed(
        {
            "address": address,
            "detail": "AUTHORIZED->FILLING",
            "payload": {
                "normalized_state": PumpState.FILLING.value,
                "previous_state": PumpState.AUTHORIZED.value,
                "active_transaction_id": tx,
                "selected_nozzle": 2,
                "communication_healthy": True,
                "state_version": version,
            },
        }
    )


async def _dc2(
    bridge: PersistenceBridge,
    *,
    address: int,
    volume: int,
    amount: int,
) -> None:
    await bridge._handle_app_decoded(
        {
            "address": address,
            "is_dc2": True,
            "payload": {
                "raw_volume": volume,
                "raw_amount": amount,
                "volume_decimals": 2,
                "amount_decimals": 2,
                "raw_price": PRICE,
                "price_decimals": 0,
                "selected_nozzle": 2,
            },
        }
    )


async def _hangup_complete(
    bridge: PersistenceBridge,
    *,
    address: int,
    volume: int,
    amount: int,
    tx_uuid: str,
    version: int = 9,
) -> None:
    await bridge._handle_state_changed(
        {
            "address": address,
            "detail": "FILLING->FILLING_COMPLETE",
            "payload": {
                "normalized_state": PumpState.FILLING_COMPLETE.value,
                "previous_state": PumpState.FILLING.value,
                "active_transaction_id": tx_uuid,
                "selected_nozzle": 2,
                "communication_healthy": True,
                "state_version": version,
                "event": "FILLING_COMPLETED",
                "awaiting_filling_complete": False,
                "filled_volume_raw": volume,
                "filled_amount_raw": amount,
                "raw_price": PRICE,
                "price_decimals": 0,
                "sale_lifecycle": "FILLING_COMPLETED",
                "may_publish_sale": True,
                "filling_seen_this_boot": True,
            },
        }
    )


@pytest.mark.asyncio
async def test_three_sessions_111_517_074_distinct_identities(
    engine_factory: tuple,
) -> None:
    """1.11 + 5.17 + 0.74 L → three UUIDs; total 7.02 L."""
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-3", dart_address=2
        )
        pump_db = pump.id

    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="PROD",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_db},
        logical_by_address={2: "pump-3"},
        events=EventBus(),
        mqtt_pump_by_address={2: "pump-3"},
        mqtt_nozzle_by_address={2: "nozzle-2"},
        mqtt_source_by_address={2: "pump-3-n2"},
    )

    sessions = ((111, 150405), (517, 700535), (74, 100270))
    uuids: list[str] = []
    for idx, (vol, amt) in enumerate(sessions):
        _seed_verified(bridge, address=2, baseline=0)
        await _begin_filling(bridge, address=2, version=10 + idx)
        await _dc2(bridge, address=2, volume=vol, amount=amt)
        tx = bridge._tx_by_address[2]
        assert tx not in uuids
        uuids.append(tx)
        await _hangup_complete(
            bridge,
            address=2,
            volume=vol,
            amount=amt,
            tx_uuid=tx,
            version=20 + idx,
        )

    assert len(uuids) == 3
    assert len(set(uuids)) == 3

    async with unit_of_work(factory) as uow:
        rows = []
        for uid in uuids:
            row = await uow.transactions.get_by_uuid(uid)
            assert row is not None
            assert row.status == "COMPLETED"
            rows.append(row)
        volumes = sorted(int(r.raw_volume or 0) for r in rows)
        assert volumes == [74, 111, 517]
        assert sum(volumes) == 702  # 7.02 L at 2 dp


@pytest.mark.asyncio
async def test_meter_reset_1476_then_074_two_identities(
    engine_factory: tuple,
) -> None:
    """14.76 L hung ACTIVE then meter RESET to 0.74 → two identities, 15.50 L."""
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-3", dart_address=2
        )
        pump_db = pump.id

    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="PROD",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_db},
        logical_by_address={2: "pump-3"},
        events=EventBus(),
        mqtt_pump_by_address={2: "pump-3"},
        mqtt_nozzle_by_address={2: "nozzle-2"},
        mqtt_source_by_address={2: "pump-3-n2"},
    )

    _seed_verified(bridge, address=2, baseline=0)
    await _begin_filling(bridge, address=2, version=1)
    await _dc2(bridge, address=2, volume=1476, amount=2000000)
    first = bridge._tx_by_address[2]
    assert first

    # New physical session after RESET without hang-up — face drops; prior
    # must finalize (COMPLETED + outbox) so the sale still reaches the cloud.
    _seed_verified(bridge, address=2, baseline=0)
    await _begin_filling(bridge, address=2, version=2)
    await _dc2(bridge, address=2, volume=74, amount=100270)
    second = bridge._tx_by_address[2]
    assert second != first

    await _hangup_complete(
        bridge, address=2, volume=74, amount=100270, tx_uuid=second, version=3
    )

    async with unit_of_work(factory) as uow:
        prior = await uow.transactions.get_by_uuid(first)
        sold = await uow.transactions.get_by_uuid(second)
        assert prior is not None
        assert prior.status == "COMPLETED"
        assert prior.raw_volume == 1476
        assert sold is not None
        assert sold.status == "COMPLETED"
        assert sold.raw_volume == 74
        assert int(prior.raw_volume or 0) + int(sold.raw_volume or 0) == 1550
        claimed = await uow.sync_queue.claim_batch(limit=50)
        prior_outbox = [
            row
            for row in claimed
            if row.entity_id == first and row.event_type == "TRANSACTION_COMPLETED"
        ]
        assert prior_outbox, "prior sale must be queued for cloud after session boundary"


@pytest.mark.asyncio
async def test_growth_within_one_session_keeps_one_identity(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-3", dart_address=2
        )
        pump_db = pump.id

    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="PROD",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_db},
        logical_by_address={2: "pump-3"},
        events=EventBus(),
        mqtt_pump_by_address={2: "pump-3"},
        mqtt_nozzle_by_address={2: "nozzle-2"},
        mqtt_source_by_address={2: "pump-3-n2"},
    )
    _seed_verified(bridge, address=2, baseline=0)
    await _begin_filling(bridge, address=2, version=1)
    await _dc2(bridge, address=2, volume=100, amount=135500)
    uid = bridge._tx_by_address[2]
    await _dc2(bridge, address=2, volume=250, amount=338750)
    await _dc2(bridge, address=2, volume=517, amount=700535)
    assert bridge._tx_by_address[2] == uid
    async with unit_of_work(factory) as uow:
        open_rows = await uow.transactions.list_unresolved(station_id=STATION)
        assert len(open_rows) == 1
        assert open_rows[0].transaction_uuid == uid
        assert open_rows[0].raw_volume == 517


@pytest.mark.asyncio
async def test_equal_value_consecutive_purchases_remain_distinct(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-3", dart_address=2
        )
        pump_db = pump.id

    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="PROD",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_db},
        logical_by_address={2: "pump-3"},
        events=EventBus(),
        mqtt_pump_by_address={2: "pump-3"},
        mqtt_nozzle_by_address={2: "nozzle-2"},
        mqtt_source_by_address={2: "pump-3-n2"},
    )
    uuids: list[str] = []
    for idx in range(2):
        _seed_verified(bridge, address=2, baseline=0)
        await _begin_filling(bridge, address=2, version=10 + idx)
        await _dc2(bridge, address=2, volume=74, amount=100270)
        tx = bridge._tx_by_address[2]
        uuids.append(tx)
        await _hangup_complete(
            bridge,
            address=2,
            volume=74,
            amount=100270,
            tx_uuid=tx,
            version=20 + idx,
        )
    assert uuids[0] != uuids[1]
    async with unit_of_work(factory) as uow:
        for uid in uuids:
            row = await uow.transactions.get_by_uuid(uid)
            assert row is not None
            assert row.status == "COMPLETED"
            assert row.raw_volume == 74


@pytest.mark.asyncio
async def test_provisional_sidecar_meter_reset_mints_new_uuid(
    engine_factory: tuple,
) -> None:
    """Legacy sidecar-settle COMPLETED + meter RESET → new identity (no reopen glue)."""
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-3", dart_address=2
        )
        pump_db = pump.id
        await PumpStateService(uow).persist_context(
            pump_db_id=pump_db,
            context=PumpContext(
                pump_id="pump-3",
                dart_address=2,
                current_state=PumpState.FILLING,
                communication_healthy=True,
                state_version=1,
                selected_nozzle=2,
            ),
            observed_at=datetime.now(UTC),
        )
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_db,
                transaction_uuid="tx-prov-1476",
                nozzle_id=2,
                raw_price=PRICE,
                price_decimals=0,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="PROD",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-prov-1476",
                raw_volume=1476,
                raw_amount=2000000,
                event_key="fill:tx-prov-1476:1476:2000000",
            )
        )
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-prov-1476",
                source_completion_key="sidecar-settle:tx-prov-1476",
                raw_volume=1476,
                raw_amount=2000000,
                completion_inferred=True,
            )
        )

    bridge = PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="PROD",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_db},
        logical_by_address={2: "pump-3"},
        events=EventBus(),
        mqtt_pump_by_address={2: "pump-3"},
        mqtt_nozzle_by_address={2: "nozzle-2"},
        mqtt_source_by_address={2: "pump-3-n2"},
    )
    bridge._tx_by_address[2] = "tx-prov-1476"
    _seed_verified(bridge, address=2, baseline=0)
    await _dc2(bridge, address=2, volume=74, amount=100270)
    new_uuid = bridge._tx_by_address[2]
    assert new_uuid != "tx-prov-1476"

    async with unit_of_work(factory) as uow:
        prior = await uow.transactions.get_by_uuid("tx-prov-1476")
        assert prior is not None
        assert prior.status == "COMPLETED"
        assert prior.raw_volume == 1476
        opened = await uow.transactions.get_by_uuid(new_uuid)
        assert opened is not None
        assert opened.status == "ACTIVE"
        assert opened.raw_volume == 74
