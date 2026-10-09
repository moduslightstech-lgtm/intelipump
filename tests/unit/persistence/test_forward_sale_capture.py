"""Forward sales capture regressions (SAO pump-2) — no historical recovery.

Covers durable session identity, evidence-backed finalize, equal-value
separateness, LIMIT+hang-up single sale, and outbox enqueue.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from intelipump_fdc.controller.pump_session import PumpSession
from intelipump_fdc.controller.session_events import (
    ControllerEvent,
    ControllerEventType,
    EventBus,
)
from intelipump_fdc.controller.session_models import NozzlePosition
from intelipump_fdc.domain.pump_event import PumpEvent
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.persistence_bridge import PersistenceBridge
from intelipump_fdc.services.persistence_worker import PersistenceWorker
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    FillingUpdateRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService
from intelipump_fdc.state_machine.machine import PumpStateMachine
from intelipump_fdc.state_machine.models import ObservationRef, PumpContext
from intelipump_fdc.state_machine.wayne_mapper import MappedWayneObservation

STATION = "SAO-Redeemed-Station-1"


def _bridge(factory, pump_id: str, *, events: EventBus | None = None) -> PersistenceBridge:
    return PersistenceBridge(
        session_factory=factory,
        station_id=STATION,
        environment="PROD",
        simulated=False,
        worker=PersistenceWorker(),
        pump_id_by_address={2: pump_id},
        logical_by_address={2: "pump-2"},
        events=events or EventBus(),
        mqtt_pump_by_address={2: "pump-2"},
        mqtt_nozzle_by_address={2: "nozzle-1"},
        mqtt_source_by_address={2: "pump-2-n1"},
    )


def _seed_verified(bridge: PersistenceBridge, *, address: int = 2) -> None:
    can_pump, can_nozzle, _ = bridge._channel_identity(address)
    bridge._verified.note_nozzle_lifted(
        pump_id=can_pump, nozzle_id=can_nozzle, dart_address=address
    )
    bridge._verified.note_authorized(
        pump_id=can_pump,
        nozzle_id=can_nozzle,
        dart_address=address,
        baseline_volume_raw=0,
    )
    bridge._verified.note_dc1_state(
        pump_id=can_pump,
        nozzle_id=can_nozzle,
        dc1_state="FILLING",
        dart_address=address,
    )
    bridge._verified.note_volume(
        pump_id=can_pump,
        nozzle_id=can_nozzle,
        volume_raw=5,
        dart_address=address,
    )


async def _dc2(
    bridge: PersistenceBridge,
    *,
    volume: int,
    amount: int,
    address: int = 2,
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
                "raw_price": 1355,
                "price_decimals": 0,
                "selected_nozzle": 1,
            },
        }
    )


async def _complete_event(
    bridge: PersistenceBridge,
    *,
    volume: int,
    amount: int,
    event: str = "LIMIT_REACHED",
    state: PumpState = PumpState.LIMIT_REACHED,
    active_tx: str | None = None,
    may_publish: bool = False,
    filling_seen: bool = True,
    startup_baseline: bool = False,
    address: int = 2,
    version: int = 10,
) -> None:
    await bridge._handle_state_changed(
        {
            "address": address,
            "detail": f"FILLING->{state.value}",
            "payload": {
                "event": event,
                "normalized_state": state.value,
                "previous_state": PumpState.FILLING.value,
                "active_transaction_id": active_tx,
                "selected_nozzle": 1,
                "communication_healthy": True,
                "state_version": version,
                "may_publish_sale": may_publish,
                "sale_lifecycle": "FILLING",
                "filled_volume_raw": volume,
                "filled_amount_raw": amount,
                "filling_price_raw": 1355,
                "unit_price_raw": 1355,
                "filling_seen_this_boot": filling_seen,
                "filling_observed": filling_seen,
                "startup_baseline": startup_baseline,
                "completion_evidence_key": f"complete:frame:{version}",
                "awaiting_filling_complete": False,
            },
        }
    )


async def _begin_active(factory, pump_id: str, uuid: str) -> None:
    async with unit_of_work(factory) as uow:
        await TransactionService(uow).begin(
            BeginTransactionRequest(
                station_id=STATION,
                pump_db_id=pump_id,
                transaction_uuid=uuid,
                nozzle_id=1,
                canonical_pump_id="pump-2",
                canonical_nozzle_id="nozzle-1",
                source_identifier="pump-2-n1",
                raw_price=1355,
                price_decimals=0,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="PROD",
            )
        )


@pytest.mark.asyncio
async def test_null_sm_uuid_completes_mapped_active_and_outbox(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    bus = EventBus()
    bound: list[str] = []

    def _cap(ev: ControllerEvent) -> None:
        if ev.type is ControllerEventType.SALE_IDENTITY_BOUND:
            bound.append(str(ev.payload.get("transaction_uuid")))

    bus.add_subscriber(_cap)

    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
    await _begin_active(factory, pump_id, "tx-forward-1")

    bridge = _bridge(factory, pump_id, events=bus)
    bridge._tx_by_address[2] = "tx-forward-1"
    _seed_verified(bridge)
    await _dc2(bridge, volume=3690, amount=5000000)
    await _complete_event(
        bridge, volume=3690, amount=5000000, active_tx=None, may_publish=False
    )

    async with unit_of_work(factory) as uow:
        row = await uow.transactions.get_by_uuid("tx-forward-1")
        assert row is not None
        assert row.status == "COMPLETED"
        assert row.raw_volume == 3690
        claimed = await uow.sync_queue.claim_batch(limit=20)
        assert any(
            r.event_type == "TRANSACTION_COMPLETED" and r.entity_id == "tx-forward-1"
            for r in claimed
        )


@pytest.mark.asyncio
async def test_startup_retained_face_creates_no_sale(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id

    bridge = _bridge(factory, pump_id)
    await _complete_event(
        bridge,
        volume=1700,
        amount=2303500,
        event="FILLING_COMPLETED",
        state=PumpState.FILLING_COMPLETE,
        may_publish=False,
        filling_seen=False,
        startup_baseline=True,
    )

    async with unit_of_work(factory) as uow:
        assert await uow.transactions.list_unresolved(station_id=STATION) == ()
        assert await uow.transactions.count_completed(station_id=STATION) == 0


@pytest.mark.asyncio
async def test_positive_face_without_session_evidence_no_sale(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id

    bridge = _bridge(factory, pump_id)
    await _complete_event(
        bridge,
        volume=1700,
        amount=2303500,
        event="FILLING_COMPLETED",
        state=PumpState.FILLING_COMPLETE,
        may_publish=True,
        filling_seen=False,
        startup_baseline=False,
    )

    async with unit_of_work(factory) as uow:
        assert await uow.transactions.list_unresolved(station_id=STATION) == ()
        assert await uow.transactions.count_completed(station_id=STATION) == 0


@pytest.mark.asyncio
async def test_new_dispense_distinct_from_unresolved_prior(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
    await _begin_active(factory, pump_id, "tx-prior-active")
    async with unit_of_work(factory) as uow:
        await TransactionService(uow).update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-prior-active",
                raw_volume=1476,
                raw_amount=2000000,
            )
        )

    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-prior-active"
    _seed_verified(bridge)
    await _dc2(bridge, volume=0, amount=0)
    await _dc2(bridge, volume=74, amount=100000)

    async with unit_of_work(factory) as uow:
        prior = await uow.transactions.get_by_uuid("tx-prior-active")
        assert prior is not None
        assert prior.status == "ACTIVE"
        assert prior.raw_volume == 1476
        open_rows = await uow.transactions.list_unresolved(station_id=STATION)
        assert len(open_rows) == 2
        new_rows = [r for r in open_rows if r.transaction_uuid != "tx-prior-active"]
        assert len(new_rows) == 1
        assert new_rows[0].raw_volume == 74
        assert bridge._tx_by_address[2] == new_rows[0].transaction_uuid


@pytest.mark.asyncio
async def test_equal_value_purchases_remain_two_completed(
    engine_factory: tuple,
) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id

    bridge = _bridge(factory, pump_id)
    uuids: list[str] = []
    for i, (vol, amt) in enumerate(((740, 1000000), (740, 1000000)), start=1):
        _seed_verified(bridge)
        await bridge._handle_state_changed(
            {
                "address": 2,
                "detail": "AUTHORIZED->FILLING",
                "payload": {
                    "normalized_state": PumpState.FILLING.value,
                    "previous_state": PumpState.AUTHORIZED.value,
                    "communication_healthy": True,
                    "state_version": i * 10,
                    "selected_nozzle": 1,
                },
            }
        )
        await _dc2(bridge, volume=vol, amount=amt)
        tx = bridge._tx_by_address[2]
        uuids.append(tx)
        await _complete_event(
            bridge,
            volume=vol,
            amount=amt,
            event="FILLING_COMPLETED",
            state=PumpState.FILLING_COMPLETE,
            active_tx=None,
            may_publish=False,
            version=i * 10 + 1,
        )
        bridge._tx_by_address.pop(2, None)

    assert uuids[0] != uuids[1]
    async with unit_of_work(factory) as uow:
        a = await uow.transactions.get_by_uuid(uuids[0])
        b = await uow.transactions.get_by_uuid(uuids[1])
        assert a is not None and b is not None
        assert a.status == "COMPLETED" and b.status == "COMPLETED"
        assert await uow.transactions.count_completed(station_id=STATION) == 2
        claimed = await uow.sync_queue.claim_batch(limit=50)
        entities = {
            r.entity_id
            for r in claimed
            if r.event_type == "TRANSACTION_COMPLETED"
        }
        assert uuids[0] in entities
        assert uuids[1] in entities


@pytest.mark.asyncio
async def test_limit_then_hangup_produces_one_sale(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
    await _begin_active(factory, pump_id, "tx-limit-1")

    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-limit-1"
    _seed_verified(bridge)
    await _dc2(bridge, volume=3690, amount=5000000)
    await _complete_event(
        bridge,
        volume=3690,
        amount=5000000,
        event="LIMIT_REACHED",
        state=PumpState.LIMIT_REACHED,
        may_publish=False,
        version=20,
    )
    _seed_verified(bridge)
    bridge._tx_by_address[2] = "tx-limit-1"
    await _complete_event(
        bridge,
        volume=3690,
        amount=5000000,
        event="FILLING_COMPLETED",
        state=PumpState.FILLING_COMPLETE,
        may_publish=True,
        version=21,
    )

    async with unit_of_work(factory) as uow:
        row = await uow.transactions.get_by_uuid("tx-limit-1")
        assert row is not None and row.status == "COMPLETED"
        assert await uow.transactions.list_unresolved(station_id=STATION) == ()
        assert await uow.transactions.count_completed(station_id=STATION) == 1


@pytest.mark.asyncio
async def test_concurrent_complete_same_uuid_once(engine_factory: tuple) -> None:
    _engine, factory = engine_factory
    async with unit_of_work(factory) as uow:
        pump = await uow.pumps.upsert(
            station_id=STATION, logical_pump_id="pump-2", dart_address=2
        )
        pump_id = pump.id
    await _begin_active(factory, pump_id, "tx-race-1")

    bridge = _bridge(factory, pump_id)
    bridge._tx_by_address[2] = "tx-race-1"
    _seed_verified(bridge)
    await _dc2(bridge, volume=1000, amount=1355000)
    await _complete_event(
        bridge, volume=1000, amount=1355000, may_publish=False, version=30
    )
    await _complete_event(
        bridge, volume=1000, amount=1355000, may_publish=True, version=31
    )

    async with unit_of_work(factory) as uow:
        assert await uow.transactions.count_completed(station_id=STATION) == 1
        # One sale identity; outbox may also carry a price-enrichment replay
        # with a distinct dedupe key — still the same UUID.
        claimed = await uow.sync_queue.claim_batch(limit=50)
        completed_for_uuid = [
            r
            for r in claimed
            if r.event_type == "TRANSACTION_COMPLETED" and r.entity_id == "tx-race-1"
        ]
        assert len(completed_for_uuid) >= 1
        assert {r.entity_id for r in completed_for_uuid} == {"tx-race-1"}


def test_session_binds_and_clears_durable_identity() -> None:
    bus = EventBus()
    session = PumpSession(address=2, pump_id="pump-2", events=bus)
    assert session.machine.context.active_transaction_id is None
    session.bind_durable_sale_identity("tx-bind")
    assert session.machine.context.active_transaction_id == "tx-bind"
    session.clear_durable_sale_identity(expected="tx-bind")
    assert session.machine.context.active_transaction_id is None


def test_holster_bounce_does_not_prematurely_complete() -> None:
    bus = EventBus()
    published: list[dict] = []

    def _cap(event: ControllerEvent) -> None:
        if event.type is ControllerEventType.STATE_CHANGED:
            published.append(dict(event.payload or {}))

    bus.add_subscriber(_cap)
    s = PumpSession(address=2, pump_id="pump-2", events=bus)
    now = datetime.now(UTC)
    s.machine = PumpStateMachine(
        PumpContext(
            pump_id="pump-2",
            dart_address=2,
            current_state=PumpState.FILLING,
            communication_healthy=True,
            nozzle_out=True,
            selected_nozzle=1,
            active_transaction_id="tx-bounce",
            dispensed_volume_raw=9,
        )
    )
    s.state.filled_volume_raw = 9
    s.state.filled_amount_raw = 12200
    s.state.unit_price_raw = 1355
    s.state.nozzle_position = NozzlePosition.OUT
    s._filling_seen_this_boot = True
    s._filling_started_at = now - timedelta(milliseconds=400)
    s.state.sale_evidence.note_nozzle_out()
    s.state.sale_evidence.note_authorized(application_confirmed=True)
    s.state.sale_evidence.note_filling()
    s.state.sale_evidence.note_dc2(volume_raw=9, amount_raw=12200)
    s._noz_in_edge_at = now

    s._apply_mapped(
        MappedWayneObservation(
            event=PumpEvent.FILLING_COMPLETED,
            observation=ObservationRef(source_frame_raw_hex="bounce"),
            raw_wayne_status=5,
            nozzle_out=False,
            completion_evidence_key="complete:bounce:5",
        )
    )
    assert not any(
        p.get("may_publish_sale") is True
        and p.get("event") == PumpEvent.FILLING_COMPLETED.value
        for p in published
    )


def test_short_then_next_sale_uses_distinct_bound_uuid() -> None:
    bus = EventBus()
    s = PumpSession(address=2, pump_id="pump-2", events=bus)
    s.bind_durable_sale_identity("tx-short")
    s.clear_durable_sale_identity(expected="tx-short")
    s.bind_durable_sale_identity("tx-next")
    assert s.machine.context.active_transaction_id == "tx-next"
