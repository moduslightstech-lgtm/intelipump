"""Phase 9 cloud / MQTT unit tests (fake broker)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from intelipump_fdc.cloud.backoff import compute_backoff_seconds
from intelipump_fdc.cloud.cli import build_parser
from intelipump_fdc.cloud.command_intake import CloudCommandIntake
from intelipump_fdc.cloud.delivery import DeliveryMapper, PUBLISHABLE_QUEUE_EVENTS
from intelipump_fdc.cloud.fill_stream import LiveFillStream
from intelipump_fdc.cloud.fill_throttle import (
    FillPublishBook,
    FillThrottleConfig,
    FillThrottleState,
    should_publish_fill,
)
from intelipump_fdc.cloud.heartbeat import HeartbeatService
from intelipump_fdc.cloud.messages import build_envelope
from intelipump_fdc.cloud.mqtt.config import mqtt_config_from_settings
from intelipump_fdc.cloud.mqtt.fake import FakeMqttClient
from intelipump_fdc.cloud.mqtt.models import MqttConnectionState
from intelipump_fdc.cloud.qos import qos_for_event
from intelipump_fdc.cloud.runtime import CloudRuntime
from intelipump_fdc.cloud.schemas import CloudCommandInbound
from intelipump_fdc.cloud.sync_worker import SyncWorker
from intelipump_fdc.cloud.topics import TopicBuilder, TopicError
from intelipump_fdc.core.config import Settings
from intelipump_fdc.persistence.database import (
    configure_sqlite_pragmas,
    create_engine,
    create_session_factory,
    dispose_engine,
)
from intelipump_fdc.persistence.migrations import init_schema
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.persistence.unit_of_work import unit_of_work
from intelipump_fdc.services.pump_state_service import PumpStateService
from intelipump_fdc.services.transaction_models import (
    BeginTransactionRequest,
    CompleteTransactionRequest,
    FillingUpdateRequest,
)
from intelipump_fdc.services.transaction_service import TransactionService
from intelipump_fdc.state_machine.models import PumpContext


@pytest.fixture
async def db_factory(
    tmp_path: Path,
) -> async_sessionmaker[AsyncSession]:
    engine = create_engine(f"sqlite+aiosqlite:///{tmp_path / 'cloud.db'}")
    await configure_sqlite_pragmas(engine)
    await init_schema(engine)
    factory = create_session_factory(engine)
    yield factory
    await dispose_engine(engine)


@pytest.fixture
def topics() -> TopicBuilder:
    return TopicBuilder(environment="LAB")


def test_topic_generation(topics: TopicBuilder) -> None:
    assert (
        topics.heartbeat("InteliPump-Lab-pi-001")
        == "intelipump/lab/devices/InteliPump-Lab-pi-001/heartbeat"
    )
    assert (
        topics.transactions("InteliPump-US-Lab")
        == "intelipump/lab/stations/InteliPump-US-Lab/transactions"
    )
    assert topics.command_result("InteliPump-US-Lab", "corr-1").endswith(
        "/commands/corr-1/result"
    )


@pytest.mark.parametrize(
    "bad",
    ["", "a/b", "a+b", "a#b", "../x", "a\\b"],
)
def test_invalid_topic_identifiers(topics: TopicBuilder, bad: str) -> None:
    with pytest.raises(TopicError):
        topics.heartbeat(bad)


def test_message_envelope_serialization() -> None:
    env = build_envelope(
        event_type="TRANSACTION_COMPLETED",
        environment="LAB",
        device_id="dev-1",
        station_id="InteliPump-US-Lab",
        sequence=1,
        simulated=True,
        deduplication_key="tx-completed:k1",
        payload={"raw_volume": 1000, "raw_amount": 2500},
        pump_id="fp-1",
        transaction_id="tx-1",
    )
    data = env.to_dict()
    assert data["schemaVersion"] == "1.0"
    assert data["messageId"]
    assert data["occurredAt"]
    assert data["publishedAt"]
    assert data["payload"]["raw_volume"] == 1000
    assert isinstance(data["payload"]["raw_volume"], int)


def test_cloud_sync_cli_defaults_are_publish_only() -> None:
    args = build_parser().parse_args([])
    assert args.duration is None
    assert args.commands_enabled is False
    assert args.device_id == "InteliPump-Lab-pi-001"
    assert args.station_id == "InteliPump-US-Lab"


def test_queue_publish_filter_includes_live_fills() -> None:
    assert PUBLISHABLE_QUEUE_EVENTS == {"TRANSACTION_COMPLETED", "FILLING_UPDATED"}


def test_raw_scaled_values_remain_integers() -> None:
    with pytest.raises(ValueError, match="float"):
        build_envelope(
            event_type="TRANSACTION_COMPLETED",
            environment="LAB",
            device_id="d",
            station_id="InteliPump-US-Lab",
            sequence=1,
            simulated=True,
            deduplication_key="k",
            payload={"raw_volume": 1.5},
        )


@pytest.mark.asyncio
async def test_heartbeat_payload_and_replacement(topics: TopicBuilder) -> None:
    mqtt = FakeMqttClient(host="hb-test")
    await mqtt.connect()
    svc = HeartbeatService(
        mqtt=mqtt,
        topics=topics,
        device_id="dev-1",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        simulated=True,
        interval_seconds=60,
        payload_provider=lambda: {
            "controllerMode": "LISTEN_ONLY",
            "uptimeSeconds": 12,
            "pendingSyncCount": 2,
            "configuredPumpCount": 2,
            "healthyPumpCount": 1,
            "degradedPumpCount": 0,
            "disconnectedPumpCount": 1,
            "unresolvedTransactionCount": 0,
            "controllerLoopRunning": False,
            "databaseStatus": "OK",
        },
    )
    assert await svc.publish_once()
    assert any(m.topic.endswith("/heartbeat") for m in mqtt.published)
    body = json.loads(mqtt.published[-1].payload)
    assert body["eventType"] == "HEARTBEAT"
    assert body["payload"]["pendingSyncCount"] == 2
    assert "password" not in json.dumps(body).lower()

    await mqtt.disconnect()
    assert await svc.publish_once() is False
    assert svc._pending_latest is not None
    first_pending = svc._pending_latest
    assert await svc.publish_once() is False
    # Replacement, not unbounded growth — still a single pending dict.
    assert svc._pending_latest is not None
    assert svc._pending_latest["deduplicationKey"] == first_pending["deduplicationKey"]


def test_qos_selection() -> None:
    assert qos_for_event("HEARTBEAT") == 0
    assert qos_for_event("TRANSACTION_COMPLETED") == 1
    assert qos_for_event("FILLING_UPDATED") == 0
    assert qos_for_event("COMMAND_RESULT") == 1


def test_fill_throttling_time_volume_amount_final() -> None:
    cfg = FillThrottleConfig(
        min_interval_seconds=10, min_volume_delta=100, min_amount_delta=100
    )
    now = datetime.now(UTC)
    state = FillThrottleState()
    assert should_publish_fill(
        now=now, raw_volume=0, raw_amount=0, state=state, config=cfg
    )
    state = FillThrottleState(
        last_published_at=now,
        last_raw_volume=0,
        last_raw_amount=0,
        published_first=True,
    )
    assert not should_publish_fill(
        now=now + timedelta(seconds=1),
        raw_volume=10,
        raw_amount=10,
        state=state,
        config=cfg,
    )
    assert should_publish_fill(
        now=now + timedelta(seconds=1),
        raw_volume=200,
        raw_amount=10,
        state=state,
        config=cfg,
    )
    assert should_publish_fill(
        now=now + timedelta(seconds=1),
        raw_volume=10,
        raw_amount=200,
        state=state,
        config=cfg,
    )
    assert should_publish_fill(
        now=now + timedelta(seconds=11),
        raw_volume=10,
        raw_amount=10,
        state=state,
        config=cfg,
    )
    settled = FillThrottleState(
        last_published_at=now,
        last_raw_volume=200,
        last_raw_amount=200,
        published_first=True,
    )
    assert not should_publish_fill(
        now=now + timedelta(seconds=30),
        raw_volume=200,
        raw_amount=200,
        state=settled,
        config=cfg,
    )
    assert should_publish_fill(
        now=now,
        raw_volume=1,
        raw_amount=1,
        state=state,
        config=cfg,
        is_final=True,
    )


@pytest.mark.asyncio
async def test_live_fill_stream_completes_settled_hangup(
    db_factory: async_sessionmaker[AsyncSession], topics: TopicBuilder
) -> None:
    mqtt = FakeMqttClient(host="settle-fill")
    await mqtt.connect()
    started = datetime.now(UTC)
    async with unit_of_work(db_factory) as uow:
        pump = await uow.pumps.upsert(
            station_id="InteliPump-US-Lab",
            logical_pump_id="pump-2",
            dart_address=2,
        )
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id="InteliPump-US-Lab",
                pump_db_id=pump.id,
                transaction_uuid="tx-700",
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=3,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-700",
                raw_volume=595,
                raw_amount=70000,
                event_key="fill:tx-700:595:70000",
            )
        )

    stream = LiveFillStream(
        session_factory=db_factory,
        mqtt=mqtt,
        topics=topics,
        fill_book=FillPublishBook(),
        device_id="InteliPump-Lab-pi-001",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        simulated=False,
        settle_seconds=4.0,
    )
    await stream.publish_active_fills(now=started)
    assert any(
        json.loads(m.payload).get("eventType") == "FILLING_UPDATED" for m in mqtt.published
    )
    async with unit_of_work(db_factory) as uow:
        open_rows = await uow.transactions.list_unresolved(station_id="InteliPump-US-Lab")
        assert len(open_rows) == 1

    await stream.publish_active_fills(now=started + timedelta(seconds=5))
    async with unit_of_work(db_factory) as uow:
        assert await uow.transactions.list_unresolved(station_id="InteliPump-US-Lab") == ()
        pending = await uow.sync_queue.pending_count()
    assert pending >= 1


@pytest.mark.asyncio
async def test_live_fill_stream_does_not_publish_duplicate_after_hangup(
    db_factory: async_sessionmaker[AsyncSession], topics: TopicBuilder
) -> None:
    mqtt = FakeMqttClient(host="settle-dup")
    await mqtt.connect()
    started = datetime.now(UTC)
    async with unit_of_work(db_factory) as uow:
        pump = await uow.pumps.upsert(
            station_id="InteliPump-US-Lab",
            logical_pump_id="pump-2",
            dart_address=2,
        )
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id="InteliPump-US-Lab",
                pump_db_id=pump.id,
                transaction_uuid="tx-live-dup",
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-live-dup",
                raw_volume=170,
                raw_amount=200000,
                event_key="fill:tx-live-dup:170:200000",
            )
        )
        await svc.begin(
            BeginTransactionRequest(
                station_id="InteliPump-US-Lab",
                pump_db_id=pump.id,
                transaction_uuid="tx-hangup-dup",
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="tx-hangup-dup",
                source_completion_key="complete:tx-hangup-dup",
                raw_volume=170,
                raw_amount=200000,
            )
        )
        before = await uow.sync_queue.pending_count()

    stream = LiveFillStream(
        session_factory=db_factory,
        mqtt=mqtt,
        topics=topics,
        fill_book=FillPublishBook(),
        device_id="InteliPump-Lab-pi-001",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        simulated=False,
        settle_seconds=4.0,
    )
    await stream.publish_active_fills(now=started)
    await stream.publish_active_fills(now=started + timedelta(seconds=5))
    async with unit_of_work(db_factory) as uow:
        assert await uow.transactions.list_unresolved(station_id="InteliPump-US-Lab") == ()
        leftover = await uow.transactions.get_by_uuid("tx-live-dup")
        assert leftover is not None
        assert leftover.status == "COMPLETED"
        pending = await uow.sync_queue.pending_count()
    # Live-fill UUID must still publish COMPLETED so the dashboard leaves DISPENSING.
    assert pending > before


@pytest.mark.asyncio
async def test_live_fill_stream_does_not_settle_during_live_fill(
    db_factory: async_sessionmaker[AsyncSession], topics: TopicBuilder
) -> None:
    mqtt = FakeMqttClient(host="live-pause")
    await mqtt.connect()
    started = datetime.now(UTC)
    async with unit_of_work(db_factory) as uow:
        pump = await uow.pumps.upsert(
            station_id="InteliPump-US-Lab",
            logical_pump_id="pump-2",
            dart_address=2,
        )
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id="InteliPump-US-Lab",
                pump_db_id=pump.id,
                transaction_uuid="tx-live-pause",
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=3,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-live-pause",
                raw_volume=200,
                raw_amount=23500,
                event_key="fill:tx-live-pause:200:23500",
            )
        )
        await PumpStateService(uow).persist_context(
            pump_db_id=pump.id,
            context=PumpContext(
                pump_id="pump-2",
                dart_address=2,
                current_state=PumpState.FILLING,
                previous_state=PumpState.AUTHORIZED,
                active_transaction_id="tx-live-pause",
                communication_healthy=True,
                state_version=3,
            ),
            observed_at=started,
        )

    stream = LiveFillStream(
        session_factory=db_factory,
        mqtt=mqtt,
        topics=topics,
        fill_book=FillPublishBook(),
        device_id="InteliPump-Lab-pi-001",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        simulated=False,
        settle_seconds=4.0,
    )
    await stream.publish_active_fills(now=started)
    await stream.publish_active_fills(now=started + timedelta(seconds=5))
    async with unit_of_work(db_factory) as uow:
        open_rows = await uow.transactions.list_unresolved(station_id="InteliPump-US-Lab")
        assert len(open_rows) == 1
        assert open_rows[0].transaction_uuid == "tx-live-pause"


@pytest.mark.asyncio
async def test_live_fill_stream_force_settles_stale_filling_snapshot(
    db_factory: async_sessionmaker[AsyncSession], topics: TopicBuilder
) -> None:
    mqtt = FakeMqttClient(host="force-settle")
    await mqtt.connect()
    started = datetime.now(UTC)
    async with unit_of_work(db_factory) as uow:
        pump = await uow.pumps.upsert(
            station_id="InteliPump-US-Lab",
            logical_pump_id="pump-1",
            dart_address=1,
        )
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id="InteliPump-US-Lab",
                pump_db_id=pump.id,
                transaction_uuid="tx-stale-fill",
                nozzle_id=1,
                raw_price=1175,
                price_decimals=2,
                volume_decimals=2,
                amount_decimals=2,
                simulated=False,
                environment="LAB",
            )
        )
        await svc.update_filling(
            FillingUpdateRequest(
                transaction_uuid="tx-stale-fill",
                raw_volume=170,
                raw_amount=200000,
                event_key="fill:tx-stale-fill:170:200000",
            )
        )
        await PumpStateService(uow).persist_context(
            pump_db_id=pump.id,
            context=PumpContext(
                pump_id="pump-1",
                dart_address=1,
                current_state=PumpState.FILLING,
                previous_state=PumpState.AUTHORIZED,
                active_transaction_id="tx-stale-fill",
                communication_healthy=True,
                state_version=3,
            ),
            observed_at=started,
        )

    stream = LiveFillStream(
        session_factory=db_factory,
        mqtt=mqtt,
        topics=topics,
        fill_book=FillPublishBook(),
        device_id="InteliPump-Lab-pi-001",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        simulated=False,
        settle_seconds=4.0,
        force_settle_seconds=8.0,
    )
    await stream.publish_active_fills(now=started)
    await stream.publish_active_fills(now=started + timedelta(seconds=5))
    async with unit_of_work(db_factory) as uow:
        assert len(await uow.transactions.list_unresolved(station_id="InteliPump-US-Lab")) == 1

    await stream.publish_active_fills(now=started + timedelta(seconds=9))
    async with unit_of_work(db_factory) as uow:
        assert await uow.transactions.list_unresolved(station_id="InteliPump-US-Lab") == ()
        sold = await uow.transactions.get_by_uuid("tx-stale-fill")
        assert sold is not None
        assert sold.status == "COMPLETED"


def test_backoff_bounded() -> None:
    d1 = compute_backoff_seconds(1, base=1.0, maximum=8.0, jitter=0)
    d5 = compute_backoff_seconds(5, base=1.0, maximum=8.0, jitter=0)
    assert d1 == 1.0
    assert d5 == 8.0


@pytest.mark.asyncio
async def test_sync_worker_publishes_filling_updates(
    db_factory: async_sessionmaker[AsyncSession], topics: TopicBuilder
) -> None:
    mqtt = FakeMqttClient(host="live-fill")
    await mqtt.connect()
    mapper = DeliveryMapper(
        topics=topics,
        device_id="InteliPump-Lab-pi-001",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        simulated=False,
    )
    worker = SyncWorker(
        session_factory=db_factory,
        mqtt=mqtt,
        mapper=mapper,
        batch_size=5,
        poll_interval_seconds=0.05,
    )
    async with unit_of_work(db_factory) as uow:
        await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-fill",
            event_type="FILLING_UPDATED",
            payload={"raw_volume": 10, "raw_amount": 20, "pump_id": "pump-1"},
            deduplication_key="fill:tx-fill",
        )
    before = len(mqtt.published)
    await worker._cycle()
    assert len(mqtt.published) == before + 1
    msg = mqtt.published[-1]
    body = json.loads(msg.payload)
    assert msg.topic.endswith("/transactions")
    assert body["eventType"] == "FILLING_UPDATED"
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.pending_count() == 0
        assert await uow.sync_queue.delivered_count() == 1


@pytest.mark.asyncio
async def test_sync_worker_delivery_retry_malformed_stale(
    db_factory: async_sessionmaker[AsyncSession], topics: TopicBuilder
) -> None:
    mqtt = FakeMqttClient(host="sync-test")
    await mqtt.connect()
    mapper = DeliveryMapper(
        topics=topics,
        device_id="dev-1",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        simulated=True,
    )
    worker = SyncWorker(
        session_factory=db_factory,
        mqtt=mqtt,
        mapper=mapper,
        batch_size=5,
        poll_interval_seconds=0.05,
        stale_lock_seconds=0,
        max_attempts=20,
    )
    async with unit_of_work(db_factory) as uow:
        await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-1",
            event_type="TRANSACTION_COMPLETED",
            payload={
                "transaction_uuid": "tx-1",
                "station_id": "InteliPump-US-Lab",
                "pump_id": "fp-1",
                "raw_volume": 100,
                "raw_amount": 200,
                "simulated": True,
            },
            deduplication_key="tx-completed:k1",
        )
        await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="bad",
            event_type="TRANSACTION_COMPLETED",
            payload={"raw_volume": 1.23},  # float money → mapper/envelope fails
            deduplication_key="tx-completed:bad",
        )

    await worker._cycle()
    assert worker.stats.delivered >= 1
    assert worker.stats.skipped_malformed >= 1

    # Retry path
    async with unit_of_work(db_factory) as uow:
        await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-2",
            event_type="TRANSACTION_COMPLETED",
            payload={
                "transaction_uuid": "tx-2",
                "station_id": "InteliPump-US-Lab",
                "pump_id": "fp-1",
                "raw_volume": 1,
                "raw_amount": 2,
                "simulated": True,
            },
            deduplication_key="tx-completed:k2",
        )
    mqtt.fail_next_publish = True
    await worker._cycle()
    assert worker.stats.failed >= 1

    # Stale lock recovery: claim then release immediately
    async with unit_of_work(db_factory) as uow:
        await uow.sync_queue.enqueue(
            entity_type="transaction",
            entity_id="tx-stale",
            event_type="TRANSACTION_COMPLETED",
            payload={
                "transaction_uuid": "tx-stale",
                "station_id": "InteliPump-US-Lab",
                "pump_id": "fp-1",
                "raw_volume": 1,
                "raw_amount": 2,
                "simulated": True,
            },
            deduplication_key="tx-completed:stale",
        )
        claimed = await uow.sync_queue.claim_batch(limit=1)
        assert claimed
        released = await uow.sync_queue.release_stale_locks(older_than_seconds=0)
        assert released >= 1


@pytest.mark.asyncio
async def test_transaction_completed_once_and_offline_queue(
    db_factory: async_sessionmaker[AsyncSession], topics: TopicBuilder
) -> None:
    async with unit_of_work(db_factory) as uow:
        pump = await uow.pumps.upsert(
            station_id="InteliPump-US-Lab",
            logical_pump_id="fp-1",
            dart_address=1,
        )
        svc = TransactionService(uow)
        await svc.begin(
            BeginTransactionRequest(
                station_id="InteliPump-US-Lab",
                pump_db_id=pump.id,
                transaction_uuid="biz-tx-1",
                nozzle_id=1,
                raw_price=1000,
                price_decimals=3,
                volume_decimals=3,
                amount_decimals=2,
                simulated=True,
                environment="LAB",
            )
        )
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="biz-tx-1",
                source_completion_key="sale-key-1",
                raw_volume=5000,
                raw_amount=7500,
            )
        )
        # Duplicate complete must not create second queue row
        await svc.complete(
            CompleteTransactionRequest(
                transaction_uuid="biz-tx-1",
                source_completion_key="sale-key-1",
                raw_volume=5000,
                raw_amount=7500,
            )
        )
        pending = await uow.sync_queue.pending_count()

    assert pending >= 1  # started + completed at least
    # Stable dedupe key
    env1 = build_envelope(
        event_type="TRANSACTION_COMPLETED",
        environment="LAB",
        device_id="d",
        station_id="InteliPump-US-Lab",
        sequence=1,
        simulated=True,
        deduplication_key="tx-completed:sale-key-1",
        payload={"raw_volume": 5000, "raw_amount": 7500},
    )
    env2 = build_envelope(
        event_type="TRANSACTION_COMPLETED",
        environment="LAB",
        device_id="d",
        station_id="InteliPump-US-Lab",
        sequence=2,
        simulated=True,
        deduplication_key="tx-completed:sale-key-1",
        payload={"raw_volume": 5000, "raw_amount": 7500},
    )
    assert env1.deduplication_key == env2.deduplication_key

    mqtt = FakeMqttClient(host="off-test")
    mapper = DeliveryMapper(
        topics=topics,
        device_id="dev-1",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        simulated=True,
    )
    worker = SyncWorker(session_factory=db_factory, mqtt=mqtt, mapper=mapper)
    # Offline: queue preserved
    await worker._cycle()
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.pending_count() >= 1

    await mqtt.connect()
    await worker._cycle()
    async with unit_of_work(db_factory) as uow:
        assert await uow.sync_queue.delivered_count() >= 1


@pytest.mark.asyncio
async def test_reconnect_resubscribe_online_lwt(topics: TopicBuilder) -> None:
    mqtt = FakeMqttClient(host="reconn", client_id="c1")
    offline = {"status": "OFFLINE", "environment": "LAB", "simulated": True}
    mqtt.set_will(
        topics.device_status("dev-1"),
        json.dumps(offline),
        qos=1,
        retain=True,
    )
    await mqtt.connect()
    assert mqtt.is_connected
    await mqtt.subscribe(topics.commands("InteliPump-US-Lab"))
    assert topics.commands("InteliPump-US-Lab") in mqtt._subs
    await mqtt.disconnect()
    assert mqtt.metadata.state is MqttConnectionState.DISCONNECTED
    # LWT fanout requires a peer subscriber; will is stored
    assert mqtt._will is not None
    await mqtt.connect()
    mqtt.metadata.reconnect_count += 1
    assert mqtt.is_connected


@pytest.mark.asyncio
async def test_command_validation_and_production_not_executed(
    db_factory: async_sessionmaker[AsyncSession], topics: TopicBuilder
) -> None:
    mqtt = FakeMqttClient(host="cmd-test")
    await mqtt.connect()
    async with unit_of_work(db_factory) as uow:
        await uow.pumps.upsert(
            station_id="InteliPump-US-Lab",
            logical_pump_id="fp-1",
            dart_address=1,
        )
    intake = CloudCommandIntake(
        session_factory=db_factory,
        mqtt=mqtt,
        topics=topics,
        station_id="InteliPump-US-Lab",
        device_id="dev-1",
        environment="LAB",
        simulated=True,
        allow_lab_simulator_commands=False,
        controller_loop=None,
    )
    await intake.start()
    now = datetime.now(UTC)

    expired = CloudCommandInbound(
        commandId="c1",
        correlationId="corr-exp",
        stationId="InteliPump-US-Lab",
        pumpId="fp-1",
        commandType="READ_STATUS",
        createdAt=now - timedelta(hours=1),
        expiresAt=now - timedelta(minutes=1),
        simulatorOnly=True,
        environment="LAB",
    )
    r = await intake.handle_command(expired)
    assert r["executionStatus"] == "REJECTED"
    assert "expired" in r["blockingReasons"]

    wrong_station = CloudCommandInbound(
        commandId="c2",
        correlationId="corr-ws",
        stationId="Other-Station-Lab",
        pumpId="fp-1",
        commandType="READ_STATUS",
        createdAt=now,
        expiresAt=now + timedelta(minutes=5),
        simulatorOnly=True,
        environment="LAB",
    )
    r = await intake.handle_command(wrong_station)
    assert "wrong_station" in r["blockingReasons"]

    wrong_env = CloudCommandInbound(
        commandId="c3",
        correlationId="corr-we",
        stationId="InteliPump-US-Lab",
        pumpId="fp-1",
        commandType="READ_STATUS",
        createdAt=now,
        expiresAt=now + timedelta(minutes=5),
        simulatorOnly=True,
        environment="PROD",
    )
    r = await intake.handle_command(wrong_env)
    assert "wrong_environment" in r["blockingReasons"]

    auth = CloudCommandInbound(
        commandId="c4",
        correlationId="corr-auth",
        stationId="InteliPump-US-Lab",
        pumpId="fp-1",
        commandType="AUTHORIZE",
        createdAt=now,
        expiresAt=now + timedelta(minutes=5),
        simulatorOnly=False,
        environment="LAB",
    )
    r = await intake.handle_command(auth)
    assert r["evaluated"] is True
    assert r["executed"] is False

    # Duplicate
    r2 = await intake.handle_command(auth)
    assert "duplicate" in " ".join(r2["blockingReasons"])

    async with unit_of_work(db_factory) as uow:
        cmd = await uow.commands.get("corr-auth")
        assert cmd is not None
        audits, _total = await uow.audit.list_filtered(
            station_id="InteliPump-US-Lab", limit=20
        )
        assert any("CLOUD_COMMAND" in a.action for a in audits)

    assert any("/result" in m.topic for m in mqtt.published)


@pytest.mark.asyncio
async def test_lab_simulator_command_requires_virtual_transport(
    db_factory: async_sessionmaker[AsyncSession], topics: TopicBuilder
) -> None:
    mqtt = FakeMqttClient(host="lab-cmd")
    await mqtt.connect()
    async with unit_of_work(db_factory) as uow:
        await uow.pumps.upsert(
            station_id="InteliPump-US-Lab",
            logical_pump_id="fp-1",
            dart_address=1,
        )
    intake = CloudCommandIntake(
        session_factory=db_factory,
        mqtt=mqtt,
        topics=topics,
        station_id="InteliPump-US-Lab",
        device_id="dev-1",
        environment="LAB",
        simulated=True,
        allow_lab_simulator_commands=True,
        controller_loop=None,  # no virtual transport → no execute
    )
    now = datetime.now(UTC)
    r = await intake.handle_command(
        CloudCommandInbound(
            commandId="c5",
            correlationId="corr-read",
            stationId="InteliPump-US-Lab",
            pumpId="fp-1",
            commandType="READ_STATUS",
            createdAt=now,
            expiresAt=now + timedelta(minutes=5),
            simulatorOnly=True,
            environment="LAB",
        )
    )
    assert r["executed"] is False


@pytest.mark.asyncio
async def test_cloud_runtime_lifecycle_and_no_secrets(
    db_factory: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = Settings()
    settings.mqtt.enabled = True
    settings.mqtt.host = "runtime-fake"
    settings.mqtt.password = "super-secret-password"
    settings.mqtt.command_subscription_enabled = False
    mqtt = FakeMqttClient(host="runtime-fake", client_id="rt")
    cloud = CloudRuntime.create(
        settings=settings,
        session_factory=db_factory,
        mqtt=mqtt,
        payload_provider=lambda: {"pendingSyncCount": 0, "uptimeSeconds": 1},
    )
    await cloud.start()
    assert cloud.online_published
    assert cloud.command_intake is None
    assert cloud.heartbeat is not None
    assert mqtt._will is not None
    assert mqtt._will.topic.endswith("/status")
    assert mqtt._will.retain is True
    will = json.loads(mqtt._will.payload)
    assert will["eventType"] == "DEVICE_OFFLINE"
    assert will["schemaVersion"] == "1.0"
    assert will["payload"]["status"] == "OFFLINE"
    await cloud.heartbeat.publish_once()
    health = cloud.health_dict()
    blob = json.dumps(health)
    assert "super-secret-password" not in blob
    assert health["mqttEnabled"] is True
    cfg = mqtt_config_from_settings(settings.mqtt, device_id="dev")
    assert "super-secret" not in repr(cfg)
    await cloud.stop()
    assert cloud.started is False


@pytest.mark.asyncio
async def test_heartbeat_stops_cleanly(topics: TopicBuilder) -> None:
    mqtt = FakeMqttClient(host="hb-stop")
    await mqtt.connect()
    svc = HeartbeatService(
        mqtt=mqtt,
        topics=topics,
        device_id="dev-1",
        station_id="InteliPump-US-Lab",
        environment="LAB",
        simulated=True,
        interval_seconds=0.05,
    )
    svc.start()
    await svc.stop()
    assert svc._task is None


def test_fill_book_first_and_throttle() -> None:
    book = FillPublishBook(
        config=FillThrottleConfig(
            min_interval_seconds=60, min_volume_delta=1000, min_amount_delta=1000
        )
    )
    assert book.decide("tx", raw_volume=0, raw_amount=0)
    assert not book.decide("tx", raw_volume=1, raw_amount=1)
    assert book.decide("tx", raw_volume=0, raw_amount=0, is_final=True)
