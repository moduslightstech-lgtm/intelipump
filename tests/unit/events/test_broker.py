"""Event broker unit tests."""

from __future__ import annotations

import asyncio

import pytest

from intelipump_fdc.events.broker import EventBroker
from intelipump_fdc.events.models import EventFilter, LiveEventType
from intelipump_fdc.events.subscription import SubscriberLimitError


@pytest.mark.asyncio
async def test_broker_queue_limits_and_critical_delivery() -> None:
    broker = EventBroker(max_sse_subscribers=2, max_ws_subscribers=2, queue_size=2)
    sub = await broker.subscribe(kind="sse")
    # Fill queue
    for i in range(2):
        broker.publish_typed(
            LiveEventType.FILLING_UPDATED,
            station_id="s",
            environment="LAB",
            simulated=True,
            payload={"i": i},
        )
    # Non-critical drop
    broker.publish_typed(
        LiveEventType.FILLING_UPDATED,
        station_id="s",
        environment="LAB",
        simulated=True,
        payload={"i": 99},
    )
    assert broker.dropped_noncritical >= 1
    # Critical should still be delivered (drop oldest)
    broker.publish_typed(
        LiveEventType.TRANSACTION_COMPLETED,
        station_id="s",
        environment="LAB",
        simulated=True,
        transaction_id="tx",
    )
    items = []
    while not sub.queue.empty():
        items.append(sub.queue.get_nowait())
    assert any(
        i and i.event_type is LiveEventType.TRANSACTION_COMPLETED for i in items
    )
    await broker.unsubscribe(sub.subscription_id)


@pytest.mark.asyncio
async def test_subscriber_limit() -> None:
    broker = EventBroker(max_sse_subscribers=1, queue_size=8)
    await broker.subscribe(kind="sse")
    with pytest.raises(SubscriberLimitError):
        await broker.subscribe(kind="sse")


@pytest.mark.asyncio
async def test_filter_and_unsubscribe() -> None:
    broker = EventBroker(queue_size=8)
    sub = await broker.subscribe(
        kind="ws",
        event_filter=EventFilter(pump_id="pump-1"),
    )
    broker.publish_typed(
        LiveEventType.PUMP_STATE_CHANGED,
        station_id="s",
        environment="LAB",
        simulated=True,
        pump_id="pump-2",
    )
    broker.publish_typed(
        LiveEventType.PUMP_STATE_CHANGED,
        station_id="s",
        environment="LAB",
        simulated=True,
        pump_id="pump-1",
    )
    await asyncio.sleep(0)
    assert sub.queue.qsize() == 1
    await broker.unsubscribe(sub.subscription_id)
    assert sub.closed
