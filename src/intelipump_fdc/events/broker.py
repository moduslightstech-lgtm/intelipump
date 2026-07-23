"""In-process live event broker."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from intelipump_fdc.events.models import (
    CRITICAL_LIVE_EVENTS,
    EventFilter,
    LiveEvent,
    LiveEventType,
)
from intelipump_fdc.events.subscription import SubscriberLimitError, Subscription


class EventBroker:
    """Fan-out broker with bounded per-subscriber queues."""

    def __init__(
        self,
        *,
        max_sse_subscribers: int = 32,
        max_ws_subscribers: int = 32,
        queue_size: int = 128,
    ) -> None:
        self._max_sse = max_sse_subscribers
        self._max_ws = max_ws_subscribers
        self._queue_size = queue_size
        self._subs: dict[str, Subscription] = {}
        self._seq = 0
        self._dropped_noncritical = 0
        self._lock = asyncio.Lock()

    @property
    def active_sse(self) -> int:
        return sum(1 for s in self._subs.values() if s.kind == "sse" and not s.closed)

    @property
    def active_ws(self) -> int:
        return sum(1 for s in self._subs.values() if s.kind == "ws" and not s.closed)

    @property
    def dropped_noncritical(self) -> int:
        return self._dropped_noncritical

    def next_sequence(self) -> int:
        self._seq += 1
        return self._seq

    def publish(self, event: LiveEvent) -> None:
        critical = event.event_type in CRITICAL_LIVE_EVENTS
        for sub in list(self._subs.values()):
            if sub.closed or not sub.event_filter.matches(event):
                continue
            try:
                sub.queue.put_nowait(event)
            except asyncio.QueueFull:
                if critical:
                    with contextlib.suppress(asyncio.QueueEmpty, asyncio.QueueFull):
                        _ = sub.queue.get_nowait()
                        sub.queue.put_nowait(event)
                else:
                    self._dropped_noncritical += 1
                    sub.dropped += 1

    def make_event(
        self,
        event_type: LiveEventType,
        *,
        station_id: str,
        environment: str,
        simulated: bool,
        pump_id: str | None = None,
        transaction_id: str | None = None,
        correlation_id: str | None = None,
        severity: str | None = None,
        payload: dict[str, Any] | None = None,
        state_version: int | None = None,
    ) -> LiveEvent:
        return LiveEvent(
            event_id=str(uuid4()),
            event_type=event_type,
            timestamp=datetime.now(UTC),
            station_id=station_id,
            environment=environment,
            simulated=simulated,
            sequence=self.next_sequence(),
            pump_id=pump_id,
            transaction_id=transaction_id,
            correlation_id=correlation_id,
            severity=severity,
            payload=payload or {},
            state_version=state_version,
        )

    def publish_typed(
        self,
        event_type: LiveEventType,
        *,
        station_id: str,
        environment: str,
        simulated: bool,
        pump_id: str | None = None,
        transaction_id: str | None = None,
        correlation_id: str | None = None,
        severity: str | None = None,
        payload: dict[str, Any] | None = None,
        state_version: int | None = None,
    ) -> LiveEvent:
        event = self.make_event(
            event_type,
            station_id=station_id,
            environment=environment,
            simulated=simulated,
            pump_id=pump_id,
            transaction_id=transaction_id,
            correlation_id=correlation_id,
            severity=severity,
            payload=payload,
            state_version=state_version,
        )
        self.publish(event)
        return event

    async def subscribe(
        self,
        *,
        kind: str,
        event_filter: EventFilter | None = None,
    ) -> Subscription:
        async with self._lock:
            if kind == "sse" and self.active_sse >= self._max_sse:
                raise SubscriberLimitError("SSE subscriber limit reached")
            if kind == "ws" and self.active_ws >= self._max_ws:
                raise SubscriberLimitError("WebSocket subscriber limit reached")
            sub = Subscription(
                subscription_id=str(uuid4()),
                queue=asyncio.Queue(maxsize=self._queue_size),
                event_filter=event_filter or EventFilter(),
                kind=kind,
            )
            self._subs[sub.subscription_id] = sub
            return sub

    async def unsubscribe(self, subscription_id: str) -> None:
        async with self._lock:
            sub = self._subs.pop(subscription_id, None)
            if sub is not None:
                sub.close()

    async def close_all(self) -> None:
        async with self._lock:
            for sub in list(self._subs.values()):
                sub.close()
            self._subs.clear()

    async def stream(self, subscription: Subscription) -> AsyncIterator[LiveEvent]:
        try:
            while not subscription.closed:
                item = await subscription.queue.get()
                if item is None:
                    break
                yield item
        finally:
            await self.unsubscribe(subscription.subscription_id)
