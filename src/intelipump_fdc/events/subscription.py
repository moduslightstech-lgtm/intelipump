"""Event subscription types."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

from intelipump_fdc.events.models import EventFilter, LiveEvent


@dataclass
class Subscription:
    subscription_id: str
    queue: asyncio.Queue[LiveEvent | None]
    event_filter: EventFilter
    kind: str  # sse | ws
    dropped: int = 0
    closed: bool = False

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        with contextlib.suppress(asyncio.QueueFull):
            self.queue.put_nowait(None)


class SubscriberLimitError(Exception):
    pass
