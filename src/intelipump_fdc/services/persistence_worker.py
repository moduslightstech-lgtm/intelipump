"""Bounded async persistence worker (non-blocking for protocol loop)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

from intelipump_fdc.persistence.errors import PersistenceQueueFullError


class PersistPriority(IntEnum):
    CRITICAL = 0  # transaction complete, audit, alarms
    NORMAL = 1  # filling updates, routine state


@dataclass(order=True)
class PersistJob:
    priority: int
    seq: int
    kind: str = field(compare=False)
    payload: dict[str, Any] = field(compare=False, default_factory=dict)
    handler: Callable[[dict[str, Any]], Awaitable[None]] | None = field(
        compare=False, default=None
    )


class PersistenceWorker:
    """Background worker with priority queue and backpressure."""

    def __init__(self, *, maxsize: int = 256) -> None:
        self._queue: asyncio.PriorityQueue[PersistJob] = asyncio.PriorityQueue(
            maxsize=maxsize
        )
        self._maxsize = maxsize
        self._seq = 0
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._dropped_normal = 0
        self._processed = 0
        self._errors = 0

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    @property
    def dropped_normal(self) -> int:
        return self._dropped_normal

    @property
    def processed(self) -> int:
        return self._processed

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="persistence-worker")

    async def stop(self, *, flush: bool = True, timeout_s: float = 5.0) -> None:
        if flush:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._queue.join(), timeout=timeout_s)
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def submit(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        handler: Callable[[dict[str, Any]], Awaitable[None]],
        priority: PersistPriority = PersistPriority.NORMAL,
    ) -> None:
        self._seq += 1
        job = PersistJob(
            priority=int(priority),
            seq=self._seq,
            kind=kind,
            payload=payload,
            handler=handler,
        )
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull as exc:
            if priority is PersistPriority.CRITICAL:
                raise PersistenceQueueFullError(
                    "critical persistence queue full; refusing to drop"
                ) from exc
            self._dropped_normal += 1

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                job = await asyncio.wait_for(self._queue.get(), timeout=0.1)
            except TimeoutError:
                continue
            try:
                if job.handler is not None:
                    await job.handler(job.payload)
                self._processed += 1
            except Exception:
                self._errors += 1
            finally:
                self._queue.task_done()
