"""Bounded async persistence worker (non-blocking for protocol loop).

CRITICAL jobs (sale completion, audit) are never silently discarded:
bounded in-memory retries, write-ahead durable spill, and an explicit
degraded health signal when recovery backlog or failures remain.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

from intelipump_fdc.persistence.errors import PersistenceQueueFullError
from intelipump_fdc.services.persist_recovery import (
    PersistRecoveryStore,
    sale_persist_identity,
)

logger = logging.getLogger(__name__)

# Bounded retries for CRITICAL handler failures (temporary SQLite lock, etc.).
DEFAULT_CRITICAL_MAX_ATTEMPTS = 5
_CRITICAL_RETRY_DELAY_S = 0.05


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
    attempt: int = field(compare=False, default=0)
    identity_key: str | None = field(compare=False, default=None)


class PersistenceWorker:
    """Background worker with priority queue, CRITICAL retries, and durable spill."""

    def __init__(
        self,
        *,
        maxsize: int = 256,
        critical_max_attempts: int = DEFAULT_CRITICAL_MAX_ATTEMPTS,
        recovery_store: PersistRecoveryStore | None = None,
    ) -> None:
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
        self._critical_retries = 0
        self._critical_durable_spills = 0
        self._critical_max_attempts = max(1, int(critical_max_attempts))
        self._recovery_store = recovery_store
        self._handlers: dict[str, Callable[[dict[str, Any]], Awaitable[None]]] = {}
        self._degraded = False
        self._degraded_reason: str | None = None

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    @property
    def dropped_normal(self) -> int:
        return self._dropped_normal

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def errors(self) -> int:
        return self._errors

    @property
    def critical_retries(self) -> int:
        return self._critical_retries

    @property
    def critical_durable_spills(self) -> int:
        return self._critical_durable_spills

    @property
    def is_degraded(self) -> bool:
        if self._degraded:
            return True
        if self._recovery_store is not None and self._recovery_store.pending_count() > 0:
            return True
        return False

    @property
    def degraded_reason(self) -> str | None:
        if self._degraded_reason:
            return self._degraded_reason
        if self._recovery_store is not None:
            n = self._recovery_store.pending_count()
            if n > 0:
                return f"persist_recovery_pending={n}"
        return None

    def register_handler(
        self,
        kind: str,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        """Bind kind → handler for durable recovery replay after restart."""
        self._handlers[kind] = handler

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
        attempt: int = 0,
        identity_key: str | None = None,
    ) -> None:
        self._handlers.setdefault(kind, handler)
        self._seq += 1
        key = identity_key
        if key is None and priority is PersistPriority.CRITICAL:
            key = sale_persist_identity(kind, payload)
        job = PersistJob(
            priority=int(priority),
            seq=self._seq,
            kind=kind,
            payload=payload,
            handler=handler,
            attempt=attempt,
            identity_key=key,
        )
        if (
            priority is PersistPriority.CRITICAL
            and key is not None
            and self._recovery_store is not None
            and attempt == 0
        ):
            # Write-ahead: survive crash before/during first handler attempt.
            self._recovery_store.upsert(
                identity_key=key,
                kind=kind,
                payload=payload,
                attempt=attempt,
            )
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull as exc:
            if priority is PersistPriority.CRITICAL:
                if key is not None and self._recovery_store is not None:
                    self._recovery_store.upsert(
                        identity_key=key,
                        kind=kind,
                        payload=payload,
                        attempt=attempt,
                        last_error="queue_full",
                    )
                    self._critical_durable_spills += 1
                    self._mark_degraded("critical_queue_full_spilled_to_durable")
                    logger.error(
                        "critical persistence queue full; spilled to durable store "
                        "identity=%s kind=%s",
                        key,
                        kind,
                    )
                    return
                raise PersistenceQueueFullError(
                    "critical persistence queue full; refusing to drop"
                ) from exc
            self._dropped_normal += 1

    def recover_pending(self) -> int:
        """Re-queue PENDING durable jobs after process restart. Returns count."""
        if self._recovery_store is None:
            return 0
        pending = self._recovery_store.list_pending()
        restored = 0
        for job in pending:
            handler = self._handlers.get(job.kind)
            if handler is None:
                logger.error(
                    "persist recovery missing handler kind=%s identity=%s",
                    job.kind,
                    job.identity_key,
                )
                self._mark_degraded(f"missing_handler:{job.kind}")
                continue
            try:
                self.submit(
                    kind=job.kind,
                    payload=job.payload,
                    handler=handler,
                    priority=PersistPriority.CRITICAL,
                    attempt=0,  # fresh retry budget after process restart
                    identity_key=job.identity_key,
                )
                restored += 1
            except PersistenceQueueFullError:
                self._mark_degraded("recovery_queue_full")
                break
        if restored:
            self._mark_degraded(f"recovered_pending={restored}")
            logger.warning(
                "persist recovery re-queued %s durable CRITICAL job(s)", restored
            )
        return restored

    def _mark_degraded(self, reason: str) -> None:
        self._degraded = True
        self._degraded_reason = reason

    def clear_degraded_if_idle(self) -> None:
        """Clear degraded once durable backlog is empty and queue is drained."""
        if self._recovery_store is not None and self._recovery_store.pending_count() > 0:
            return
        if self.depth > 0:
            return
        self._degraded = False
        self._degraded_reason = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                job = await asyncio.wait_for(self._queue.get(), timeout=0.1)
            except TimeoutError:
                self.clear_degraded_if_idle()
                continue
            try:
                await self._execute(job)
            finally:
                self._queue.task_done()

    async def _execute(self, job: PersistJob) -> None:
        handler = job.handler or self._handlers.get(job.kind)
        if handler is None:
            self._errors += 1
            logger.error("persistence job has no handler kind=%s", job.kind)
            if job.priority == int(PersistPriority.CRITICAL):
                self._spill_failed(job, "missing_handler")
            return
        try:
            await handler(job.payload)
            self._processed += 1
            if job.identity_key and self._recovery_store is not None:
                self._recovery_store.mark_done(job.identity_key)
            self.clear_degraded_if_idle()
        except Exception as exc:
            self._errors += 1
            err = f"{type(exc).__name__}:{exc}"
            logger.exception(
                "persistence handler failed kind=%s attempt=%s identity=%s",
                job.kind,
                job.attempt,
                job.identity_key,
            )
            if job.priority != int(PersistPriority.CRITICAL):
                return
            # CRITICAL: never silently remove — retry or durable spill.
            next_attempt = job.attempt + 1
            if next_attempt < self._critical_max_attempts:
                self._critical_retries += 1
                self._mark_degraded(f"critical_retry kind={job.kind}")
                if job.identity_key and self._recovery_store is not None:
                    self._recovery_store.upsert(
                        identity_key=job.identity_key,
                        kind=job.kind,
                        payload=job.payload,
                        attempt=next_attempt,
                        last_error=err,
                    )
                await asyncio.sleep(_CRITICAL_RETRY_DELAY_S * next_attempt)
                try:
                    self.submit(
                        kind=job.kind,
                        payload=job.payload,
                        handler=handler,
                        priority=PersistPriority.CRITICAL,
                        attempt=next_attempt,
                        identity_key=job.identity_key,
                    )
                except PersistenceQueueFullError:
                    self._spill_failed(job, err)
                return
            self._spill_failed(job, err)

    def _spill_failed(self, job: PersistJob, error: str) -> None:
        """Keep failed CRITICAL work durable; never discard without a trace."""
        self._critical_durable_spills += 1
        self._mark_degraded(f"critical_persist_failed kind={job.kind}")
        key = job.identity_key or f"{job.kind}:{job.seq}"
        if self._recovery_store is not None:
            self._recovery_store.upsert(
                identity_key=key,
                kind=job.kind,
                payload=job.payload,
                attempt=job.attempt,
                last_error=error,
            )
            logger.error(
                "CRITICAL persistence spilled to durable store identity=%s "
                "kind=%s attempts=%s error=%s",
                key,
                job.kind,
                job.attempt,
                error,
            )
            return
        logger.error(
            "CRITICAL persistence failed with no recovery store; "
            "job retained only in degraded signal identity=%s kind=%s error=%s",
            key,
            job.kind,
            error,
        )
