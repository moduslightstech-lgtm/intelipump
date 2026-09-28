"""Bounded async persistence worker (non-blocking for protocol loop).

CRITICAL jobs (sale completion, audit) are never silently discarded:
bounded in-memory retries, write-ahead durable spill, in-process requeue
when the memory queue was full, and an explicit degraded health signal.

Sale write-ahead (off serial loop)
---------------------------------
``submit`` returns without fsync. After a CRITICAL job is accepted into the
memory queue, exactly one tracked durable write task is scheduled via
``asyncio.to_thread``. The worker awaits that same task before running the
sale handler, so durability lands before side effects.

Crash window (explicit)
-----------------------
From successful ``put_nowait`` until the tracked write task completes, a hard
process crash can lose the sale from the recovery store (it exists only in the
in-memory queue). The window closes when the write finishes — before the
handler runs. Queue-full CRITICAL spills fsync synchronously on the submit
path and have no such window. Late writes after ``mark_done`` are suppressed
via a per-identity epoch so a stalled thread cannot recreate a done record.
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
    PersistRecoveryError,
    PersistRecoveryIOError,
    PersistRecoveryStore,
    sale_persist_identity,
)

logger = logging.getLogger(__name__)

DEFAULT_CRITICAL_MAX_ATTEMPTS = 5
_CRITICAL_RETRY_DELAY_S = 0.05
_REQUEUE_IDLE_INTERVAL_S = 0.1


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
        self._inflight_identities: set[str] = set()
        self._requeue_task: asyncio.Task[None] | None = None
        # One tracked durable write task per sale identity (submit → await in worker).
        self._pending_writes: dict[str, asyncio.Task[None]] = {}
        # Bumped on mark_done / invalidate so a late to_thread upsert cannot recreate.
        self._write_epoch: dict[str, int] = {}

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
    def pending_durable_writes(self) -> int:
        return sum(1 for t in self._pending_writes.values() if not t.done())

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
        self._handlers[kind] = handler

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="persistence-worker")
            # Requeue spilled CRITICAL jobs even while a handler is still running
            # (must not wait for an empty queue or process restart).
            self._requeue_task = asyncio.create_task(
                self._requeue_while_busy(), name="persistence-requeue"
            )

    async def stop(self, *, flush: bool = True, timeout_s: float = 5.0) -> None:
        if flush:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._queue.join(), timeout=timeout_s)
            # Handlers await their tracked write, but drain any stragglers
            # (e.g. write scheduled then job not yet joined under timing races).
            await self._flush_pending_writes(timeout_s=timeout_s)
        self._stop.set()
        for task in (self._requeue_task, self._task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._requeue_task = None
        self._task = None
        if not flush:
            for write_task in list(self._pending_writes.values()):
                write_task.cancel()
            self._pending_writes.clear()
        else:
            # Cancel anything still running after flush timeout.
            for write_task in list(self._pending_writes.values()):
                if not write_task.done():
                    write_task.cancel()
            self._pending_writes.clear()

    async def _flush_pending_writes(self, *, timeout_s: float) -> None:
        pending = [t for t in self._pending_writes.values() if not t.done()]
        if not pending:
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=timeout_s,
            )

    async def _requeue_while_busy(self) -> None:
        """Pull durable spills into the memory queue while work is in flight."""
        while not self._stop.is_set():
            try:
                await asyncio.sleep(_REQUEUE_IDLE_INTERVAL_S)
                if self._recovery_store is None:
                    continue
                if self.depth >= self._maxsize:
                    continue
                with contextlib.suppress(PersistRecoveryError):
                    self.recover_pending()
            except asyncio.CancelledError:
                raise

    def submit(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        handler: Callable[[dict[str, Any]], Awaitable[None]],
        priority: PersistPriority = PersistPriority.NORMAL,
        attempt: int = 0,
        identity_key: str | None = None,
        write_ahead: bool | None = None,
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
        do_write_ahead = (
            write_ahead
            if write_ahead is not None
            else (priority is PersistPriority.CRITICAL and key is not None and attempt == 0)
        )
        try:
            self._queue.put_nowait(job)
            if key is not None:
                self._inflight_identities.add(key)
        except asyncio.QueueFull as exc:
            if priority is PersistPriority.CRITICAL:
                if key is not None and self._recovery_store is not None:
                    # Must land on disk before return — rare path; sync fsync OK.
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
                        "identity=%s kind=%s (will requeue in-process)",
                        key,
                        kind,
                    )
                    return
                raise PersistenceQueueFullError(
                    "critical persistence queue full; refusing to drop"
                ) from exc
            self._dropped_normal += 1
            return
        # Schedule only after accept — one tracked write; never fsync on serial loop.
        if do_write_ahead and key is not None:
            self._schedule_durable_upsert(
                identity_key=key,
                kind=kind,
                payload=payload,
                attempt=attempt,
            )

    def _schedule_durable_upsert(
        self,
        *,
        identity_key: str,
        kind: str,
        payload: dict[str, Any],
        attempt: int,
        last_error: str | None = None,
    ) -> asyncio.Task[None] | None:
        """Start or reuse the single tracked durable write for this sale identity.

        Disk I/O runs in a worker thread. Returns the task when a loop is running;
        performs a synchronous upsert when called outside an event loop.
        """
        store = self._recovery_store
        if store is None:
            return None

        existing = self._pending_writes.get(identity_key)
        if existing is not None and not existing.done():
            return existing

        epoch = self._write_epoch.get(identity_key, 0)

        def _write() -> None:
            if self._write_epoch.get(identity_key, 0) != epoch:
                return
            store.upsert(
                identity_key=identity_key,
                kind=kind,
                payload=payload,
                attempt=attempt,
                last_error=last_error,
            )
            # Late-write guard: mark_done may have raced after upsert started.
            if self._write_epoch.get(identity_key, 0) != epoch:
                store.mark_done(identity_key)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            _write()
            return None

        async def _run() -> None:
            try:
                await asyncio.to_thread(_write)
            except Exception:
                self._mark_degraded("persist_recovery_write_failed")
                logger.exception(
                    "durable write-ahead failed identity=%s kind=%s",
                    identity_key,
                    kind,
                )
                raise

        task = loop.create_task(_run(), name=f"persist-recovery-upsert:{identity_key}")
        self._pending_writes[identity_key] = task

        def _cleanup(done: asyncio.Task[None]) -> None:
            if self._pending_writes.get(identity_key) is done:
                self._pending_writes.pop(identity_key, None)

        task.add_done_callback(_cleanup)
        return task

    def _invalidate_durable_write(self, identity_key: str) -> None:
        """Bump epoch so any in-flight/stale upsert cannot recreate after done."""
        self._write_epoch[identity_key] = self._write_epoch.get(identity_key, 0) + 1

    async def _await_durable_write(self, job: PersistJob) -> None:
        """Block the worker (not the serial loop) until write-ahead has settled."""
        if (
            job.priority != int(PersistPriority.CRITICAL)
            or job.identity_key is None
            or self._recovery_store is None
        ):
            return
        task = self._pending_writes.get(job.identity_key)
        if task is None:
            return
        try:
            await task
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise PersistRecoveryIOError(
                f"write-ahead failed for {job.identity_key}: {exc}"
            ) from exc

    def recover_pending(self) -> int:
        """Re-queue PENDING durable jobs (restart or in-process queue-full)."""
        if self._recovery_store is None:
            return 0
        try:
            pending = self._recovery_store.list_pending()
        except PersistRecoveryError:
            self._mark_degraded("persist_recovery_store_error")
            logger.exception("persist recovery store unreadable")
            raise
        restored = 0
        for job in pending:
            if self.depth >= self._maxsize:
                break
            if job.identity_key in self._inflight_identities:
                continue
            handler = self._handlers.get(job.kind)
            if handler is None:
                logger.error(
                    "persist recovery missing handler kind=%s identity=%s",
                    job.kind,
                    job.identity_key,
                )
                self._mark_degraded(f"missing_handler:{job.kind}")
                continue
            # Already durable — do not rewrite; attempt>0 skips default write-ahead.
            self.submit(
                kind=job.kind,
                payload=job.payload,
                handler=handler,
                priority=PersistPriority.CRITICAL,
                attempt=max(1, job.attempt),
                identity_key=job.identity_key,
                write_ahead=False,
            )
            # If still full, submit spilled again; stop this pass.
            if job.identity_key not in self._inflight_identities and self.depth >= self._maxsize:
                break
            if job.identity_key in self._inflight_identities:
                restored += 1
        if restored:
            self._mark_degraded(f"requeued_pending={restored}")
            logger.warning(
                "persist recovery re-queued %s durable CRITICAL job(s) in-process",
                restored,
            )
        return restored

    def _mark_degraded(self, reason: str) -> None:
        self._degraded = True
        self._degraded_reason = reason

    def clear_degraded_if_idle(self) -> None:
        if self._recovery_store is not None and self._recovery_store.pending_count() > 0:
            return
        if self.depth > 0:
            return
        if self.pending_durable_writes > 0:
            return
        self._degraded = False
        self._degraded_reason = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                job = await asyncio.wait_for(
                    self._queue.get(), timeout=_REQUEUE_IDLE_INTERVAL_S
                )
            except TimeoutError:
                # In-process recovery: queue-full spills must not wait for restart.
                if self._recovery_store is not None and self.depth < self._maxsize:
                    with contextlib.suppress(PersistRecoveryError):
                        self.recover_pending()
                self.clear_degraded_if_idle()
                continue
            try:
                await self._execute(job)
            finally:
                self._queue.task_done()

    def _release_inflight(self, identity_key: str | None) -> None:
        if identity_key:
            self._inflight_identities.discard(identity_key)

    async def _execute(self, job: PersistJob) -> None:
        handler = job.handler or self._handlers.get(job.kind)
        if handler is None:
            self._errors += 1
            logger.error("persistence job has no handler kind=%s", job.kind)
            if job.priority == int(PersistPriority.CRITICAL):
                self._spill_failed(job, "missing_handler")
            self._release_inflight(job.identity_key)
            return
        # Await the single tracked write-ahead (worker task — not poll loop).
        try:
            await self._await_durable_write(job)
        except PersistRecoveryError as exc:
            await self._handle_critical_failure(job, handler, str(exc))
            return
        try:
            await handler(job.payload)
            self._processed += 1
            if job.identity_key and self._recovery_store is not None:
                self._invalidate_durable_write(job.identity_key)
                self._recovery_store.mark_done(job.identity_key)
            self._release_inflight(job.identity_key)
            self.clear_degraded_if_idle()
        except Exception as exc:
            err = f"{type(exc).__name__}:{exc}"
            logger.exception(
                "persistence handler failed kind=%s attempt=%s identity=%s",
                job.kind,
                job.attempt,
                job.identity_key,
            )
            if job.priority != int(PersistPriority.CRITICAL):
                self._release_inflight(job.identity_key)
                return
            await self._handle_critical_failure(job, handler, err)

    async def _handle_critical_failure(
        self,
        job: PersistJob,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
        err: str,
    ) -> None:
        self._errors += 1
        next_attempt = job.attempt + 1
        if next_attempt < self._critical_max_attempts:
            self._critical_retries += 1
            self._mark_degraded(f"critical_retry kind={job.kind}")
            # Ensure durable record exists before requeue (sync OK on worker path).
            spilled = False
            if job.identity_key and self._recovery_store is not None:
                try:
                    self._recovery_store.upsert(
                        identity_key=job.identity_key,
                        kind=job.kind,
                        payload=job.payload,
                        attempt=next_attempt,
                        last_error=err,
                    )
                    spilled = True
                except PersistRecoveryError:
                    self._mark_degraded("persist_recovery_write_failed")
                    logger.exception(
                        "CRITICAL retry spill failed identity=%s; "
                        "requeue with write-ahead so the sale is not lost",
                        job.identity_key,
                    )
            await asyncio.sleep(_CRITICAL_RETRY_DELAY_S * next_attempt)
            # Keep identity in-flight across retry so the background requeue
            # task cannot submit a duplicate while this retry is pending.
            # If sync spill failed, schedule another tracked write-ahead.
            try:
                self.submit(
                    kind=job.kind,
                    payload=job.payload,
                    handler=handler,
                    priority=PersistPriority.CRITICAL,
                    attempt=next_attempt,
                    identity_key=job.identity_key,
                    write_ahead=not spilled,
                )
            except PersistenceQueueFullError:
                self._spill_failed(job, err)
                self._release_inflight(job.identity_key)
            return
        self._spill_failed(job, err)
        self._release_inflight(job.identity_key)

    def _spill_failed(self, job: PersistJob, error: str) -> None:
        self._critical_durable_spills += 1
        self._mark_degraded(f"critical_persist_failed kind={job.kind}")
        key = job.identity_key or f"{job.kind}:{job.seq}"
        if self._recovery_store is not None:
            try:
                self._recovery_store.upsert(
                    identity_key=key,
                    kind=job.kind,
                    payload=job.payload,
                    attempt=job.attempt,
                    last_error=error,
                )
            except PersistRecoveryError:
                logger.exception(
                    "CRITICAL persistence could not spill to durable store "
                    "identity=%s kind=%s — sale retained via degraded signal only",
                    key,
                    job.kind,
                )
                return
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
