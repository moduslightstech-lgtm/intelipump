"""Bounded async persistence worker (non-blocking for protocol loop).

CRITICAL jobs (sale completion, audit) are never silently discarded:
bounded in-memory retries, write-ahead durable spill, in-process requeue
when the memory queue was full, and an explicit degraded health signal.

Sale write-ahead (off serial loop) — durable handoff first
---------------------------------------------------------
CRITICAL completions schedule exactly one tracked durable write via
``asyncio.to_thread`` **before** memory-queue accept. ``submit`` still
returns without blocking the serial/poll loop on fsync. The job is
``put_nowait`` only after that write succeeds (or on the rare sync
queue-full spill path). The worker awaits the same tracked task before
running the sale handler.

Completed write tasks (success or failure) stay in ``_pending_writes`` until
``_await_durable_write`` consumes them, so a fast failure cannot be mistaken
for "no write scheduled / success".

Crash window (narrowed)
-----------------------
While handoff is in flight (write scheduled, not yet durable), the sale is
not yet on the recovery store and not yet on the memory queue. Evidence must
remain held (RESET gated via ``is_handoff_pending``) until durable. After
durable handoff, a hard crash before SQLite is recoverable via
``recover_pending``. Queue-full CRITICAL spills fsync synchronously on the
submit path. Late writes after ``mark_done`` are suppressed via epoch.

If the final durable spill fails after attempts are exhausted, the sale is
retained in-memory (degraded health) for retry when storage returns — never
released as completed.
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


class PersistFlushIncompleteError(Exception):
    """Raised when ``stop(flush=True)`` cannot make all CRITICAL sales durable.

    Covers undurable CRITICAL work still in memory at flush end: retained sales,
    queue-join timeout (blocked handler), and pending write-ahead timeout.
    Those jobs are **not** safely on durable storage for process exit. Raising
    this error must never be treated as a successful / safe durability shutdown.

    The remaining hard-crash window (handoff scheduled, write not yet durable,
    while RESET is still gated) cannot be closed by flush alone; restart must
    surface capture uncertainty if the face was cleared anyway.
    """

    def __init__(
        self,
        *,
        retained_count: int = 0,
        identity_keys: tuple[str, ...] = (),
        reasons: tuple[str, ...] = (),
        queue_depth: int = 0,
        pending_writes: int = 0,
    ) -> None:
        self.retained_count = retained_count
        self.identity_keys = identity_keys
        self.reasons = reasons
        self.queue_depth = queue_depth
        self.pending_writes = pending_writes
        reason_txt = ",".join(reasons) if reasons else "undurable_critical"
        keys = ", ".join(identity_keys[:5])
        extra = f" identities={keys}" if keys else ""
        super().__init__(
            f"persist flush incomplete ({reason_txt}): "
            f"retained={retained_count} queue_depth={queue_depth} "
            f"pending_writes={pending_writes}{extra}"
        )

    def operator_message(self) -> str:
        """Explicit operator-facing text: undurable CRITICAL work is not safe."""
        reason_txt = ", ".join(self.reasons) if self.reasons else "undurable_critical"
        return (
            "SALE_DURABILITY_NOT_SAFE: graceful flush incomplete "
            f"({reason_txt}); "
            f"{self.retained_count} sale(s) in _retained_sales, "
            f"queue_depth={self.queue_depth}, "
            f"pending_writes={self.pending_writes}. "
            f"identities={list(self.identity_keys)}. "
            "These CRITICAL jobs are not durably stored for process exit and "
            "may be LOST. Do not treat shutdown as durable success."
        )


# Bounded shutdown flush policy:
# 1) Within one deadline, drain: pending durable writes → handoff callbacks
#    (enqueue) → retained spill → queue join / handler completion. Repeat until
#    idle or the deadline expires. Do not cancel in-flight writes on a partial
#    wait timeout (that would drop identity keys and mis-report durability).
# 2) Snapshot undurable CRITICAL state (timeouts / retained / inflight /
#    handoff-pending) BEFORE cancelling worker tasks; then cancel; then raise
#    PersistFlushIncompleteError if anything was undurable.
# 3) Distinguish safely-durable backlog (on recovery store, awaiting handler)
#    from undurable work (handoff not yet spilled / retained memory-only).
# Hard crash caveat: write scheduled but not yet durable remains an unavoidable
# memory-only loss window (RESET stays gated via handoff_pending).


def identity_address(identity_key: str | None) -> int | None:
    """Parse dart address from ``kind:addr:tx:event`` persist identity keys."""
    if not identity_key:
        return None
    parts = identity_key.split(":")
    if len(parts) < 3:
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


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


@dataclass
class _RetainedSale:
    """CRITICAL sale held after final spill failure (storage unavailable)."""

    kind: str
    payload: dict[str, Any]
    identity_key: str
    handler: Callable[[dict[str, Any]], Awaitable[None]]
    last_error: str
    attempt: int


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
        # Write scheduled but not yet durable / queued (RESET must wait).
        self._handoff_pending: set[str] = set()
        self._handoff_durable: set[str] = set()
        self._handoff_listeners: list[Callable[[str, bool], None]] = []
        self._capture_uncertainty = 0
        # Tracked durable write per sale — kept until worker consumes the result.
        self._pending_writes: dict[str, asyncio.Task[None]] = {}
        # Bumped on mark_done / invalidate so a late to_thread upsert cannot recreate.
        self._write_epoch: dict[str, int] = {}
        # Sales that could not be spilled after final failure (retry when disk returns).
        self._retained_sales: dict[str, _RetainedSale] = {}
        # Serializes retained-sale spill/submit across _run and _requeue_while_busy.
        # Created in start() / first retry so it binds to the running event loop.
        self._retained_lock: asyncio.Lock | None = None

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
    def handoff_pending_count(self) -> int:
        return len(self._handoff_pending)

    @property
    def capture_uncertainty_count(self) -> int:
        return self._capture_uncertainty

    def is_handoff_pending(self, identity_key: str | None) -> bool:
        if not identity_key:
            return False
        return identity_key in self._handoff_pending

    def is_address_handoff_pending(self, address: int) -> bool:
        """True if any incomplete durable handoff is tied to this dart address."""
        return any(
            identity_address(key) == address for key in self._handoff_pending
        )

    def is_handoff_durable(self, identity_key: str | None) -> bool:
        if not identity_key:
            return False
        return identity_key in self._handoff_durable

    def on_handoff(self, listener: Callable[[str, bool], None]) -> None:
        """Notify ``listener(identity_key, durable_ok)`` when handoff settles."""
        self._handoff_listeners.append(listener)

    def note_capture_uncertainty(self, *, reason: str) -> None:
        self._capture_uncertainty += 1
        self._mark_degraded(f"capture_uncertainty:{reason}")
        logger.warning("capture_uncertainty reason=%s count=%s", reason, self._capture_uncertainty)

    def _notify_handoff(self, identity_key: str, *, durable_ok: bool) -> None:
        for listener in list(self._handoff_listeners):
            try:
                listener(identity_key, durable_ok)
            except Exception:  # noqa: BLE001 — never break persist path
                logger.exception("handoff listener failed identity=%s", identity_key)

    @property
    def retained_sale_count(self) -> int:
        return len(self._retained_sales)

    @property
    def pending_durable_writes(self) -> int:
        return sum(1 for t in self._pending_writes.values() if not t.done())

    @property
    def is_degraded(self) -> bool:
        if self._degraded:
            return True
        if self._retained_sales:
            return True
        if self._recovery_store is not None and self._recovery_store.pending_count() > 0:
            return True
        return False

    @property
    def degraded_reason(self) -> str | None:
        if self._degraded_reason:
            return self._degraded_reason
        if self._retained_sales:
            return f"critical_undurable_retained={len(self._retained_sales)}"
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
            if self._retained_lock is None:
                self._retained_lock = asyncio.Lock()
            self._task = asyncio.create_task(self._run(), name="persistence-worker")
            # Requeue spilled CRITICAL jobs even while a handler is still running
            # (must not wait for an empty queue or process restart).
            self._requeue_task = asyncio.create_task(
                self._requeue_while_busy(), name="persistence-requeue"
            )

    async def stop(self, *, flush: bool = True, timeout_s: float = 5.0) -> None:
        flush_error: PersistFlushIncompleteError | None = None
        if flush:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + max(0.0, float(timeout_s))

            def _remaining() -> float:
                return max(0.0, deadline - loop.time())

            reasons: list[str] = []
            await self._drain_flush(deadline=deadline, reasons=reasons)

            # Snapshot undurable CRITICAL state BEFORE cancelling tasks.
            retained_keys = tuple(self._retained_sales.keys())
            inflight_keys = tuple(sorted(self._inflight_identities))
            pending_write_keys = tuple(
                sorted(k for k, t in self._pending_writes.items() if not t.done())
            )
            handoff_keys = tuple(sorted(self._handoff_pending))
            identity_keys = tuple(
                dict.fromkeys(
                    (*retained_keys, *inflight_keys, *pending_write_keys, *handoff_keys)
                )
            )
            if retained_keys and "retained_undurable" not in reasons:
                # Still memory-only after spill attempt / timeout.
                if "retained_spill_timeout" not in reasons:
                    reasons.append("retained_undurable")
            # Handoff still open with no durable spill → undurable crash window.
            if handoff_keys and "pending_write_timeout" not in reasons:
                if any(k not in self._handoff_durable for k in handoff_keys):
                    if "handoff_undurable" not in reasons:
                        reasons.append("handoff_undurable")
            queue_depth = self.depth
            pending_writes = self.pending_durable_writes
            incomplete = bool(
                reasons
                or retained_keys
                or queue_depth > 0
                or pending_writes > 0
                or handoff_keys
            )
            if incomplete:
                if not reasons:
                    reasons.append("undurable_critical")
                flush_error = PersistFlushIncompleteError(
                    retained_count=len(retained_keys),
                    identity_keys=identity_keys,
                    reasons=tuple(dict.fromkeys(reasons)),
                    queue_depth=queue_depth,
                    pending_writes=pending_writes,
                )
                self._mark_degraded(
                    f"flush_incomplete:{','.join(flush_error.reasons)}"
                )
                logger.error("%s", flush_error.operator_message())

        self._stop.set()
        for task in (self._requeue_task, self._task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._requeue_task = None
        self._task = None
        for write_task in list(self._pending_writes.values()):
            if not write_task.done():
                write_task.cancel()
        self._pending_writes.clear()
        if flush_error is not None:
            raise flush_error

    def _flush_idle(self) -> bool:
        """True when no durable-handoff / retained / queue work remains."""
        if self.pending_durable_writes > 0:
            return False
        if self._handoff_pending:
            return False
        if self._retained_sales:
            return False
        if self.depth > 0:
            return False
        # Unfinished join work: unfinished tasks_done accounting.
        if not self._queue.empty():
            return False
        return True

    async def _drain_flush(
        self, *, deadline: float, reasons: list[str]
    ) -> None:
        """Coordinate writes, handoff enqueue, retained spill, and handlers.

        Bounded by ``deadline`` (monotonic). Does not cancel in-flight durable
        writes on a partial timeout — cancel happens only after snapshot in
        ``stop``.
        """
        loop = asyncio.get_running_loop()

        def _remaining() -> float:
            return max(0.0, deadline - loop.time())

        # Cap iterations so a pathological callback loop cannot spin forever
        # inside a long timeout_s.
        for _ in range(64):
            rem = _remaining()
            if rem <= 0:
                if not self._flush_idle():
                    if self.pending_durable_writes > 0 or self._handoff_pending:
                        if "pending_write_timeout" not in reasons:
                            reasons.append("pending_write_timeout")
                    elif self._retained_sales:
                        if "retained_spill_timeout" not in reasons:
                            reasons.append("retained_spill_timeout")
                    elif self.depth > 0:
                        if "queue_join_timeout" not in reasons:
                            reasons.append("queue_join_timeout")
                return

            # 1) Await in-flight durable writes (do not cancel on timeout).
            if self.pending_durable_writes > 0:
                if not await self._flush_pending_writes(timeout_s=rem):
                    if "pending_write_timeout" not in reasons:
                        reasons.append("pending_write_timeout")
                    return

            # 2) Let handoff done-callbacks enqueue jobs after write success.
            await asyncio.sleep(0)

            # 3) Spill retained (storage-recovery) sales while time remains.
            if self._retained_sales:
                rem = _remaining()
                if rem <= 0:
                    if "retained_spill_timeout" not in reasons:
                        reasons.append("retained_spill_timeout")
                    return
                try:
                    await asyncio.wait_for(
                        self._retry_retained_sales(), timeout=rem
                    )
                except TimeoutError:
                    if "retained_spill_timeout" not in reasons:
                        reasons.append("retained_spill_timeout")
                    return
                except PersistRecoveryError:
                    # Spill failed; retained stays for snapshot.
                    pass
                await asyncio.sleep(0)

            # 4) Drain memory queue / handlers (safely-durable backlog).
            rem = _remaining()
            if rem <= 0:
                if self.depth > 0 and "queue_join_timeout" not in reasons:
                    reasons.append("queue_join_timeout")
                return
            try:
                await asyncio.wait_for(self._queue.join(), timeout=rem)
            except TimeoutError:
                if "queue_join_timeout" not in reasons:
                    reasons.append("queue_join_timeout")
                return

            # 5) Callbacks from just-finished handlers may schedule more work.
            await asyncio.sleep(0)
            if self._flush_idle():
                return
            # More handoff / retained / queue work appeared — continue loop.

    async def _flush_pending_writes(self, *, timeout_s: float) -> bool:
        """Await in-flight write-ahead tasks. Returns False if timed out.

        Uses ``asyncio.wait`` so a timeout does **not** cancel the durable
        write tasks (``wait_for(gather)`` would cancel them and drop identity
        keys from the incomplete-flush snapshot).
        """
        pending = [t for t in self._pending_writes.values() if not t.done()]
        if not pending:
            return True
        _done, still = await asyncio.wait(pending, timeout=max(0.0, timeout_s))
        return not still

    async def _requeue_while_busy(self) -> None:
        """Pull durable spills / retained sales into the memory queue."""
        while not self._stop.is_set():
            try:
                await asyncio.sleep(_REQUEUE_IDLE_INTERVAL_S)
                await self._retry_retained_sales()
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
        # Durable handoff first for CRITICAL write-ahead: schedule disk spill,
        # then enqueue only after success. Serial loop never awaits fsync.
        if (
            do_write_ahead
            and key is not None
            and self._recovery_store is not None
            and priority is PersistPriority.CRITICAL
        ):
            # One durable identity → one handoff / one effective completion.
            # Repeated submissions while a write is blocked, after durable spill,
            # or while retained must not attach another callback or enqueue twice.
            # Leave a finished failed write in ``_pending_writes`` until the
            # worker consumes it via ``_await_durable_write`` (fast-failure
            # regression) — do not pop it here.
            existing_write = self._pending_writes.get(key)
            if (
                key in self._inflight_identities
                or key in self._retained_sales
                or key in self._handoff_durable
                or (
                    key in self._handoff_pending
                    and existing_write is not None
                    and not existing_write.done()
                )
            ):
                return
            self._handoff_pending.add(key)
            self._handoff_durable.discard(key)
            task = self._schedule_durable_upsert(
                identity_key=key,
                kind=kind,
                payload=payload,
                attempt=attempt,
            )
            if task is None:
                # No running loop — sync write already completed in schedule.
                self._handoff_pending.discard(key)
                self._handoff_durable.add(key)
                self._notify_handoff(key, durable_ok=True)
                self._enqueue_job(job, key=key)
                return

            def _after_handoff(done: asyncio.Task[None]) -> None:
                try:
                    exc = done.exception()
                except asyncio.CancelledError:
                    return
                if exc is not None:
                    logger.error(
                        "durable handoff failed identity=%s; retaining sale; "
                        "holding evidence gate",
                        key,
                    )
                    handler = job.handler or self._handlers.get(job.kind)
                    self._retain_undurable_sale(job, handler, str(exc))
                    self._notify_handoff(key, durable_ok=False)
                    return
                self._handoff_pending.discard(key)
                self._handoff_durable.add(key)
                self._notify_handoff(key, durable_ok=True)
                self._enqueue_job(job, key=key)

            task.add_done_callback(_after_handoff)
            return

        self._enqueue_job(job, key=key)
        if do_write_ahead and key is not None:
            self._schedule_durable_upsert(
                identity_key=key,
                kind=kind,
                payload=payload,
                attempt=attempt,
            )

    def _enqueue_job(self, job: PersistJob, *, key: str | None) -> None:
        try:
            self._queue.put_nowait(job)
            if key is not None:
                self._inflight_identities.add(key)
        except asyncio.QueueFull as exc:
            if job.priority == int(PersistPriority.CRITICAL):
                if key is not None and self._recovery_store is not None:
                    # Already durable (handoff-first) or sync spill now.
                    if key not in self._handoff_durable:
                        self._recovery_store.upsert(
                            identity_key=key,
                            kind=job.kind,
                            payload=job.payload,
                            attempt=job.attempt,
                            last_error="queue_full",
                        )
                        self._handoff_durable.add(key)
                        self._handoff_pending.discard(key)
                    self._critical_durable_spills += 1
                    self._mark_degraded("critical_queue_full_spilled_to_durable")
                    logger.error(
                        "critical persistence queue full; spilled to durable store "
                        "identity=%s kind=%s (will requeue in-process)",
                        key,
                        job.kind,
                    )
                    return
                raise PersistenceQueueFullError(
                    "critical persistence queue full; refusing to drop"
                ) from exc
            self._dropped_normal += 1
            return

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

        Disk I/O runs in a worker thread. A completed task is left in place until
        the persistence worker consumes it (success or failure).
        """
        store = self._recovery_store
        if store is None:
            return None

        existing = self._pending_writes.get(identity_key)
        # Reuse in-flight *or* completed-but-unconsumed result.
        if existing is not None:
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
        # No done-callback removal — worker consumes via _await_durable_write.
        return task

    def _invalidate_durable_write(self, identity_key: str) -> None:
        """Bump epoch so any in-flight/stale upsert cannot recreate after done."""
        self._write_epoch[identity_key] = self._write_epoch.get(identity_key, 0) + 1
        self._handoff_pending.discard(identity_key)
        self._handoff_durable.discard(identity_key)
        # Bound epoch map growth — only the current counter is needed.
        if len(self._write_epoch) > 256:
            keep = set(self._pending_writes) | self._handoff_pending | set(
                self._retained_sales
            )
            self._write_epoch = {
                k: v for k, v in self._write_epoch.items() if k in keep or k == identity_key
            }

    async def _await_durable_write(self, job: PersistJob) -> None:
        """Block the worker (not the serial loop) until write-ahead has settled.

        Consumes the tracked task (including a already-finished failure) so a
        fast write error cannot look like "no write / success".
        """
        if (
            job.priority != int(PersistPriority.CRITICAL)
            or job.identity_key is None
            or self._recovery_store is None
        ):
            return
        key = job.identity_key
        task = self._pending_writes.get(key)
        if task is None:
            return
        try:
            await task
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise PersistRecoveryIOError(
                f"write-ahead failed for {key}: {exc}"
            ) from exc
        finally:
            # Consume only after the worker has observed the result.
            if self._pending_writes.get(key) is task:
                self._pending_writes.pop(key, None)

    async def _store_upsert(
        self,
        *,
        identity_key: str,
        kind: str,
        payload: dict[str, Any],
        attempt: int,
        last_error: str | None = None,
    ) -> None:
        """Durable upsert off the controller/worker event-loop thread."""
        store = self._recovery_store
        if store is None:
            raise PersistRecoveryIOError("no recovery store configured")
        await asyncio.to_thread(
            lambda: store.upsert(
                identity_key=identity_key,
                kind=kind,
                payload=payload,
                attempt=attempt,
                last_error=last_error,
            )
        )

    async def _store_mark_done(self, identity_key: str) -> None:
        store = self._recovery_store
        if store is None:
            return
        await asyncio.to_thread(store.mark_done, identity_key)

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
            if job.identity_key in self._retained_sales:
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

    async def _retry_retained_sales(self) -> int:
        """When storage returns, spill retained sales and re-queue for handler work.

        Serialized so ``_run`` and ``_requeue_while_busy`` cannot spill/submit the
        same retained sale concurrently.
        """
        async with self._ensure_retained_lock():
            if not self._retained_sales or self._recovery_store is None:
                return 0
            restored = 0
            for key, retained in list(self._retained_sales.items()):
                if self.depth >= self._maxsize:
                    break
                try:
                    await self._store_upsert(
                        identity_key=retained.identity_key,
                        kind=retained.kind,
                        payload=retained.payload,
                        attempt=0,
                        last_error=retained.last_error,
                    )
                except PersistRecoveryError:
                    # Storage still unavailable — keep retained + degraded.
                    continue
                self._retained_sales.pop(key, None)
                self._critical_durable_spills += 1
                # Confirmed durable spill: settle handoff gate so RESET/price
                # can resume for this address. Notify before enqueue so listeners
                # see durable_ok while identity is still attributable.
                self._handoff_pending.discard(key)
                self._handoff_durable.add(key)
                self._notify_handoff(key, durable_ok=True)
                # Fresh attempts once durable again.
                try:
                    self.submit(
                        kind=retained.kind,
                        payload=retained.payload,
                        handler=retained.handler,
                        priority=PersistPriority.CRITICAL,
                        attempt=0,
                        identity_key=retained.identity_key,
                        write_ahead=False,
                    )
                except PersistenceQueueFullError:
                    # Already on disk — recover_pending will pick it up.
                    self._release_inflight(retained.identity_key)
                    restored += 1
                    continue
                restored += 1
                logger.warning(
                    "retained CRITICAL sale spilled after storage recovery identity=%s",
                    key,
                )
            if restored and not self._retained_sales:
                self.clear_degraded_if_idle()
            return restored

    def _ensure_retained_lock(self) -> asyncio.Lock:
        if self._retained_lock is None:
            self._retained_lock = asyncio.Lock()
        return self._retained_lock

    def _mark_degraded(self, reason: str) -> None:
        self._degraded = True
        self._degraded_reason = reason

    def clear_degraded_if_idle(self) -> None:
        if self._retained_sales:
            return
        if self._recovery_store is not None and self._recovery_store.pending_count() > 0:
            return
        if self.depth > 0:
            return
        if self.pending_durable_writes > 0:
            return
        if self._pending_writes:
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
                await self._retry_retained_sales()
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
                await self._spill_failed(job, "missing_handler")
            else:
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
                await self._store_mark_done(job.identity_key)
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
            # Ensure durable record exists before requeue (off event-loop thread).
            spilled = False
            if job.identity_key and self._recovery_store is not None:
                try:
                    await self._store_upsert(
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
            # If spill failed, schedule another tracked write-ahead.
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
                await self._spill_failed(job, err)
            return
        await self._spill_failed(job, err)

    def _retain_undurable_sale(
        self,
        job: PersistJob,
        handler: Callable[[dict[str, Any]], Awaitable[None]] | None,
        error: str,
    ) -> None:
        key = job.identity_key or f"{job.kind}:{job.seq}"
        resolved = handler or job.handler or self._handlers.get(job.kind)
        if resolved is None:
            self._mark_degraded(f"critical_undurable_no_handler kind={job.kind}")
            logger.error(
                "CRITICAL sale undurable and no handler to retain identity=%s",
                key,
            )
            return
        self._retained_sales[key] = _RetainedSale(
            kind=job.kind,
            payload=job.payload,
            identity_key=key,
            handler=resolved,
            last_error=error,
            attempt=job.attempt,
        )
        # Keep identity reserved so we do not treat the sale as completed.
        self._inflight_identities.add(key)
        self._mark_degraded("critical_undurable_retained")
        logger.error(
            "CRITICAL sale retained for retry after durable spill failure "
            "identity=%s kind=%s error=%s",
            key,
            job.kind,
            error,
        )

    async def _spill_failed(self, job: PersistJob, error: str) -> bool:
        """Final durable spill. Returns True if landed on disk; else retains sale."""
        self._mark_degraded(f"critical_persist_failed kind={job.kind}")
        key = job.identity_key or f"{job.kind}:{job.seq}"
        handler = job.handler or self._handlers.get(job.kind)
        if self._recovery_store is None:
            if handler is not None and job.identity_key:
                self._retain_undurable_sale(job, handler, error)
            else:
                logger.error(
                    "CRITICAL persistence failed with no recovery store; "
                    "job retained only in degraded signal identity=%s kind=%s error=%s",
                    key,
                    job.kind,
                    error,
                )
            return False
        try:
            await self._store_upsert(
                identity_key=key,
                kind=job.kind,
                payload=job.payload,
                attempt=job.attempt,
                last_error=error,
            )
        except PersistRecoveryError:
            logger.exception(
                "CRITICAL persistence could not spill to durable store "
                "identity=%s kind=%s — retaining sale for retry",
                key,
                job.kind,
            )
            self._retain_undurable_sale(job, handler, error)
            return False
        self._critical_durable_spills += 1
        self._release_inflight(job.identity_key)
        logger.error(
            "CRITICAL persistence spilled to durable store identity=%s "
            "kind=%s attempts=%s error=%s",
            key,
            job.kind,
            job.attempt,
            error,
        )
        return True
