"""Permanent serial reader thread for bench poll sessions.

Only this thread calls ``serial.read()``. Chunks are timestamped immediately
after the OS read returns and placed on a thread-safe queue for the asyncio
poll collector. Individual reads are never cancelled.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from queue import Empty, Full, Queue

from intelipump_fdc.protocol.dart.transport.serial import read_serial_chunk

logger = logging.getLogger(__name__)

# Bound queue so a stalled collector cannot grow without limit.
_DEFAULT_QUEUE_MAX = 4096


@dataclass(frozen=True, slots=True)
class SerialChunk:
    """One UART read result with capture-time metadata."""

    raw: bytes
    monotonic_ns: int
    monotonic_s: float
    timestamp_utc: str
    read_sequence: int
    error: BaseException | None = None

    @property
    def is_error(self) -> bool:
        return self.error is not None


class PermanentSerialReader:
    """Background thread: the sole caller of ``serial.read`` for a session."""

    def __init__(
        self,
        ser: object,
        *,
        read_chunk_size: int = 256,
        queue_maxsize: int = _DEFAULT_QUEUE_MAX,
        name: str = "bench-serial-reader",
    ) -> None:
        self._ser = ser
        self._read_chunk_size = read_chunk_size
        self._queue: Queue[SerialChunk] = Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=name,
            daemon=True,
        )
        self._read_sequence = 0
        self._active_reads = 0
        self._max_active_reads = 0
        self._lock = threading.Lock()
        self._started = False

    @property
    def max_active_reads(self) -> int:
        """Peak concurrent ``serial.read`` calls (must stay at 1)."""
        with self._lock:
            return self._max_active_reads

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop.clear()
        self._thread.start()

    def stop(self, *, join_timeout_s: float = 2.0) -> None:
        """Request stop after the current short read finishes; never cancel it."""
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=join_timeout_s)

    def get_nowait(self) -> SerialChunk | None:
        try:
            return self._queue.get_nowait()
        except Empty:
            return None

    def get(self, timeout_s: float) -> SerialChunk | None:
        """Blocking get for bridging from asyncio via ``to_thread``."""
        if timeout_s <= 0:
            return self.get_nowait()
        try:
            return self._queue.get(timeout=timeout_s)
        except Empty:
            return None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                with self._lock:
                    self._active_reads += 1
                    if self._active_reads > self._max_active_reads:
                        self._max_active_reads = self._active_reads
                try:
                    raw = read_serial_chunk(self._ser, self._read_chunk_size)
                finally:
                    with self._lock:
                        self._active_reads -= 1
            except Exception as exc:
                mono_ns = time.monotonic_ns()
                mono_s = time.monotonic()
                utc = datetime.now(UTC).isoformat()
                self._read_sequence += 1
                chunk = SerialChunk(
                    raw=b"",
                    monotonic_ns=mono_ns,
                    monotonic_s=mono_s,
                    timestamp_utc=utc,
                    read_sequence=self._read_sequence,
                    error=exc,
                )
                self._put(chunk)
                logger.warning("permanent serial reader stopping on error: %s", exc)
                return

            # Timestamp immediately after the OS read returns.
            mono_ns = time.monotonic_ns()
            mono_s = time.monotonic()
            utc = datetime.now(UTC).isoformat()
            if not raw:
                continue
            self._read_sequence += 1
            chunk = SerialChunk(
                raw=bytes(raw),
                monotonic_ns=mono_ns,
                monotonic_s=mono_s,
                timestamp_utc=utc,
                read_sequence=self._read_sequence,
            )
            self._put(chunk)

    def _put(self, chunk: SerialChunk) -> None:
        try:
            self._queue.put(chunk, timeout=0.5)
        except Full:
            logger.error(
                "serial chunk queue full; dropping read_sequence=%s bytes=%s",
                chunk.read_sequence,
                len(chunk.raw),
            )
