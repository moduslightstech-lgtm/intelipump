"""Capture-timestamp ownership tests for permanent serial reader architecture."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path

import pytest

from intelipump_fdc.bench_poll.poll_io import (
    ChunkOwnership,
    StatusPollOutcome,
    send_status_poll_and_read_response,
)
from intelipump_fdc.bench_poll.serial_reader import PermanentSerialReader, SerialChunk
from intelipump_fdc.continuous_poll_bench.session import (
    ContinuousPollSession,
    ContinuousPollSessionConfig,
)
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll

WAYNE_25 = bytes.fromhex(
    "50 30 02 08 00 00 00 00 00 00 00 00 03 04 00 99 "
    "07 07 01 01 00 0e 55 03 fa"
)


def _make_chunk(
    raw: bytes,
    *,
    mono_s: float,
    seq: int,
) -> SerialChunk:
    return SerialChunk(
        raw=raw,
        monotonic_ns=int(mono_s * 1_000_000_000),
        monotonic_s=mono_s,
        timestamp_utc=datetime.now(UTC).isoformat(),
        read_sequence=seq,
    )


@dataclass
class TimestampQueueTransport:
    """Injects SerialChunk objects with explicit capture timestamps."""

    device: str = "/tmp/fake-ownership"
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    _open: bool = False
    _queue: list[SerialChunk] = field(default_factory=list)
    _seq: int = 0
    # Raw bytes released after write; stamped at dequeue (after TX).
    after_write: list[tuple[float, bytes]] = field(default_factory=list)
    _post_write: list[tuple[float, bytes]] = field(default_factory=list)
    _write_mono: float | None = None
    pending: list[SerialChunk] = field(default_factory=list)

    @property
    def is_open(self) -> bool:
        return self._open

    async def open(self) -> None:
        self._open = True

    async def close(self) -> None:
        self._open = False

    async def flush(self) -> None:
        return None

    def inject(self, chunk: SerialChunk) -> None:
        self._queue.append(chunk)

    async def get_chunk(self, timeout_s: float) -> SerialChunk | None:
        deadline = time.monotonic() + max(0.0, timeout_s)
        while time.monotonic() < deadline:
            if self.pending:
                return self.pending.pop(0)
            if self._queue:
                return self._queue.pop(0)
            if self._post_write:
                offset_s, raw = self._post_write[0]
                base = self._write_mono if self._write_mono is not None else time.monotonic()
                ready_at = base + offset_s
                now = time.monotonic()
                if now < ready_at:
                    await asyncio.sleep(min(0.002, ready_at - now))
                    continue
                self._post_write.pop(0)
                self._seq += 1
                # Stamp at delivery with actual monotonic time (>= TX).
                return _make_chunk(raw, mono_s=time.monotonic(), seq=self._seq)
            await asyncio.sleep(0.001)
        return None

    async def write(self, data: bytes) -> int:
        self.write_count += 1
        self.written.append(data)
        self._write_mono = time.monotonic()
        self._post_write.extend(self.after_write)
        return len(data)


@pytest.mark.asyncio
async def test_late_bytes_after_poll1_deadline_never_owned_by_poll2() -> None:
    """(a) Blocking-read-late bytes must never be assigned to Poll 2."""
    transport = TimestampQueueTransport()
    await transport.open()

    # Poll 1: timeout (no owned chunks). Then inject a "late" frame with
    # capture time during poll1 window that was dequeued late — use capture
    # before poll2 TX but after poll1 deadline via pending injection between polls.
    r1 = await send_status_poll_and_read_response(transport, 1, 50)
    assert r1.outcome is StatusPollOutcome.TIMEOUT_NO_RESPONSE
    poll1_deadline = r1.t0 + 0.050

    # Late frame captured after poll1 deadline.
    late_mono = poll1_deadline + 0.010
    transport.inject(_make_chunk(WAYNE_25, mono_s=late_mono, seq=1))

    # Small delay then Poll 2 TX.
    await asyncio.sleep(0.02)
    owned: list[tuple[SerialChunk, ChunkOwnership]] = []
    r2 = await send_status_poll_and_read_response(
        transport,
        1,
        80,
        on_chunk=lambda c, o: owned.append((c, o)),
    )
    # Late chunk relative to poll1 is stale relative to poll2 TX (before TX).
    assert all(o is not ChunkOwnership.OWNED for _, o in owned) or r2.owned_chunks == ()
    assert r2.outcome is not StatusPollOutcome.DATA_RESPONSE or all(
        c.monotonic_s >= r2.t0 for c in r2.owned_chunks
    )
    # Must not attribute the late WAYNE frame as poll2 data.
    if r2.frame is not None:
        assert r2.t0 <= (r2.data_frame.capture_monotonic_ns or 0) / 1e9  # type: ignore[union-attr]
    assert r2.stale_chunks >= 1 or r2.data_frame is None
    assert all(c.raw != WAYNE_25 for c in r2.owned_chunks) or all(
        c.monotonic_s >= r2.t0 and c.monotonic_s < r2.t0 + 0.080
        for c in r2.owned_chunks
    )


@pytest.mark.asyncio
async def test_chunks_before_poll2_tx_are_stale_unowned() -> None:
    """(b) Chunks captured before Poll 2 TX are stale/unowned."""
    transport = TimestampQueueTransport()
    await transport.open()
    # Preload a chunk with capture time in the past relative to upcoming TX.
    past = time.monotonic() - 0.05
    transport.inject(_make_chunk(WAYNE_25, mono_s=past, seq=1))
    owned_flags: list[ChunkOwnership] = []
    response = await send_status_poll_and_read_response(
        transport,
        1,
        80,
        on_chunk=lambda _c, o: owned_flags.append(o),
    )
    assert ChunkOwnership.STALE in owned_flags
    assert response.stale_chunks >= 1
    assert response.outcome is StatusPollOutcome.TIMEOUT_NO_RESPONSE
    assert response.owned_chunks == ()
    assert response.data_frame is None


@pytest.mark.asyncio
async def test_fragmented_frame_within_deadline_assembles() -> None:
    """(c) Frame fragmented across queue entries within one deadline assembles."""
    transport = TimestampQueueTransport(
        after_write=[
            (0.005, WAYNE_25[:10]),
            (0.010, WAYNE_25[10:]),
        ]
    )
    await transport.open()
    response = await send_status_poll_and_read_response(transport, 1, 200)
    assert response.outcome is StatusPollOutcome.DATA_RESPONSE
    assert response.frame is not None
    assert response.frame.raw_frame == WAYNE_25
    assert len(response.owned_chunks) == 2


@pytest.mark.asyncio
async def test_late_frame_logged_but_not_poll2_data() -> None:
    """(d) Late frame fully logged but not counted as Poll 2 data."""
    transport = TimestampQueueTransport()
    await transport.open()
    r1 = await send_status_poll_and_read_response(transport, 1, 40)
    assert r1.outcome is StatusPollOutcome.TIMEOUT_NO_RESPONSE

    # Wait until the late capture time is in the past, then inject.
    late = r1.t0 + 0.060
    await asyncio.sleep(max(0.0, late - time.monotonic()))
    transport.inject(_make_chunk(WAYNE_25, mono_s=late, seq=99))

    logged: list[tuple[bytes, ChunkOwnership]] = []
    r2 = await send_status_poll_and_read_response(
        transport,
        1,
        50,
        on_chunk=lambda c, o: logged.append((c.raw, o)),
    )
    assert any(raw == WAYNE_25 for raw, _ in logged)
    assert all(o is ChunkOwnership.STALE for _, o in logged)
    assert r2.outcome is StatusPollOutcome.TIMEOUT_NO_RESPONSE
    assert r2.stale_chunks >= 1
    assert r2.data_frame is None


@pytest.mark.asyncio
async def test_no_two_pyserial_reads_concurrent() -> None:
    """(e) Permanent reader never runs concurrent serial.read calls."""

    class FakeSer:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self.active = 0
            self.max_active = 0
            self.calls = 0

        @property
        def in_waiting(self) -> int:
            return 0

        def read(self, n: int) -> bytes:
            with self._lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                self.calls += 1
            try:
                time.sleep(0.01)
                return b""
            finally:
                with self._lock:
                    self.active -= 1

    ser = FakeSer()
    reader = PermanentSerialReader(ser, read_chunk_size=64)
    reader.start()
    await asyncio.sleep(0.08)
    reader.stop(join_timeout_s=1.0)
    assert ser.max_active == 1
    assert reader.max_active_reads == 1
    assert ser.calls >= 1


@pytest.mark.asyncio
async def test_tx_events_never_closer_than_300ms(tmp_path: Path) -> None:
    """(f) No two TX events occur less than 300 ms apart."""

    @dataclass
    class SlowAutoTransport:
        device: str = "/tmp/fake-spacing"
        written: list[bytes] = field(default_factory=list)
        write_count: int = 0
        chunks: list[bytes] = field(default_factory=list)
        _open: bool = False
        _read_seq: int = 0

        @property
        def is_open(self) -> bool:
            return self._open

        async def open(self) -> None:
            self._open = True

        async def close(self) -> None:
            self._open = False

        async def flush(self) -> None:
            return None

        async def get_chunk(self, timeout_s: float) -> SerialChunk | None:
            deadline = time.monotonic() + max(0.0, timeout_s)
            while time.monotonic() < deadline:
                if self.chunks:
                    data = self.chunks.pop(0)
                    self._read_seq += 1
                    return SerialChunk(
                        raw=data,
                        monotonic_ns=time.monotonic_ns(),
                        monotonic_s=time.monotonic(),
                        timestamp_utc=datetime.now(UTC).isoformat(),
                        read_sequence=self._read_seq,
                    )
                await asyncio.sleep(0.002)
            return None

        async def write(self, data: bytes) -> int:
            self.write_count += 1
            self.written.append(data)
            # Delayed response to stress scheduling.
            await asyncio.sleep(0.05)
            self.chunks.append(WAYNE_25)
            return len(data)

    transport = SlowAutoTransport()
    session = ContinuousPollSession(
        transport,  # type: ignore[arg-type]
        ContinuousPollSessionConfig(
            port="/tmp/fake",
            addresses=(1,),
            baud=9600,
            duration_seconds=1.5,
            poll_interval_ms=300,
            response_timeout_ms=250,
            evidence_jsonl=tmp_path / "e.jsonl",
            evidence_md=tmp_path / "e.md",
            max_writes=20,
            simulator_validation=True,
        ),
    )
    summary = await session.run()
    assert int(summary["pollsSent"]) >= 2  # type: ignore[arg-type]
    import json

    records = [
        json.loads(line)
        for line in (tmp_path / "e.jsonl").read_text().splitlines()
        if line
    ]
    tx_ns = [r["monotonicNs"] for r in records if r.get("direction") == "TX"]
    assert len(tx_ns) >= 2
    gaps_ms = [(b - a) / 1e6 for a, b in pairwise(tx_ns)]
    assert all(g >= 295 for g in gaps_ms), gaps_ms


@pytest.mark.asyncio
async def test_capture_timestamps_reflect_actual_time() -> None:
    """(g) timestampUtc and monotonic timestamps reflect actual capture time."""
    transport = TimestampQueueTransport()
    await transport.open()
    mono_before = time.monotonic()
    utc_before = datetime.now(UTC)
    transport.after_write = [(0.0, WAYNE_25)]
    response = await send_status_poll_and_read_response(transport, 1, 200)
    mono_after = time.monotonic()
    utc_after = datetime.now(UTC)
    assert response.outcome is StatusPollOutcome.DATA_RESPONSE
    assert mono_before <= response.t0 <= mono_after
    tx_utc = datetime.fromisoformat(response.timestamp_utc_tx)
    assert utc_before <= tx_utc <= utc_after
    assert len(response.owned_chunks) == 1
    chunk = response.owned_chunks[0]
    assert mono_before <= chunk.monotonic_s <= mono_after
    chunk_utc = datetime.fromisoformat(chunk.timestamp_utc)
    assert utc_before <= chunk_utc <= utc_after
    assert response.poll_tx == build_poll(1)
