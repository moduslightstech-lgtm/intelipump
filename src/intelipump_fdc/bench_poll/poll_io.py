"""Shared status-poll TX/RX used by single-poll and continuous benches.

Both ``intelipump-poll-bench`` and ``intelipump-continuous-poll-bench`` must call
:func:`send_status_poll_and_read_response` for every poll cycle. Continuous mode
must not maintain a divergent receive path.

Chunks are consumed only from the transport reader queue. Poll ownership is
determined by each chunk's capture monotonic timestamp relative to TX and the
response deadline — never by dequeue time alone.

``SHORT_CONTROL_70`` is an interim/control response: log and keep reading until
``DATA_FRAME``, disconnect, or the bounded response deadline.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

from intelipump_fdc.bench_poll.serial_reader import SerialChunk
from intelipump_fdc.bench_poll.transport import BenchByteTransport
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.captured_classify import (
    CapturedFrameClass,
    CapturedFrameView,
)
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEvent,
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.protocol.dart.transport.errors import TransportNotOpenError

logger = logging.getLogger(__name__)

# Quiet gap after a timeout before the next TX (not an input drain).
DEFAULT_QUIET_GAP_MS = 25.0
DEFAULT_QUIET_GAP_MAX_WAIT_MS = 500.0


class StatusPollOutcome(StrEnum):
    """Terminal outcome of one poll-cycle response window."""

    DATA_RESPONSE = "data_response"
    CONTROL_ONLY = "control_only"
    TIMEOUT_NO_RESPONSE = "timeout_no_response"
    PROTOCOL_ERROR = "protocol_error"
    DISCONNECT = "disconnect"
    STOPPED = "stopped"


class ChunkOwnership(StrEnum):
    OWNED = "owned"
    STALE = "stale"
    LATE = "late"


@dataclass(frozen=True, slots=True)
class ObservedFrame:
    """One complete protocol frame observed inside a poll-cycle window."""

    frame: DartLineFrame
    captured: CapturedFrameView | None
    latency_ms: float
    event: AssemblerEvent
    ownership: ChunkOwnership = ChunkOwnership.OWNED
    capture_monotonic_ns: int | None = None
    capture_timestamp_utc: str | None = None

    @property
    def classification(self) -> str:
        if self.captured is not None:
            return self.captured.classification.value
        return self.frame.control_type.value

    @property
    def is_short_control_70(self) -> bool:
        return (
            self.captured is not None
            and self.captured.classification is CapturedFrameClass.SHORT_CONTROL_70
        )

    @property
    def is_data_frame(self) -> bool:
        if self.captured is not None:
            return self.captured.classification is CapturedFrameClass.DATA_FRAME
        return False


@dataclass(frozen=True, slots=True)
class StatusPollResponse:
    """Result of one shared status-poll request/response cycle."""

    outcome: StatusPollOutcome
    poll_tx: bytes
    logical_address: int
    wire_address: int
    chunks: tuple[bytes, ...]
    t0: float
    mono_tx_ns: int
    timestamp_utc_tx: str
    observed_frames: tuple[ObservedFrame, ...] = ()
    latency_ms: float | None = None
    frame: DartLineFrame | None = None
    captured: CapturedFrameView | None = None
    terminal_event: AssemblerEvent | None = None
    partial_events: tuple[AssemblerEvent, ...] = ()
    disconnect_error: BaseException | None = None
    message: str | None = None
    stale_chunks: int = 0
    late_chunks: int = 0
    unowned_frames: int = 0
    owned_chunks: tuple[SerialChunk, ...] = ()
    stale_chunk_records: tuple[SerialChunk, ...] = ()
    late_chunk_records: tuple[SerialChunk, ...] = ()

    @property
    def control_frames(self) -> tuple[ObservedFrame, ...]:
        return tuple(
            f
            for f in self.observed_frames
            if f.is_short_control_70 and f.ownership is ChunkOwnership.OWNED
        )

    @property
    def data_frame(self) -> ObservedFrame | None:
        for f in self.observed_frames:
            if f.is_data_frame and f.ownership is ChunkOwnership.OWNED:
                return f
        return None


@dataclass
class PollCycleCounters:
    """Per-cycle ownership counters returned to session stats."""

    stale_chunks: int = 0
    late_chunks: int = 0
    unowned_frames: int = 0
    data_responses: int = 0
    control_responses: int = 0
    timeout_no_response: int = 0


ChunkCallback = Callable[[SerialChunk, ChunkOwnership], None]
FrameCallback = Callable[[ObservedFrame], None]


def _device_still_usable(transport: BenchByteTransport) -> bool:
    if not transport.is_open:
        return False
    checker = getattr(transport, "device_path_exists", None)
    if callable(checker):
        return bool(checker())
    device = getattr(transport, "device", None)
    if device is None:
        return True
    path = str(device)
    if path.startswith(("/tmp", "memory", "pty:")):
        return True
    from pathlib import Path

    return Path(path).exists()


def _is_interim_control(event: AssemblerEvent) -> bool:
    captured = event.captured
    return (
        captured is not None
        and captured.classification is CapturedFrameClass.SHORT_CONTROL_70
    )


def _is_data_frame_event(event: AssemblerEvent) -> bool:
    captured = event.captured
    return (
        captured is not None
        and captured.classification is CapturedFrameClass.DATA_FRAME
    )


def _feed_unowned(
    assembler: LegacyIgemStreamAssembler,
    chunk: SerialChunk,
    ownership: ChunkOwnership,
    *,
    t0: float,
    on_observed_frame: FrameCallback | None,
) -> list[ObservedFrame]:
    """Assemble late/stale bytes for logging only — never as owned poll data."""
    frames: list[ObservedFrame] = []
    for event in assembler.feed(chunk.raw):
        if event.kind is not AssemblerEventKind.FRAME or event.frame is None:
            continue
        latency_ms = (chunk.monotonic_s - t0) * 1000.0
        observed = ObservedFrame(
            frame=event.frame,
            captured=event.captured,
            latency_ms=latency_ms,
            event=event,
            ownership=ownership,
            capture_monotonic_ns=chunk.monotonic_ns,
            capture_timestamp_utc=chunk.timestamp_utc,
        )
        frames.append(observed)
        if on_observed_frame is not None:
            on_observed_frame(observed)
    return frames


async def wait_for_quiet_gap(
    transport: BenchByteTransport,
    *,
    quiet_gap_ms: float = DEFAULT_QUIET_GAP_MS,
    max_wait_ms: float = DEFAULT_QUIET_GAP_MAX_WAIT_MS,
    on_chunk: ChunkCallback | None = None,
    on_observed_frame: FrameCallback | None = None,
    t0_reference: float | None = None,
) -> tuple[int, int]:
    """Wait until no newly captured bytes for ``quiet_gap_ms``.

    Late bytes are dequeued and logged as unowned; nothing is discarded from
    evidence. Returns ``(late_chunks, unowned_frames)``.
    """
    quiet_s = max(0.001, quiet_gap_ms / 1000.0)
    max_wait_s = max(quiet_s, max_wait_ms / 1000.0)
    deadline = time.monotonic() + max_wait_s
    late_chunks = 0
    unowned_frames = 0
    assembler = LegacyIgemStreamAssembler()
    t0 = t0_reference if t0_reference is not None else time.monotonic()

    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        chunk = await transport.get_chunk(min(quiet_s, remaining))
        if chunk is None:
            # No newly captured bytes for the quiet gap.
            return late_chunks, unowned_frames
        if chunk.is_error:
            # Surface disconnect on next poll; treat as ending quiet wait.
            if on_chunk is not None:
                on_chunk(chunk, ChunkOwnership.LATE)
            late_chunks += 1
            return late_chunks, unowned_frames
        late_chunks += 1
        if on_chunk is not None:
            on_chunk(chunk, ChunkOwnership.LATE)
        for observed in _feed_unowned(
            assembler,
            chunk,
            ChunkOwnership.LATE,
            t0=t0,
            on_observed_frame=on_observed_frame,
        ):
            unowned_frames += 1
            _ = observed
    return late_chunks, unowned_frames


async def send_status_poll_and_read_response(
    transport: BenchByteTransport,
    logical_address: int,
    timeout_ms: int,
    *,
    read_size: int = 256,
    stop_event: asyncio.Event | None = None,
    on_chunk: ChunkCallback | None = None,
    on_observed_frame: FrameCallback | None = None,
) -> StatusPollResponse:
    """Send one verified status POLL and collect within one response window.

    Ownership rules (capture timestamp, not dequeue time):

    * ``tx_monotonic <= chunk.monotonic < deadline`` → owned by this poll
    * ``chunk.monotonic < tx_monotonic`` → stale/unowned
    * ``chunk.monotonic >= deadline`` → late; not attached to this poll
    """
    del read_size  # Chunk size is owned by the permanent reader thread.
    wire_address = encode_wire_address(logical_address)
    poll = build_poll(logical_address)
    assembler = LegacyIgemStreamAssembler()
    unowned_assembler = LegacyIgemStreamAssembler()
    owned_raw: list[bytes] = []
    owned_chunks: list[SerialChunk] = []
    stale_records: list[SerialChunk] = []
    late_records: list[SerialChunk] = []
    observed: list[ObservedFrame] = []
    stale_chunks = 0
    late_chunks = 0
    unowned_frames = 0

    # TX timestamps are captured at the actual write/flush point.
    t0 = 0.0
    mono_tx_ns = 0
    timestamp_utc_tx = ""

    try:
        await transport.write(poll)
        flush = getattr(transport, "flush", None)
        if callable(flush):
            await flush()
        t0 = time.monotonic()
        mono_tx_ns = time.monotonic_ns()
        timestamp_utc_tx = datetime.now(UTC).isoformat()
    except (OSError, TransportNotOpenError) as exc:
        t0 = time.monotonic()
        mono_tx_ns = time.monotonic_ns()
        timestamp_utc_tx = datetime.now(UTC).isoformat()
        return StatusPollResponse(
            outcome=StatusPollOutcome.DISCONNECT,
            poll_tx=poll,
            logical_address=logical_address,
            wire_address=wire_address,
            chunks=(),
            t0=t0,
            mono_tx_ns=mono_tx_ns,
            timestamp_utc_tx=timestamp_utc_tx,
            disconnect_error=exc,
            message=str(exc),
        )

    deadline = t0 + (timeout_ms / 1000.0)

    while time.monotonic() < deadline and not (
        stop_event is not None and stop_event.is_set()
    ):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            chunk = await transport.get_chunk(min(remaining, 0.05))
        except TransportNotOpenError as exc:
            return StatusPollResponse(
                outcome=StatusPollOutcome.DISCONNECT,
                poll_tx=poll,
                logical_address=logical_address,
                wire_address=wire_address,
                chunks=tuple(owned_raw),
                t0=t0,
                mono_tx_ns=mono_tx_ns,
                timestamp_utc_tx=timestamp_utc_tx,
                observed_frames=tuple(observed),
                owned_chunks=tuple(owned_chunks),
                stale_chunk_records=tuple(stale_records),
                late_chunk_records=tuple(late_records),
                stale_chunks=stale_chunks,
                late_chunks=late_chunks,
                unowned_frames=unowned_frames,
                disconnect_error=exc,
                message=str(exc),
            )
        except OSError as exc:
            if not _device_still_usable(transport):
                return StatusPollResponse(
                    outcome=StatusPollOutcome.DISCONNECT,
                    poll_tx=poll,
                    logical_address=logical_address,
                    wire_address=wire_address,
                    chunks=tuple(owned_raw),
                    t0=t0,
                    mono_tx_ns=mono_tx_ns,
                    timestamp_utc_tx=timestamp_utc_tx,
                    observed_frames=tuple(observed),
                    owned_chunks=tuple(owned_chunks),
                    stale_chunk_records=tuple(stale_records),
                    late_chunk_records=tuple(late_records),
                    stale_chunks=stale_chunks,
                    late_chunks=late_chunks,
                    unowned_frames=unowned_frames,
                    disconnect_error=exc,
                    message=str(exc),
                )
            continue

        if chunk is None:
            continue

        if chunk.is_error:
            err = chunk.error
            assert err is not None
            if not _device_still_usable(transport):
                return StatusPollResponse(
                    outcome=StatusPollOutcome.DISCONNECT,
                    poll_tx=poll,
                    logical_address=logical_address,
                    wire_address=wire_address,
                    chunks=tuple(owned_raw),
                    t0=t0,
                    mono_tx_ns=mono_tx_ns,
                    timestamp_utc_tx=timestamp_utc_tx,
                    observed_frames=tuple(observed),
                    owned_chunks=tuple(owned_chunks),
                    stale_chunk_records=tuple(stale_records),
                    late_chunk_records=tuple(late_records),
                    stale_chunks=stale_chunks,
                    late_chunks=late_chunks,
                    unowned_frames=unowned_frames,
                    disconnect_error=err,
                    message=str(err),
                )
            # Transient / recoverable: keep waiting inside the window.
            continue

        # Ownership by capture timestamp (not dequeue time).
        if chunk.monotonic_s < t0:
            stale_chunks += 1
            stale_records.append(chunk)
            if on_chunk is not None:
                on_chunk(chunk, ChunkOwnership.STALE)
            for _obs in _feed_unowned(
                unowned_assembler,
                chunk,
                ChunkOwnership.STALE,
                t0=t0,
                on_observed_frame=on_observed_frame,
            ):
                unowned_frames += 1
            continue

        if chunk.monotonic_s >= deadline:
            late_chunks += 1
            late_records.append(chunk)
            if on_chunk is not None:
                on_chunk(chunk, ChunkOwnership.LATE)
            for _obs in _feed_unowned(
                unowned_assembler,
                chunk,
                ChunkOwnership.LATE,
                t0=t0,
                on_observed_frame=on_observed_frame,
            ):
                unowned_frames += 1
            # Later queue items cannot be owned (FIFO capture order).
            break

        owned_raw.append(chunk.raw)
        owned_chunks.append(chunk)
        if on_chunk is not None:
            on_chunk(chunk, ChunkOwnership.OWNED)

        for event in assembler.feed(chunk.raw):
            if event.kind is AssemblerEventKind.NOISE:
                continue
            if event.kind is AssemblerEventKind.PARTIAL:
                continue
            if event.kind is AssemblerEventKind.OVERFLOW:
                return StatusPollResponse(
                    outcome=StatusPollOutcome.PROTOCOL_ERROR,
                    poll_tx=poll,
                    logical_address=logical_address,
                    wire_address=wire_address,
                    chunks=tuple(owned_raw),
                    t0=t0,
                    mono_tx_ns=mono_tx_ns,
                    timestamp_utc_tx=timestamp_utc_tx,
                    observed_frames=tuple(observed),
                    owned_chunks=tuple(owned_chunks),
                    stale_chunk_records=tuple(stale_records),
                    late_chunk_records=tuple(late_records),
                    stale_chunks=stale_chunks,
                    late_chunks=late_chunks,
                    unowned_frames=unowned_frames,
                    terminal_event=event,
                    message=event.message or "overflow",
                )
            if event.kind is AssemblerEventKind.REJECTED:
                return StatusPollResponse(
                    outcome=StatusPollOutcome.PROTOCOL_ERROR,
                    poll_tx=poll,
                    logical_address=logical_address,
                    wire_address=wire_address,
                    chunks=tuple(owned_raw),
                    t0=t0,
                    mono_tx_ns=mono_tx_ns,
                    timestamp_utc_tx=timestamp_utc_tx,
                    observed_frames=tuple(observed),
                    owned_chunks=tuple(owned_chunks),
                    stale_chunk_records=tuple(stale_records),
                    late_chunk_records=tuple(late_records),
                    stale_chunks=stale_chunks,
                    late_chunks=late_chunks,
                    unowned_frames=unowned_frames,
                    terminal_event=event,
                    message=event.message,
                )
            if event.kind is AssemblerEventKind.FRAME:
                assert event.frame is not None
                latency_ms = (chunk.monotonic_s - t0) * 1000.0
                observed_frame = ObservedFrame(
                    frame=event.frame,
                    captured=event.captured,
                    latency_ms=latency_ms,
                    event=event,
                    ownership=ChunkOwnership.OWNED,
                    capture_monotonic_ns=chunk.monotonic_ns,
                    capture_timestamp_utc=chunk.timestamp_utc,
                )
                observed.append(observed_frame)
                if on_observed_frame is not None:
                    on_observed_frame(observed_frame)

                if _is_interim_control(event):
                    logger.info(
                        "interim SHORT_CONTROL_70 latency_ms=%.2f; "
                        "continuing response window",
                        latency_ms,
                    )
                    continue

                if _is_data_frame_event(event):
                    return StatusPollResponse(
                        outcome=StatusPollOutcome.DATA_RESPONSE,
                        poll_tx=poll,
                        logical_address=logical_address,
                        wire_address=wire_address,
                        chunks=tuple(owned_raw),
                        t0=t0,
                        mono_tx_ns=mono_tx_ns,
                        timestamp_utc_tx=timestamp_utc_tx,
                        observed_frames=tuple(observed),
                        owned_chunks=tuple(owned_chunks),
                        stale_chunk_records=tuple(stale_records),
                        late_chunk_records=tuple(late_records),
                        stale_chunks=stale_chunks,
                        late_chunks=late_chunks,
                        unowned_frames=unowned_frames,
                        latency_ms=latency_ms,
                        frame=event.frame,
                        captured=event.captured,
                        terminal_event=event,
                    )

                return StatusPollResponse(
                    outcome=StatusPollOutcome.PROTOCOL_ERROR,
                    poll_tx=poll,
                    logical_address=logical_address,
                    wire_address=wire_address,
                    chunks=tuple(owned_raw),
                    t0=t0,
                    mono_tx_ns=mono_tx_ns,
                    timestamp_utc_tx=timestamp_utc_tx,
                    observed_frames=tuple(observed),
                    owned_chunks=tuple(owned_chunks),
                    stale_chunk_records=tuple(stale_records),
                    late_chunk_records=tuple(late_records),
                    stale_chunks=stale_chunks,
                    late_chunks=late_chunks,
                    unowned_frames=unowned_frames,
                    latency_ms=latency_ms,
                    frame=event.frame,
                    captured=event.captured,
                    terminal_event=event,
                    message=(
                        f"unexpected_frame {observed_frame.classification}"
                    ),
                )

    if stop_event is not None and stop_event.is_set():
        assembler.reset()
        return StatusPollResponse(
            outcome=StatusPollOutcome.STOPPED,
            poll_tx=poll,
            logical_address=logical_address,
            wire_address=wire_address,
            chunks=tuple(owned_raw),
            t0=t0,
            mono_tx_ns=mono_tx_ns,
            timestamp_utc_tx=timestamp_utc_tx,
            observed_frames=tuple(observed),
            owned_chunks=tuple(owned_chunks),
            stale_chunk_records=tuple(stale_records),
            late_chunk_records=tuple(late_records),
            stale_chunks=stale_chunks,
            late_chunks=late_chunks,
            unowned_frames=unowned_frames,
            message="operator_interrupt",
        )

    partial = tuple(assembler.expire_partial())
    owned_observed = [
        f for f in observed if f.ownership is ChunkOwnership.OWNED
    ]
    if owned_observed:
        last = owned_observed[-1]
        return StatusPollResponse(
            outcome=StatusPollOutcome.CONTROL_ONLY,
            poll_tx=poll,
            logical_address=logical_address,
            wire_address=wire_address,
            chunks=tuple(owned_raw),
            t0=t0,
            mono_tx_ns=mono_tx_ns,
            timestamp_utc_tx=timestamp_utc_tx,
            observed_frames=tuple(observed),
            owned_chunks=tuple(owned_chunks),
            stale_chunk_records=tuple(stale_records),
            late_chunk_records=tuple(late_records),
            stale_chunks=stale_chunks,
            late_chunks=late_chunks,
            unowned_frames=unowned_frames,
            latency_ms=last.latency_ms,
            frame=last.frame,
            captured=last.captured,
            terminal_event=last.event,
            partial_events=partial,
            message="control_only_no_data_frame_before_deadline",
        )

    return StatusPollResponse(
        outcome=StatusPollOutcome.TIMEOUT_NO_RESPONSE,
        poll_tx=poll,
        logical_address=logical_address,
        wire_address=wire_address,
        chunks=tuple(owned_raw),
        t0=t0,
        mono_tx_ns=mono_tx_ns,
        timestamp_utc_tx=timestamp_utc_tx,
        observed_frames=tuple(observed),
        owned_chunks=tuple(owned_chunks),
        stale_chunk_records=tuple(stale_records),
        late_chunk_records=tuple(late_records),
        stale_chunks=stale_chunks,
        late_chunks=late_chunks,
        unowned_frames=unowned_frames,
        partial_events=partial,
        message="no_frame_before_deadline",
    )


# Re-export for type checkers / tests that previously imported field helpers.
__all__ = [
    "DEFAULT_QUIET_GAP_MAX_WAIT_MS",
    "DEFAULT_QUIET_GAP_MS",
    "ChunkOwnership",
    "ObservedFrame",
    "PollCycleCounters",
    "StatusPollOutcome",
    "StatusPollResponse",
    "send_status_poll_and_read_response",
    "wait_for_quiet_gap",
]
