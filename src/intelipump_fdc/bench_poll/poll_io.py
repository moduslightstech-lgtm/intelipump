"""Shared status-poll TX/RX used by single-poll and continuous benches.

Both ``intelipump-poll-bench`` and ``intelipump-continuous-poll-bench`` must call
:func:`send_status_poll_and_read_response` for every poll cycle. Continuous mode
must not maintain a divergent receive path.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from intelipump_fdc.bench_poll.transport import BenchByteTransport
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.captured_classify import CapturedFrameView
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEvent,
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.protocol.dart.transport.errors import TransportNotOpenError

logger = logging.getLogger(__name__)

_TRANSIENT_EMPTY_MARKER = "device reports readiness to read but returned no data"
_MAX_CONSECUTIVE_TRANSIENT_EMPTY = 3
_TRANSIENT_EMPTY_GRACE_MS = 50


class StatusPollOutcome(StrEnum):
    FRAME = "frame"
    TIMEOUT = "timeout"
    DISCONNECT = "disconnect"
    STOPPED = "stopped"
    OVERFLOW = "overflow"
    REJECTED = "rejected"


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
    latency_ms: float | None = None
    frame: DartLineFrame | None = None
    captured: CapturedFrameView | None = None
    terminal_event: AssemblerEvent | None = None
    partial_events: tuple[AssemblerEvent, ...] = ()
    disconnect_error: BaseException | None = None
    message: str | None = None


def _is_transient_empty_read(exc: BaseException) -> bool:
    return _TRANSIENT_EMPTY_MARKER in str(exc).lower()


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


async def _read_one_chunk(
    transport: BenchByteTransport,
    *,
    read_size: int,
    remaining_s: float,
    consecutive_transient_empty: int,
    on_transient_empty: Callable[[BaseException, int], None] | None,
) -> tuple[bytes | None, int, BaseException | None]:
    """Read one chunk; ``None`` bytes means confirmed disconnect.

    Matches the single-poll wait slice (``min(remaining, 0.05)``) but relies on
    :class:`BenchPollSerialTransport` parking any in-flight UART bytes if the
    await is cancelled — so cancelled ``to_thread`` work cannot drop bytes.
    """
    if remaining_s <= 0:
        return b"", consecutive_transient_empty, None
    grace_deadline = time.monotonic() + min(
        remaining_s, _TRANSIENT_EMPTY_GRACE_MS / 1000.0
    )
    while True:
        try:
            chunk = await asyncio.wait_for(
                transport.read(read_size),
                timeout=min(remaining_s, 0.05),
            )
            return chunk, 0, None
        except TimeoutError:
            return b"", consecutive_transient_empty, None
        except TransportNotOpenError as exc:
            return None, consecutive_transient_empty, exc
        except OSError as exc:
            if _is_transient_empty_read(exc):
                if not _device_still_usable(transport):
                    return None, consecutive_transient_empty, exc
                consecutive_transient_empty += 1
                logger.info(
                    "transient_empty_read count=%s: %s",
                    consecutive_transient_empty,
                    exc,
                )
                if on_transient_empty is not None:
                    on_transient_empty(exc, consecutive_transient_empty)
                if consecutive_transient_empty >= _MAX_CONSECUTIVE_TRANSIENT_EMPTY:
                    return None, consecutive_transient_empty, exc
                if time.monotonic() >= grace_deadline:
                    return b"", consecutive_transient_empty, None
                await asyncio.sleep(0.005)
                remaining_s = max(0.0, grace_deadline - time.monotonic())
                continue
            return None, consecutive_transient_empty, exc


async def send_status_poll_and_read_response(
    transport: BenchByteTransport,
    logical_address: int,
    timeout_ms: int,
    *,
    read_size: int = 256,
    stop_event: asyncio.Event | None = None,
    on_chunk: Callable[[bytes], None] | None = None,
    on_transient_empty: Callable[[BaseException, int], None] | None = None,
) -> StatusPollResponse:
    """Send one verified status POLL and collect the response.

    This is the sole request/response path for both poll benches:

    1. Build and write exactly one ``build_poll(logical_address)`` frame.
    2. Flush TX (when the transport supports it).
    3. Read/buffer serial chunks with a single reader until a complete assembler
       frame, timeout, stop, or confirmed disconnect.
    4. Never call ``reset_input_buffer`` and never use a second drain/read path
       during the response window.
    """
    wire_address = encode_wire_address(logical_address)
    poll = build_poll(logical_address)
    assembler = LegacyIgemStreamAssembler()
    chunks: list[bytes] = []
    t0 = time.monotonic()
    mono_tx_ns = time.monotonic_ns()
    consecutive_transient = 0

    try:
        await transport.write(poll)
        flush = getattr(transport, "flush", None)
        if callable(flush):
            await flush()
    except (OSError, TransportNotOpenError) as exc:
        return StatusPollResponse(
            outcome=StatusPollOutcome.DISCONNECT,
            poll_tx=poll,
            logical_address=logical_address,
            wire_address=wire_address,
            chunks=(),
            t0=t0,
            mono_tx_ns=mono_tx_ns,
            disconnect_error=exc,
            message=str(exc),
        )

    deadline = t0 + (timeout_ms / 1000.0)
    # Persistent buffer across reads within this poll only (fresh assembler).
    while time.monotonic() < deadline and not (
        stop_event is not None and stop_event.is_set()
    ):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        chunk, consecutive_transient, disconnect_exc = await _read_one_chunk(
            transport,
            read_size=read_size,
            remaining_s=remaining,
            consecutive_transient_empty=consecutive_transient,
            on_transient_empty=on_transient_empty,
        )
        if chunk is None:
            return StatusPollResponse(
                outcome=StatusPollOutcome.DISCONNECT,
                poll_tx=poll,
                logical_address=logical_address,
                wire_address=wire_address,
                chunks=tuple(chunks),
                t0=t0,
                mono_tx_ns=mono_tx_ns,
                disconnect_error=disconnect_exc,
                message=str(disconnect_exc) if disconnect_exc else "serial_disconnect",
            )
        if not chunk:
            await asyncio.sleep(0.001)
            continue
        chunks.append(chunk)
        if on_chunk is not None:
            on_chunk(chunk)
        for event in assembler.feed(chunk):
            if event.kind is AssemblerEventKind.NOISE:
                continue
            if event.kind is AssemblerEventKind.PARTIAL:
                continue
            if event.kind is AssemblerEventKind.OVERFLOW:
                return StatusPollResponse(
                    outcome=StatusPollOutcome.OVERFLOW,
                    poll_tx=poll,
                    logical_address=logical_address,
                    wire_address=wire_address,
                    chunks=tuple(chunks),
                    t0=t0,
                    mono_tx_ns=mono_tx_ns,
                    terminal_event=event,
                    message=event.message or "overflow",
                )
            if event.kind is AssemblerEventKind.REJECTED:
                return StatusPollResponse(
                    outcome=StatusPollOutcome.REJECTED,
                    poll_tx=poll,
                    logical_address=logical_address,
                    wire_address=wire_address,
                    chunks=tuple(chunks),
                    t0=t0,
                    mono_tx_ns=mono_tx_ns,
                    terminal_event=event,
                    message=event.message,
                )
            if event.kind is AssemblerEventKind.FRAME:
                latency_ms = (time.monotonic() - t0) * 1000.0
                return StatusPollResponse(
                    outcome=StatusPollOutcome.FRAME,
                    poll_tx=poll,
                    logical_address=logical_address,
                    wire_address=wire_address,
                    chunks=tuple(chunks),
                    t0=t0,
                    mono_tx_ns=mono_tx_ns,
                    latency_ms=latency_ms,
                    frame=event.frame,
                    captured=event.captured,
                    terminal_event=event,
                )

    if stop_event is not None and stop_event.is_set():
        assembler.reset()
        return StatusPollResponse(
            outcome=StatusPollOutcome.STOPPED,
            poll_tx=poll,
            logical_address=logical_address,
            wire_address=wire_address,
            chunks=tuple(chunks),
            t0=t0,
            mono_tx_ns=mono_tx_ns,
            message="operator_interrupt",
        )

    partial = tuple(assembler.expire_partial())
    return StatusPollResponse(
        outcome=StatusPollOutcome.TIMEOUT,
        poll_tx=poll,
        logical_address=logical_address,
        wire_address=wire_address,
        chunks=tuple(chunks),
        t0=t0,
        mono_tx_ns=mono_tx_ns,
        partial_events=partial,
        message="no_frame_before_deadline",
    )
