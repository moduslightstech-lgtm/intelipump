"""Shared helpers for real-Wayne single-shot active writes."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

from intelipump_fdc.bench_poll.poll_io import (
    StatusPollOutcome,
    send_status_poll_and_read_response,
)
from intelipump_fdc.bench_poll.transport import BenchByteTransport
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.protocol.dart.line.captured_classify import CapturedFrameClass
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.real_wayne_price.status_decode import (
    DecodedStatusSnapshot,
    StatusPreconditionError,
    decode_status_frame,
)


@dataclass(slots=True)
class AckWaitResult:
    """ACK wait outcome; unpacks as ``(matched, outcome, observed_hex)``."""

    matched: bool
    outcome: str
    observed_hex: list[str]
    not_before_monotonic_s: float
    ack_monotonic_s: float | None = None
    ack_latency_ms: float | None = None
    stale_rejected_hex: list[str] = field(default_factory=list)

    def __iter__(self) -> Iterator[object]:
        yield self.matched
        yield self.outcome
        yield self.observed_hex


async def drain_pending_rx(
    transport: BenchByteTransport,
    *,
    quiet_ms: float = 20.0,
    max_chunks: int = 64,
) -> list[str]:
    """Discard pending RX chunks before an active write (stale-ACK defense).

    Empties anything already queued, then waits a short quiet window for the
    permanent reader to deliver OS-buffered bytes. Never transmits.
    """
    drained: list[str] = []

    while len(drained) < max_chunks:
        chunk = await transport.get_chunk(0.0)
        if chunk is None or chunk.is_error or not chunk.raw:
            break
        drained.append(chunk.raw.hex(" "))

    if quiet_ms <= 0:
        return drained

    deadline = time.monotonic() + (quiet_ms / 1000.0)
    while time.monotonic() < deadline and len(drained) < max_chunks:
        remaining = deadline - time.monotonic()
        chunk = await transport.get_chunk(min(0.01, max(0.0, remaining)))
        if chunk is None or chunk.is_error or not chunk.raw:
            continue
        drained.append(chunk.raw.hex(" "))
    return drained


async def wait_for_ack_frame(
    transport: BenchByteTransport,
    *,
    expected_ack: bytes,
    timeout_ms: int,
    not_before_monotonic_s: float | None = None,
) -> AckWaitResult:
    """Wait for an ACK observed strictly after ``not_before_monotonic_s``.

    Chunks with ``monotonic_s < not_before`` are recorded as rejected stale
    bytes and never produce ``ACK_MATCH``. When ``not_before`` is omitted,
    the wait start time is used (legacy behavior for other benches).
    """
    assembler = LegacyIgemStreamAssembler()
    observed: list[str] = []
    stale_rejected: list[str] = []
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    not_before = (
        time.monotonic()
        if not_before_monotonic_s is None
        else float(not_before_monotonic_s)
    )
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        chunk = await transport.get_chunk(min(remaining, 0.05))
        if chunk is None or chunk.is_error or not chunk.raw:
            continue
        if chunk.monotonic_s < not_before:
            stale_rejected.append(chunk.raw.hex(" "))
            continue
        for event in assembler.feed(chunk.raw):
            if event.kind is not AssemblerEventKind.FRAME:
                continue
            raw = event.raw
            observed.append(raw.hex(" "))
            view = event.captured
            matched = raw == expected_ack or (
                view is not None
                and view.classification is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK
                and len(raw) == 3
                and raw[0] == expected_ack[0]
                and raw[1] == expected_ack[1]
            )
            if matched:
                latency_ms = (chunk.monotonic_s - not_before) * 1000.0
                return AckWaitResult(
                    matched=True,
                    outcome="ACK_MATCH",
                    observed_hex=observed,
                    not_before_monotonic_s=not_before,
                    ack_monotonic_s=chunk.monotonic_s,
                    ack_latency_ms=latency_ms,
                    stale_rejected_hex=stale_rejected,
                )
    return AckWaitResult(
        matched=False,
        outcome="ACK_TIMEOUT",
        observed_hex=observed,
        not_before_monotonic_s=not_before,
        ack_monotonic_s=None,
        ack_latency_ms=None,
        stale_rejected_hex=stale_rejected,
    )


async def poll_status_until(
    transport: BenchByteTransport,
    *,
    logical_address: int,
    wire_address: int,
    response_timeout_ms: int,
    predicate: Callable[[DecodedStatusSnapshot], bool],
    settle_ms: int,
    max_attempts: int,
    failure_label: str,
) -> tuple[DecodedStatusSnapshot, DartLineFrame, int, list[str]]:
    """Poll until predicate matches or attempts exhausted.

    Returns ``(snap, frame, poll_count, attempt_notes)``.
    """
    notes: list[str] = []
    last_snap: DecodedStatusSnapshot | None = None
    last_frame: DartLineFrame | None = None
    polls = 0
    for attempt in range(1, max_attempts + 1):
        if settle_ms > 0:
            await asyncio.sleep(settle_ms / 1000.0)
        response = await send_status_poll_and_read_response(
            transport,
            logical_address,
            response_timeout_ms,
        )
        polls += 1
        if response.outcome is not StatusPollOutcome.DATA_RESPONSE:
            notes.append(f"attempt{attempt}:{response.outcome.value}")
            continue
        assert response.frame is not None
        snap = decode_status_frame(
            response.frame, expected_wire_address=wire_address
        )
        last_snap = snap
        last_frame = response.frame
        notes.append(
            f"attempt{attempt}:dc1={snap.dc1_name}/{snap.dc1_code}"
        )
        if predicate(snap):
            return snap, response.frame, polls, notes
    if last_snap is None or last_frame is None:
        raise StatusPreconditionError(
            f"{failure_label}: no DATA status response",
            reasons=[f"{failure_label}_no_data", *notes],
            poll_count=polls,
        )
    raise StatusPreconditionError(
        f"{failure_label}: status not reached "
        f"(last={last_snap.dc1_name}/{last_snap.dc1_code})",
        reasons=[
            f"{failure_label}_not_reached",
            f"last_dc1={last_snap.dc1_name}/{last_snap.dc1_code}",
            *notes,
        ],
        last_snap=last_snap,
        last_frame=last_frame,
        poll_count=polls,
    )


def dc1_is(status: WaynePumpStatus) -> Callable[[DecodedStatusSnapshot], bool]:
    code = int(status)

    def _pred(snap: DecodedStatusSnapshot) -> bool:
        return snap.dc1_code == code and bool(snap.crc_valid)

    return _pred


def next_sequence_nibble(sequence: int) -> int:
    if not 0 <= sequence <= 0x0F:
        raise ValueError(f"sequence out of range: {sequence}")
    return 0 if sequence == 0x0F else sequence + 1


def sequence_stale_status_hint(
    *,
    sequence: int,
    ack_outcome: str,
    refusal_reasons: list[str],
) -> str | None:
    """Optional diagnostic when ACK'd but DC1 did not change."""
    if ack_outcome != "ACK_MATCH":
        return None
    joined = " ".join(refusal_reasons)
    if "_not_reached" not in joined and "dc1_not_" not in joined:
        return None
    nxt = next_sequence_nibble(sequence)
    ctrl_cur = 0x30 | (sequence & 0x0F)
    ctrl_nxt = 0x30 | (nxt & 0x0F)
    return (
        "ACK_MATCH with unchanged DC1: not necessarily a duplicate sequence. "
        f"DATA CTRL low nibble encodes sequence on the wire "
        f"(0x{ctrl_cur:02X}→0x{ctrl_nxt:02X}). "
        f"If the prior active write already used a lower sequence, try "
        f"--sequence {nxt} once after confirming pre-write RX drain / "
        f"post-write ACK timing in evidence; otherwise check DART alternate "
        f"flow (nozzle OUT then RESET) and confirm the display physically "
        f"before further TX"
    )
