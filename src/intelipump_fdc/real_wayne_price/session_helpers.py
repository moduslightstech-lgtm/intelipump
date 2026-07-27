"""Shared helpers for real-Wayne single-shot active writes."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

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


async def wait_for_ack_frame(
    transport: BenchByteTransport,
    *,
    expected_ack: bytes,
    timeout_ms: int,
) -> tuple[bool, str, list[str]]:
    assembler = LegacyIgemStreamAssembler()
    observed: list[str] = []
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    t0 = time.monotonic()
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        chunk = await transport.get_chunk(min(remaining, 0.05))
        if chunk is None or chunk.is_error or not chunk.raw:
            continue
        if chunk.monotonic_s < t0:
            continue
        for event in assembler.feed(chunk.raw):
            if event.kind is not AssemblerEventKind.FRAME:
                continue
            raw = event.raw
            observed.append(raw.hex(" "))
            view = event.captured
            if raw == expected_ack:
                return True, "ACK_MATCH", observed
            if (
                view is not None
                and view.classification is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK
                and len(raw) == 3
                and raw[0] == expected_ack[0]
                and raw[1] == expected_ack[1]
            ):
                return True, "ACK_MATCH", observed
    return False, "ACK_TIMEOUT", observed


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
        )
    raise StatusPreconditionError(
        f"{failure_label}: status not reached "
        f"(last={last_snap.dc1_name}/{last_snap.dc1_code})",
        reasons=[
            f"{failure_label}_not_reached",
            f"last_dc1={last_snap.dc1_name}/{last_snap.dc1_code}",
            *notes,
        ],
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
    """Hint when L2 ACK'd but DC1 did not change (common duplicate-sequence case)."""
    if ack_outcome != "ACK_MATCH":
        return None
    joined = " ".join(refusal_reasons)
    if "_not_reached" not in joined and "dc1_not_" not in joined:
        return None
    nxt = next_sequence_nibble(sequence)
    return (
        "ACK_MATCH with unchanged DC1 often means DART L2 treated this DATA "
        f"frame as a retransmission of sequence {sequence}; retry once with "
        f"--sequence {nxt} (advance after each accepted active write)"
    )
