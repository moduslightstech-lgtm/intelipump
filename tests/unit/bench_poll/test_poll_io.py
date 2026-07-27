"""Shared status-poll I/O: SHORT_CONTROL_70 is interim; DATA_FRAME completes."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import pytest

from intelipump_fdc.bench_poll.poll_io import (
    StatusPollOutcome,
    send_status_poll_and_read_response,
)
from intelipump_fdc.protocol.dart.line.captured_classify import CapturedFrameClass
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll

WAYNE_25 = bytes.fromhex(
    "50 30 02 08 00 00 00 00 00 00 00 00 03 04 00 99 "
    "07 07 01 01 00 0e 55 03 fa"
)
SHORT_70 = bytes.fromhex("50 70 FA")


@dataclass
class ScriptedTransport:
    """Delivers scripted RX chunks after each POLL write."""

    device: str = "/tmp/fake-poll-io"
    chunks: list[bytes] = field(default_factory=list)
    written: list[bytes] = field(default_factory=list)
    write_count: int = 0
    flush_count: int = 0
    read_calls: int = 0
    _open: bool = False
    _queue: list[bytes] = field(default_factory=list)

    @property
    def is_open(self) -> bool:
        return self._open

    async def open(self) -> None:
        self._open = True

    async def close(self) -> None:
        self._open = False

    async def flush(self) -> None:
        self.flush_count += 1

    async def read(self, max_bytes: int) -> bytes:
        self.read_calls += 1
        if not self._queue:
            await asyncio.sleep(0.002)
            return b""
        data = self._queue.pop(0)
        return data[:max_bytes]

    async def write(self, data: bytes) -> int:
        self.write_count += 1
        self.written.append(data)
        self._queue.extend(self.chunks)
        return len(data)


@pytest.mark.asyncio
async def test_70_then_data_frame_is_data_response() -> None:
    transport = ScriptedTransport(chunks=[SHORT_70, WAYNE_25])
    await transport.open()
    observed: list[str] = []
    response = await send_status_poll_and_read_response(
        transport,
        1,
        250,
        on_observed_frame=lambda f: observed.append(f.classification),
    )
    assert response.outcome is StatusPollOutcome.DATA_RESPONSE
    assert observed == [
        CapturedFrameClass.SHORT_CONTROL_70.value,
        CapturedFrameClass.DATA_FRAME.value,
    ]
    assert len(response.control_frames) == 1
    assert response.frame is not None
    assert response.frame.raw_frame == WAYNE_25
    assert response.captured is not None
    assert response.captured.classification is CapturedFrameClass.DATA_FRAME
    assert transport.write_count == 1


@pytest.mark.asyncio
async def test_70_only_until_deadline_is_control_only() -> None:
    transport = ScriptedTransport(chunks=[SHORT_70])
    await transport.open()
    response = await send_status_poll_and_read_response(transport, 1, 80)
    assert response.outcome is StatusPollOutcome.CONTROL_ONLY
    assert len(response.control_frames) == 1
    assert response.control_frames[0].frame.raw_frame == SHORT_70
    assert response.data_frame is None
    assert transport.write_count == 1


@pytest.mark.asyncio
async def test_data_frame_directly_is_data_response() -> None:
    transport = ScriptedTransport(chunks=[WAYNE_25])
    await transport.open()
    response = await send_status_poll_and_read_response(transport, 1, 250)
    assert response.outcome is StatusPollOutcome.DATA_RESPONSE
    assert response.control_frames == ()
    assert response.frame is not None
    assert response.frame.raw_frame == WAYNE_25


@pytest.mark.asyncio
async def test_fragmented_70_and_fragmented_data_lose_no_bytes() -> None:
    """Fragmented 70 then fragmented DATA must reassemble without loss."""
    fragments = [
        SHORT_70[0:1],
        SHORT_70[1:],
        WAYNE_25[0:1],
        WAYNE_25[1:12],
        WAYNE_25[12:],
    ]
    assert b"".join(fragments) == SHORT_70 + WAYNE_25

    transport = ScriptedTransport(chunks=list(fragments))
    await transport.open()
    seen: list[bytes] = []
    response = await send_status_poll_and_read_response(
        transport,
        1,
        250,
        on_chunk=seen.append,
    )
    assert response.outcome is StatusPollOutcome.DATA_RESPONSE
    assert b"".join(response.chunks) == SHORT_70 + WAYNE_25
    assert b"".join(seen) == SHORT_70 + WAYNE_25
    assert response.chunks == tuple(fragments)
    assert response.frame is not None
    assert response.frame.raw_frame == WAYNE_25
    assert response.frame.crc_valid is True
    assert len(response.control_frames) == 1
    assert response.control_frames[0].frame.raw_frame == SHORT_70


@pytest.mark.asyncio
async def test_single_and_continuous_identical_chunk_and_frame_handling() -> None:
    """Both benches must call the same function and see the same chunks/frame."""
    fragments = [SHORT_70, WAYNE_25[0:1], WAYNE_25[1:]]

    async def _run_once() -> tuple[tuple[bytes, ...], bytes, StatusPollOutcome]:
        transport = ScriptedTransport(chunks=list(fragments))
        await transport.open()
        response = await send_status_poll_and_read_response(transport, 1, 250)
        assert response.frame is not None
        return response.chunks, response.frame.raw_frame, response.outcome

    single_chunks, single_frame, single_outcome = await _run_once()
    continuous_chunks, continuous_frame, continuous_outcome = await _run_once()

    assert single_outcome is StatusPollOutcome.DATA_RESPONSE
    assert continuous_outcome is StatusPollOutcome.DATA_RESPONSE
    assert single_chunks == continuous_chunks == tuple(fragments)
    assert single_frame == continuous_frame == WAYNE_25
    assert b"".join(single_chunks) == SHORT_70 + WAYNE_25


@pytest.mark.asyncio
async def test_no_second_reader_and_cancelled_read_byte_loss() -> None:
    """Only one read path; TX flush then reads; no concurrent drain."""
    transport = ScriptedTransport(chunks=[WAYNE_25])
    await transport.open()
    order: list[str] = []

    orig_write = transport.write
    orig_flush = transport.flush
    orig_read = transport.read

    async def write(data: bytes) -> int:
        order.append("write")
        return await orig_write(data)

    async def flush() -> None:
        order.append("flush")
        await orig_flush()

    async def read(max_bytes: int) -> bytes:
        order.append("read")
        return await orig_read(max_bytes)

    transport.write = write  # type: ignore[method-assign]
    transport.flush = flush  # type: ignore[method-assign]
    transport.read = read  # type: ignore[method-assign]

    response = await send_status_poll_and_read_response(transport, 1, 250)
    assert response.outcome is StatusPollOutcome.DATA_RESPONSE
    assert order[:3] == ["write", "flush", "read"]
    assert "drain" not in order
    assert transport.write_count == 1
    assert response.poll_tx == build_poll(1)


@pytest.mark.asyncio
async def test_timeout_no_response_when_idle() -> None:
    transport = ScriptedTransport(chunks=[])
    await transport.open()
    response = await send_status_poll_and_read_response(transport, 1, 40)
    assert response.outcome is StatusPollOutcome.TIMEOUT_NO_RESPONSE
    assert response.observed_frames == ()
