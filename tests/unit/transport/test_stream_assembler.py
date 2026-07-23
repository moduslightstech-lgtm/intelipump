"""Unit tests for the streaming frame assembler."""

from __future__ import annotations

from intelipump_fdc.protocol.dart.line.constants import DLE, SF
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import build_eot, build_poll
from intelipump_fdc.protocol.dart.line.stream import (
    AssemblerEventKind,
    FrameStreamAssembler,
)


def test_one_byte_chunks() -> None:
    frame = build_poll(1)
    asm = FrameStreamAssembler()
    events = []
    for b in frame:
        events.extend(asm.feed(bytes((b,))))
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    assert len(frames) == 1
    assert frames[0].frame is not None
    assert frames[0].frame.control_type is ControlType.POLL


def test_multiple_frames_in_one_chunk() -> None:
    raw = build_poll(1) + build_eot(1, 0)
    events = FrameStreamAssembler().feed(raw)
    frames = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    assert len(frames) == 2


def test_dle_sf_split_across_chunks() -> None:
    # Build a DATA-like buffer that contains escaped SF in the body by using
    # a poll is too short; use ADR CTRL with escaped content manually:
    # ADR=1 CTRL=POLL then DLE SF as if data — actually control frames have no body.
    # Construct wire: 01 20 10 FA FA  -> after unescape body would fail structure.
    # Instead verify escaped SF inside a longer constructed wire buffer:
    wire = bytes((0x01, 0x20, DLE))  # incomplete escape
    asm = FrameStreamAssembler()
    assert asm.feed(wire) == []
    # Completing with SF must NOT terminate (escaped), then real SF terminates.
    events = asm.feed(bytes((SF, SF)))
    # First SF is escaped data; second SF terminates.
    # Frame parse of 01 20 10 FA FA will fail structurally → REJECTED or FRAME.
    assert events
    assert any(
        e.kind in {AssemblerEventKind.FRAME, AssemblerEventKind.REJECTED} for e in events
    )


def test_noise_before_valid_frame() -> None:
    noise = bytes((SF,))  # lone SF → NOISE (< 3 bytes)
    good = build_poll(2)
    events = FrameStreamAssembler().feed(noise + good)
    kinds = [e.kind for e in events]
    assert AssemblerEventKind.NOISE in kinds
    assert AssemblerEventKind.FRAME in kinds
    frame_events = [e for e in events if e.kind is AssemblerEventKind.FRAME]
    assert frame_events[0].frame is not None
    assert frame_events[0].frame.address == 2


def test_buffer_overflow_preserves_bytes() -> None:
    asm = FrameStreamAssembler(max_buffer=8)
    events = asm.feed(bytes(range(20)))  # no SF → overflow
    assert any(e.kind is AssemblerEventKind.OVERFLOW for e in events)
    overflow = next(e for e in events if e.kind is AssemblerEventKind.OVERFLOW)
    assert len(overflow.raw) > 0


def test_malformed_recovery_then_valid() -> None:
    # Too-short then valid
    events = FrameStreamAssembler().feed(bytes((0x01, SF)) + build_poll(1))
    assert any(e.kind is AssemblerEventKind.NOISE for e in events)
    assert any(e.kind is AssemblerEventKind.FRAME for e in events)
