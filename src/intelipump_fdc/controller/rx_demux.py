"""Per-address RX demultiplexer with capture-time frame ownership.

One shared stream assembler feeds independent queues for wire addresses
``0x50`` / ``0x51`` so one consumer never discards another address's frames.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from enum import StrEnum

from intelipump_fdc.protocol.dart.line.addressing import (
    LEGACY_IGEM_WIRE_ADDRESS_SET,
    decode_wire_address,
)
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEvent,
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)
from intelipump_fdc.protocol.dart.line.models import DartLineFrame


class DemuxDiagnosticKind(StrEnum):
    REJECTED = "REJECTED"
    OVERFLOW = "OVERFLOW"
    NOISE = "NOISE"
    PARTIAL = "PARTIAL"
    DROPPED_UNKNOWN_ADDRESS = "DROPPED_UNKNOWN_ADDRESS"
    QUEUE_OVERFLOW = "QUEUE_OVERFLOW"


@dataclass(frozen=True, slots=True)
class TimestampedFrame:
    """Assembled line frame with first/last byte monotonic capture times."""

    frame: DartLineFrame
    raw: bytes
    first_byte_time: float
    last_byte_time: float
    wire_address: int
    logical_address: int | None = None

    @property
    def is_short_bus_response(self) -> bool:
        return self.frame.control_type in {
            ControlType.EOT,
            ControlType.ACK,
            ControlType.NAK,
            ControlType.POLL,
        }


@dataclass(frozen=True, slots=True)
class DemuxDiagnostic:
    kind: DemuxDiagnosticKind
    raw: bytes
    message: str = ""
    capture_mono: float | None = None


class AddressFrameDemux:
    """Reconstruct frames from a byte stream; route by wire address."""

    def __init__(
        self,
        wire_addresses: tuple[int, ...] = (0x50, 0x51),
        *,
        queue_maxsize: int = 64,
        quiet_gap_timeout_s: float = 0.015,
        max_buffer: int = 512,
    ) -> None:
        allowed = tuple(
            a for a in wire_addresses if a in LEGACY_IGEM_WIRE_ADDRESS_SET
        )
        if not allowed:
            raise ValueError("wire_addresses must include at least one legacy iGEM ADR")
        self.wire_addresses = allowed
        self.queue_maxsize = queue_maxsize
        self.quiet_gap_timeout_s = quiet_gap_timeout_s
        self._assembler = LegacyIgemStreamAssembler(max_buffer=max_buffer)
        self._queues: dict[int, asyncio.Queue[TimestampedFrame]] = {
            addr: asyncio.Queue(maxsize=queue_maxsize) for addr in allowed
        }
        self._frame_first_byte_time: float | None = None
        self._last_byte_mono: float | None = None
        self._pending_byte_times: list[float] = []
        self.diagnostics: list[DemuxDiagnostic] = []
        self.stale_frame_count = 0
        self.malformed_count = 0
        self.queue_overflow_count = 0
        self.dropped_unknown_address_count = 0

    def reset(self) -> None:
        self._assembler.reset()
        self._frame_first_byte_time = None
        self._last_byte_mono = None
        self._pending_byte_times.clear()
        for q in self._queues.values():
            while not q.empty():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    break

    def pending_size(self, wire_address: int) -> int:
        q = self._queues.get(wire_address)
        return 0 if q is None else q.qsize()

    def feed(
        self,
        data: bytes,
        *,
        capture_mono: float | None = None,
        byte_times: list[float] | None = None,
    ) -> list[DemuxDiagnostic]:
        """Feed RX bytes; return diagnostics for malformed / overflow traffic."""
        if not data:
            return []
        now = capture_mono if capture_mono is not None else time.monotonic()
        diags: list[DemuxDiagnostic] = []
        for idx, byte in enumerate(data):
            t = (
                byte_times[idx]
                if byte_times is not None and idx < len(byte_times)
                else now
            )
            self._last_byte_mono = t
            if self._assembler.pending_size == 0 and self._frame_first_byte_time is None:
                self._frame_first_byte_time = t
            self._pending_byte_times.append(t)
            for event in self._assembler.feed(bytes((byte,))):
                diags.extend(self._handle_assembler_event(event, last_byte_time=t))
        self.diagnostics.extend(diags)
        return diags

    def maybe_expire_partial(self, *, now: float | None = None) -> list[DemuxDiagnostic]:
        """Quiet-gap recovery for stuck incomplete frames."""
        if self._assembler.pending_size == 0:
            return []
        current = now if now is not None else time.monotonic()
        last = self._last_byte_mono
        if last is None or (current - last) < self.quiet_gap_timeout_s:
            return []
        events = self._assembler.expire_partial()
        self._frame_first_byte_time = None
        self._pending_byte_times.clear()
        diags: list[DemuxDiagnostic] = []
        for event in events:
            diags.append(
                DemuxDiagnostic(
                    kind=DemuxDiagnosticKind.PARTIAL,
                    raw=event.raw,
                    message=event.message or "quiet_gap_partial_expire",
                    capture_mono=current,
                )
            )
            self.malformed_count += 1
        self.diagnostics.extend(diags)
        return diags

    async def get(
        self,
        wire_address: int,
        *,
        timeout_s: float,
    ) -> TimestampedFrame | None:
        """Pop one frame for ``wire_address`` or ``None`` on temporary empty."""
        q = self._queues.get(wire_address)
        if q is None:
            return None
        if timeout_s <= 0:
            try:
                return q.get_nowait()
            except asyncio.QueueEmpty:
                return None
        try:
            return await asyncio.wait_for(q.get(), timeout=timeout_s)
        except TimeoutError:
            return None

    def get_nowait(self, wire_address: int) -> TimestampedFrame | None:
        q = self._queues.get(wire_address)
        if q is None:
            return None
        try:
            return q.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def _handle_assembler_event(
        self, event: AssemblerEvent, *, last_byte_time: float
    ) -> list[DemuxDiagnostic]:
        if event.kind is AssemblerEventKind.FRAME and event.frame is not None:
            raw = event.raw
            n = len(raw)
            times = self._pending_byte_times[:n]
            del self._pending_byte_times[:n]
            first = (
                times[0]
                if times
                else (
                    self._frame_first_byte_time
                    if self._frame_first_byte_time is not None
                    else last_byte_time
                )
            )
            last = times[-1] if times else last_byte_time
            self._frame_first_byte_time = None
            return self._enqueue_frame(event.frame, raw=raw, first=first, last=last)

        if event.raw:
            n = min(len(event.raw), len(self._pending_byte_times))
            del self._pending_byte_times[:n]
        self._frame_first_byte_time = None
        kind_map = {
            AssemblerEventKind.REJECTED: DemuxDiagnosticKind.REJECTED,
            AssemblerEventKind.OVERFLOW: DemuxDiagnosticKind.OVERFLOW,
            AssemblerEventKind.NOISE: DemuxDiagnosticKind.NOISE,
            AssemblerEventKind.PARTIAL: DemuxDiagnosticKind.PARTIAL,
        }
        kind = kind_map.get(event.kind, DemuxDiagnosticKind.REJECTED)
        self.malformed_count += 1
        return [
            DemuxDiagnostic(
                kind=kind,
                raw=event.raw,
                message=event.message,
                capture_mono=last_byte_time,
            )
        ]

    def _enqueue_frame(
        self,
        frame: DartLineFrame,
        *,
        raw: bytes,
        first: float,
        last: float,
    ) -> list[DemuxDiagnostic]:
        wire = frame.address
        logical: int | None
        try:
            logical = decode_wire_address(wire)
        except Exception:
            logical = None
        stamped = TimestampedFrame(
            frame=frame,
            raw=raw,
            first_byte_time=first,
            last_byte_time=last,
            wire_address=wire,
            logical_address=logical,
        )
        q = self._queues.get(wire)
        if q is None:
            self.dropped_unknown_address_count += 1
            return [
                DemuxDiagnostic(
                    kind=DemuxDiagnosticKind.DROPPED_UNKNOWN_ADDRESS,
                    raw=raw,
                    message=f"wire=0x{wire:02X}",
                    capture_mono=last,
                )
            ]
        try:
            q.put_nowait(stamped)
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                q.get_nowait()
            try:
                q.put_nowait(stamped)
            except asyncio.QueueFull:
                self.queue_overflow_count += 1
                return [
                    DemuxDiagnostic(
                        kind=DemuxDiagnosticKind.QUEUE_OVERFLOW,
                        raw=raw,
                        message=f"wire=0x{wire:02X} queue full",
                        capture_mono=last,
                    )
                ]
            self.queue_overflow_count += 1
            return [
                DemuxDiagnostic(
                    kind=DemuxDiagnosticKind.QUEUE_OVERFLOW,
                    raw=raw,
                    message=f"wire=0x{wire:02X} dropped oldest",
                    capture_mono=last,
                )
            ]
        return []
