"""Streaming DART frame assembler for arbitrary byte chunks."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from intelipump_fdc.protocol.dart.line.constants import DLE, MAX_BUFFER_SIZE, SF
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame, ParseError


class AssemblerEventKind(StrEnum):
    FRAME = "FRAME"
    REJECTED = "REJECTED"
    OVERFLOW = "OVERFLOW"
    NOISE = "NOISE"


@dataclass(frozen=True, slots=True)
class AssemblerEvent:
    kind: AssemblerEventKind
    raw: bytes
    frame: DartLineFrame | None = None
    error: ParseError | None = None
    message: str = ""


class FrameStreamAssembler:
    """Accumulate wire bytes and emit frames ending at an unescaped SF.

    Escaped ``DLE SF`` (``10 FA``) is never treated as a frame terminator.
    Rejected/noise bytes are preserved on events (no silent loss).
    """

    def __init__(self, *, max_buffer: int = MAX_BUFFER_SIZE * 2) -> None:
        if max_buffer < 3:
            raise ValueError("max_buffer must be >= 3")
        self._max_buffer = max_buffer
        self._buf = bytearray()
        self._after_dle = False

    @property
    def pending_size(self) -> int:
        return len(self._buf) + (1 if self._after_dle else 0)

    def reset(self) -> bytes:
        """Clear internal state; return discarded bytes for diagnostics."""
        discarded = bytes(self._buf)
        self._buf.clear()
        self._after_dle = False
        return discarded

    def feed(self, data: bytes) -> list[AssemblerEvent]:
        events: list[AssemblerEvent] = []
        for byte in data:
            events.extend(self._feed_byte(byte))
        return events

    def _feed_byte(self, byte: int) -> list[AssemblerEvent]:
        if self._after_dle:
            self._after_dle = False
            self._buf.append(byte)
            # Escaped SF is data, not a terminator.
            return self._check_overflow()

        if byte == DLE:
            self._buf.append(DLE)
            self._after_dle = True
            return self._check_overflow()

        self._buf.append(byte)
        if byte != SF:
            return self._check_overflow()

        # Unescaped SF: frame candidate.
        raw = bytes(self._buf)
        self._buf.clear()
        self._after_dle = False

        if len(raw) < 3:
            return [
                AssemblerEvent(
                    kind=AssemblerEventKind.NOISE,
                    raw=raw,
                    message="SF with fewer than 3 bytes; preserved as noise",
                )
            ]

        parsed = parse_frame(raw)
        if isinstance(parsed, ParseError):
            return [
                AssemblerEvent(
                    kind=AssemblerEventKind.REJECTED,
                    raw=raw,
                    error=parsed,
                    message=parsed.message,
                )
            ]
        return [
            AssemblerEvent(
                kind=AssemblerEventKind.FRAME,
                raw=raw,
                frame=parsed,
                message="frame assembled",
            )
        ]

    def _check_overflow(self) -> list[AssemblerEvent]:
        # Include pending DLE marker in effective size.
        size = len(self._buf) + (1 if self._after_dle else 0)
        if size <= self._max_buffer:
            return []
        discarded = self.reset()
        return [
            AssemblerEvent(
                kind=AssemblerEventKind.OVERFLOW,
                raw=discarded,
                message=f"buffer exceeded max_buffer={self._max_buffer}; reset",
            )
        ]
