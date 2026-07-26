"""Persistent-buffer assembler for captured Wayne iGEM / ePump framing.

Rules derived from passive capture evidence:

- Frame starts only at wire ADR 0x50 or 0x51 (never payload bytes like 0x13).
- Short frames: ADR CTRL SF (POLL 0x20, SHORT_CONTROL_70 0x70-0x7F,
  SEQUENCE_CONTROL_OR_ACK 0xC0-0xCF, and other 3-byte controls).
- DATA frames (CTRL 0x30-0x3F): body may contain DLE-escaped 0xFA; terminate
  on the first unescaped SF (capture ends DATA with 03 FA).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, auto

from intelipump_fdc.protocol.dart.line.addressing import (
    LEGACY_IGEM_WIRE_ADDRESS_SET,
    decode_wire_address,
)
from intelipump_fdc.protocol.dart.line.captured_classify import (
    CapturedFrameClass,
    CapturedFrameView,
    InferredDirection,
    PayloadSemantics,
    classify_captured_control,
    infer_direction,
)
from intelipump_fdc.protocol.dart.line.constants import (
    CONTROL_TYPE_MASK,
    DATA_BASE,
    DLE,
    MAX_BUFFER_SIZE,
    SEQUENCE_MASK,
    SF,
)
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame, ParseError


class AssemblerEventKind(StrEnum):
    FRAME = "FRAME"
    REJECTED = "REJECTED"
    OVERFLOW = "OVERFLOW"
    NOISE = "NOISE"
    PARTIAL = "PARTIAL"


@dataclass(frozen=True, slots=True)
class AssemblerEvent:
    kind: AssemblerEventKind
    raw: bytes
    frame: DartLineFrame | None = None
    error: ParseError | None = None
    message: str = ""
    captured: CapturedFrameView | None = None


class _State(StrEnum):
    HUNT = auto()
    NEED_CTRL = auto()
    NEED_SHORT_SF = auto()
    DATA_BODY = auto()


class LegacyIgemStreamAssembler:
    """Accumulate wire bytes; emit frames for the captured iGEM address profile."""

    def __init__(self, *, max_buffer: int = MAX_BUFFER_SIZE * 2) -> None:
        if max_buffer < 3:
            raise ValueError("max_buffer must be >= 3")
        self._max_buffer = max_buffer
        self._buf = bytearray()
        self._state = _State.HUNT
        self._after_dle = False
        self._discarded = bytearray()

    @property
    def pending_size(self) -> int:
        return len(self._buf)

    @property
    def pending_raw(self) -> bytes:
        return bytes(self._buf)

    def reset(self) -> bytes:
        discarded = bytes(self._buf) + bytes(self._discarded)
        self._buf.clear()
        self._discarded.clear()
        self._after_dle = False
        self._state = _State.HUNT
        return discarded

    def take_discarded(self) -> bytes:
        out = bytes(self._discarded)
        self._discarded.clear()
        return out

    def expire_partial(self) -> list[AssemblerEvent]:
        """Partial-frame timeout: preserve diagnostics, do not silently drop."""
        if not self._buf and not self._discarded:
            return []
        raw = bytes(self._buf)
        discarded = self.take_discarded()
        self._buf.clear()
        self._after_dle = False
        self._state = _State.HUNT
        note = "partial_frame_timeout"
        if discarded:
            note = f"{note}; prior_discarded_hex={discarded.hex(' ')}"
        return [
            AssemblerEvent(
                kind=AssemblerEventKind.PARTIAL,
                raw=raw if raw else discarded,
                message=note,
                captured=CapturedFrameView(
                    classification=CapturedFrameClass.PARTIAL_FRAME,
                    wire_address=raw[0] if raw else 0,
                    logical_address=None,
                    control=raw[1] if len(raw) > 1 else 0,
                    sequence_nibble=None,
                    possible_acknowledgement=False,
                    raw_payload_hex=raw.hex(" ") if raw else None,
                    payload_length=len(raw),
                    payload_semantics=PayloadSemantics.UNKNOWN_PAYLOAD,
                    inferred_direction=InferredDirection.UNKNOWN_DIRECTION,
                    crc_valid=None,
                    dart_control_type=None,
                ),
            )
        ]

    def feed(self, data: bytes) -> list[AssemblerEvent]:
        events: list[AssemblerEvent] = []
        for byte in data:
            events.extend(self._feed_byte(byte))
        return events

    def _feed_byte(self, byte: int) -> list[AssemblerEvent]:
        if self._state is _State.HUNT:
            if byte in LEGACY_IGEM_WIRE_ADDRESS_SET:
                self._buf.append(byte)
                self._state = _State.NEED_CTRL
                return self._check_overflow()
            self._discarded.append(byte)
            return []

        if self._state is _State.NEED_CTRL:
            self._buf.append(byte)
            ctrl = byte
            if (ctrl & CONTROL_TYPE_MASK) == DATA_BASE:
                self._state = _State.DATA_BODY
                self._after_dle = False
            else:
                self._state = _State.NEED_SHORT_SF
            return self._check_overflow()

        if self._state is _State.NEED_SHORT_SF:
            self._buf.append(byte)
            if byte != SF:
                raw = bytes(self._buf)
                return self._reject_and_resync(
                    raw, "expected SF terminator for short frame"
                )
            raw = bytes(self._buf)
            self._buf.clear()
            self._state = _State.HUNT
            return self._emit_parsed(raw)

        return self._feed_data_body(byte)

    def _feed_data_body(self, byte: int) -> list[AssemblerEvent]:
        if self._after_dle:
            self._after_dle = False
            self._buf.append(byte)
            return self._check_overflow()

        if byte == DLE:
            self._buf.append(DLE)
            self._after_dle = True
            return self._check_overflow()

        self._buf.append(byte)
        if byte != SF:
            return self._check_overflow()

        # Unescaped SF ends the DATA frame (capture: … 03 FA).
        raw = bytes(self._buf)
        self._buf.clear()
        self._after_dle = False
        self._state = _State.HUNT
        return self._emit_parsed(raw)

    def _emit_parsed(self, raw: bytes) -> list[AssemblerEvent]:
        captured = self._view_from_raw(raw)
        parsed = parse_frame(raw)
        if isinstance(parsed, ParseError):
            return [
                AssemblerEvent(
                    kind=AssemblerEventKind.REJECTED,
                    raw=raw,
                    error=parsed,
                    message=parsed.message,
                    captured=captured,
                )
            ]
        return [
            AssemblerEvent(
                kind=AssemblerEventKind.FRAME,
                raw=raw,
                frame=parsed,
                message="frame assembled",
                captured=captured,
            )
        ]

    def _view_from_raw(self, raw: bytes) -> CapturedFrameView:
        wire = raw[0] if raw else 0
        ctrl = raw[1] if len(raw) > 1 else 0
        classification = (
            classify_captured_control(ctrl)
            if len(raw) >= 2
            else CapturedFrameClass.UNKNOWN_FRAME
        )

        logical: int | None
        try:
            logical = decode_wire_address(wire) if wire else None
        except Exception:
            logical = None

        seq: int | None = None
        possible_ack = False
        if classification is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK:
            seq = ctrl & SEQUENCE_MASK
            possible_ack = True
        elif classification in {
            CapturedFrameClass.DATA_FRAME,
            CapturedFrameClass.SHORT_CONTROL_70,
        }:
            seq = ctrl & SEQUENCE_MASK

        payload = b""
        crc_valid: bool | None = None
        dart_type: ControlType | None = None
        parsed = parse_frame(raw)
        if isinstance(parsed, DartLineFrame):
            payload = parsed.payload
            crc_valid = parsed.crc_valid
            dart_type = parsed.control_type
            if parsed.control_type is ControlType.ACK:
                classification = CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK
                possible_ack = True
                seq = parsed.sequence
            elif parsed.control_type is ControlType.EOT:
                classification = CapturedFrameClass.SHORT_CONTROL_70
            elif parsed.control_type is ControlType.POLL:
                classification = CapturedFrameClass.POLL
            elif parsed.control_type is ControlType.DATA:
                classification = CapturedFrameClass.DATA_FRAME

        return CapturedFrameView(
            classification=classification,
            wire_address=wire,
            logical_address=logical,
            control=ctrl,
            sequence_nibble=seq,
            possible_acknowledgement=possible_ack,
            raw_payload_hex=payload.hex(" ") if payload else None,
            payload_length=len(payload),
            payload_semantics=PayloadSemantics.UNKNOWN_PAYLOAD,
            inferred_direction=infer_direction(classification),
            crc_valid=crc_valid,
            dart_control_type=dart_type,
        )

    def _reject_and_resync(self, raw: bytes, message: str) -> list[AssemblerEvent]:
        events = [
            AssemblerEvent(
                kind=AssemblerEventKind.REJECTED,
                raw=raw,
                message=message,
                captured=self._view_from_raw(raw),
            )
        ]
        self._buf.clear()
        self._after_dle = False
        self._state = _State.HUNT
        for idx, b in enumerate(raw[1:], start=1):
            if b in LEGACY_IGEM_WIRE_ADDRESS_SET:
                for rest in raw[idx:]:
                    events.extend(self._feed_byte(rest))
                break
            self._discarded.append(b)
        return events

    def _check_overflow(self) -> list[AssemblerEvent]:
        if len(self._buf) <= self._max_buffer:
            return []
        discarded = bytes(self._buf) + bytes(self._discarded)
        self._buf.clear()
        self._discarded.clear()
        self._after_dle = False
        self._state = _State.HUNT
        return [
            AssemblerEvent(
                kind=AssemblerEventKind.OVERFLOW,
                raw=discarded,
                message=f"buffer exceeded max_buffer={self._max_buffer}; reset",
                captured=CapturedFrameView(
                    classification=CapturedFrameClass.UNKNOWN_FRAME,
                    wire_address=discarded[0] if discarded else 0,
                    logical_address=None,
                    control=0,
                    sequence_nibble=None,
                    possible_acknowledgement=False,
                    raw_payload_hex=discarded.hex(" "),
                    payload_length=len(discarded),
                    payload_semantics=PayloadSemantics.UNKNOWN_PAYLOAD,
                    inferred_direction=InferredDirection.UNKNOWN_DIRECTION,
                    crc_valid=None,
                    dart_control_type=None,
                ),
            )
        ]
