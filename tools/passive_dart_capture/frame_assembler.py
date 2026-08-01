"""Streaming DART frame assembly for passive capture (read-only wrappers)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from intelipump_fdc.protocol.dart.line.captured_classify import (
    CapturedFrameClass,
    InferredDirection,
    classify_captured_control,
    infer_direction,
)
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.protocol.dart.line.stream import (
    AssemblerEvent,
    AssemblerEventKind,
    FrameStreamAssembler,
)
from tools.passive_dart_capture.serial_reader import SerialChunk


@dataclass(slots=True)
class AssembledFrame:
    """One assembled (or rejected) frame candidate with timestamps."""

    raw: bytes
    first_byte_timestamp_utc: datetime
    last_byte_timestamp_utc: datetime
    first_byte_monotonic_ns: int
    last_byte_monotonic_ns: int
    complete: bool
    frame_class: str
    address_hex: str | None
    control_hex: str | None
    crc_valid: bool | None
    crc_expected: str | None
    crc_observed: str | None
    parse_warnings: list[str] = field(default_factory=list)
    direction: str = "UNKNOWN"
    direction_confidence: str = "NONE"
    direction_inference_reason: str = ""
    dart_frame: DartLineFrame | None = None
    payload: bytes = b""


@dataclass(slots=True)
class _PendingStamp:
    first_utc: datetime
    first_mono: int
    last_utc: datetime
    last_mono: int


class PassiveFrameAssembler:
    """Wraps FrameStreamAssembler and stamps frames with chunk timestamps."""

    def __init__(self) -> None:
        self._asm = FrameStreamAssembler()
        self._pending: _PendingStamp | None = None
        self._frame_sequence = 0

    @property
    def pending_size(self) -> int:
        return self._asm.pending_size

    @property
    def next_frame_sequence(self) -> int:
        return self._frame_sequence

    def take_frame_sequence(self) -> int:
        seq = self._frame_sequence
        self._frame_sequence = seq + 1
        return seq

    def reset(self) -> bytes:
        self._pending = None
        return self._asm.reset()

    def feed_chunk(self, chunk: SerialChunk) -> list[AssembledFrame]:
        """Feed one OS read; return zero or more assembled frame records."""
        if not chunk.data:
            return []

        if self._pending is None:
            self._pending = _PendingStamp(
                first_utc=chunk.capture_timestamp_utc,
                first_mono=chunk.monotonic_timestamp_ns,
                last_utc=chunk.capture_timestamp_utc,
                last_mono=chunk.monotonic_timestamp_ns,
            )
        else:
            self._pending.last_utc = chunk.capture_timestamp_utc
            self._pending.last_mono = chunk.monotonic_timestamp_ns

        # Stamp for all frames completed by this chunk (merged-bus OS read).
        chunk_stamp = self._pending
        events = self._asm.feed(chunk.data)
        out: list[AssembledFrame] = [
            self._event_to_frame(event, stamp=chunk_stamp) for event in events
        ]
        if self._asm.pending_size == 0:
            self._pending = None
        else:
            # Remnant continues; attribute subsequent completion to this chunk
            # start until more bytes arrive.
            self._pending = _PendingStamp(
                first_utc=chunk.capture_timestamp_utc,
                first_mono=chunk.monotonic_timestamp_ns,
                last_utc=chunk.capture_timestamp_utc,
                last_mono=chunk.monotonic_timestamp_ns,
            )
        return out

    def flush_partial(self, *, session_now: datetime, mono_ns: int) -> AssembledFrame | None:
        """Emit a PARTIAL_FRAME for any leftover buffer (e.g. on shutdown).

        Prefer timestamps from the chunk that introduced the remnant; fall back
        to ``session_now`` / ``mono_ns`` only when no pending stamp exists.
        """
        stamp = self._pending
        discarded = self.reset()
        if not discarded:
            return None
        first_utc = stamp.first_utc if stamp is not None else session_now
        last_utc = stamp.last_utc if stamp is not None else session_now
        first_mono = stamp.first_mono if stamp is not None else mono_ns
        last_mono = stamp.last_mono if stamp is not None else mono_ns
        return AssembledFrame(
            raw=discarded,
            first_byte_timestamp_utc=first_utc,
            last_byte_timestamp_utc=last_utc,
            first_byte_monotonic_ns=first_mono,
            last_byte_monotonic_ns=last_mono,
            complete=False,
            frame_class=CapturedFrameClass.PARTIAL_FRAME.value,
            address_hex=f"{discarded[0]:02X}" if discarded else None,
            control_hex=f"{discarded[1]:02X}" if len(discarded) > 1 else None,
            crc_valid=None,
            crc_expected=None,
            crc_observed=None,
            parse_warnings=["partial buffer at shutdown/flush"],
            direction="UNKNOWN",
            direction_confidence="NONE",
            direction_inference_reason="incomplete frame; no direction inference",
        )

    def _event_to_frame(self, event: AssemblerEvent, *, stamp: _PendingStamp) -> AssembledFrame:
        warnings: list[str] = []
        if event.message:
            warnings.append(event.message)

        if event.kind is AssemblerEventKind.FRAME and event.frame is not None:
            return self._from_dart_frame(event.frame, stamp=stamp, warnings=warnings)

        if event.kind is AssemblerEventKind.REJECTED:
            err = event.error
            if err is not None:
                warnings.append(f"{err.code.value}: {err.message}")
            addr = f"{event.raw[0]:02X}" if event.raw else None
            ctrl = f"{event.raw[1]:02X}" if len(event.raw) > 1 else None
            frame_class = CapturedFrameClass.UNKNOWN_FRAME.value
            if ctrl is not None:
                frame_class = classify_captured_control(int(ctrl, 16)).value
            return AssembledFrame(
                raw=event.raw,
                first_byte_timestamp_utc=stamp.first_utc,
                last_byte_timestamp_utc=stamp.last_utc,
                first_byte_monotonic_ns=stamp.first_mono,
                last_byte_monotonic_ns=stamp.last_mono,
                complete=False,
                frame_class=frame_class,
                address_hex=addr,
                control_hex=ctrl,
                crc_valid=False if err and "CRC" in err.message.upper() else None,
                crc_expected=None,
                crc_observed=None,
                parse_warnings=warnings,
                direction="UNKNOWN",
                direction_confidence="NONE",
                direction_inference_reason="rejected/malformed frame",
            )

        # NOISE / OVERFLOW
        warnings.append(event.kind.value)
        return AssembledFrame(
            raw=event.raw,
            first_byte_timestamp_utc=stamp.first_utc,
            last_byte_timestamp_utc=stamp.last_utc,
            first_byte_monotonic_ns=stamp.first_mono,
            last_byte_monotonic_ns=stamp.last_mono,
            complete=False,
            frame_class=CapturedFrameClass.UNKNOWN_FRAME.value,
            address_hex=f"{event.raw[0]:02X}" if event.raw else None,
            control_hex=f"{event.raw[1]:02X}" if len(event.raw) > 1 else None,
            crc_valid=None,
            crc_expected=None,
            crc_observed=None,
            parse_warnings=warnings,
            direction="UNKNOWN",
            direction_confidence="NONE",
            direction_inference_reason=event.kind.value.lower(),
        )

    def _from_dart_frame(
        self,
        frame: DartLineFrame,
        *,
        stamp: _PendingStamp,
        warnings: list[str],
    ) -> AssembledFrame:
        classification = classify_captured_control(frame.control)
        # Short control ACK family (C0-CF) and EOT (70) called out for evidence.
        if classification is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK:
            frame_class = "SHORT_ACK"
        elif classification is CapturedFrameClass.POLL:
            frame_class = "POLL"
        elif classification is CapturedFrameClass.SHORT_CONTROL_70:
            frame_class = "SHORT_CONTROL_EOT"
        elif classification is CapturedFrameClass.DATA_FRAME:
            frame_class = "DATA"
        else:
            frame_class = classification.value

        inferred = infer_direction(classification)
        if inferred is InferredDirection.UNKNOWN_DIRECTION:
            direction = "UNKNOWN"
            confidence = "NONE"
            reason = "no shape-based direction inference"
        else:
            direction = "INFERRED"
            confidence = "SHAPE_ONLY"
            reason = f"{inferred.value}; shape-based only, not measured"

        if frame.unknown_control:
            warnings.append(f"unknown control byte 0x{frame.control:02X}")

        crc_expected = (
            f"{frame.computed_crc:04X}" if frame.computed_crc is not None else None
        )
        crc_observed = (
            f"{frame.received_crc:04X}" if frame.received_crc is not None else None
        )
        complete = True
        if frame.crc_valid is False:
            warnings.append("CRC invalid")
            # Still a complete wire frame structurally.
            frame_class = "DATA_CRC_INVALID" if frame_class == "DATA" else frame_class

        return AssembledFrame(
            raw=frame.raw_frame,
            first_byte_timestamp_utc=stamp.first_utc,
            last_byte_timestamp_utc=stamp.last_utc,
            first_byte_monotonic_ns=stamp.first_mono,
            last_byte_monotonic_ns=stamp.last_mono,
            complete=complete,
            frame_class=frame_class,
            address_hex=f"{frame.address:02X}",
            control_hex=f"{frame.control:02X}",
            crc_valid=frame.crc_valid,
            crc_expected=crc_expected,
            crc_observed=crc_observed,
            parse_warnings=warnings,
            direction=direction,
            direction_confidence=confidence,
            direction_inference_reason=reason,
            dart_frame=frame,
            payload=frame.payload,
        )


def frame_to_record(
    *,
    session_id: str,
    frame_sequence: int,
    assembled: AssembledFrame,
    transactions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "recordType": "frame",
        "sessionId": session_id,
        "frameSequence": frame_sequence,
        "firstByteTimestampUtc": assembled.first_byte_timestamp_utc.isoformat(),
        "lastByteTimestampUtc": assembled.last_byte_timestamp_utc.isoformat(),
        "firstByteMonotonicNs": assembled.first_byte_monotonic_ns,
        "lastByteMonotonicNs": assembled.last_byte_monotonic_ns,
        "rawHex": assembled.raw.hex(" ").upper(),
        "addressHex": assembled.address_hex,
        "controlHex": assembled.control_hex,
        "frameClass": assembled.frame_class,
        "complete": assembled.complete,
        "crcValid": assembled.crc_valid,
        "crcExpected": assembled.crc_expected,
        "crcObserved": assembled.crc_observed,
        "parseWarnings": list(assembled.parse_warnings),
        "direction": assembled.direction,
        "directionConfidence": assembled.direction_confidence,
        "directionInferenceReason": assembled.direction_inference_reason,
        "source": "EPUMP_PASSIVE_CAPTURE",
        "transactions": transactions or [],
    }


# Silence unused ParseError import concern — re-export for tests.
__all__ = [
    "AssembledFrame",
    "PassiveFrameAssembler",
    "frame_to_record",
]
