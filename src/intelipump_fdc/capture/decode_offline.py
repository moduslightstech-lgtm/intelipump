"""Offline decode of passive capture JSONL (never opens a serial port)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.line.addressing import (
    AddressMappingError,
    decode_wire_address,
)
from intelipump_fdc.protocol.dart.line.captured_classify import CapturedFrameClass
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)


@dataclass
class CandidateFrame:
    offset: int
    byte_count: int
    raw_hex: str
    control_type: str | None
    address: int | None
    sequence: int | None
    crc_valid: bool | None
    certainty: str
    notes: list[str] = field(default_factory=list)
    application: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "offset": self.offset,
            "byteCount": self.byte_count,
            "rawHex": self.raw_hex,
            "controlType": self.control_type,
            "address": self.address,
            "sequence": self.sequence,
            "crcValid": self.crc_valid,
            "certainty": self.certainty,
            "notes": list(self.notes),
            "application": self.application,
        }


@dataclass
class OfflineDecodeResult:
    capture_id: str | None
    total_rx_bytes: int
    undecoded_trailing_hex: str
    undecoded_noise_hex: list[str]
    candidate_frames: list[CandidateFrame]
    crc_valid_count: int
    crc_invalid_count: int
    possible_addresses: list[int]
    control_frame_count: int
    data_frame_count: int
    rejected_count: int
    warnings: list[str]
    serial_events: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "captureId": self.capture_id,
            "totalRxBytes": self.total_rx_bytes,
            "undecodedTrailingHex": self.undecoded_trailing_hex,
            "undecodedNoiseHex": list(self.undecoded_noise_hex),
            "candidateFrames": [f.to_dict() for f in self.candidate_frames],
            "crcValidCount": self.crc_valid_count,
            "crcInvalidCount": self.crc_invalid_count,
            "possibleAddresses": list(self.possible_addresses),
            "controlFrameCount": self.control_frame_count,
            "dataFrameCount": self.data_frame_count,
            "rejectedCount": self.rejected_count,
            "warnings": list(self.warnings),
            "serialEvents": list(self.serial_events),
            "interpretationNote": (
                "All frame/address/application fields are candidate "
                "interpretations unless certainty=confirmed_by_structure. "
                "Never treat guessed structure as confirmed field truth."
            ),
        }


def _raw_hex_to_bytes(raw_hex: str) -> bytes:
    parts = raw_hex.replace(",", " ").split()
    if not parts:
        return b""
    return bytes(int(p, 16) for p in parts)


def load_capture_rx_bytes(path: Path) -> tuple[str | None, bytes, list[dict[str, Any]]]:
    """Load RX chunks in file order; never opens a serial port."""
    capture_id: str | None = None
    buf = bytearray()
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fp:
        for line_no, line in enumerate(fp, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at line {line_no}: {exc}") from exc
            if capture_id is None:
                capture_id = obj.get("captureId")
            rtype = obj.get("recordType")
            if rtype == "event":
                events.append(obj)
                continue
            if rtype != "rx_chunk":
                continue
            if obj.get("direction") != "RX":
                raise ValueError(
                    f"non-RX direction in capture at line {line_no}: {obj.get('direction')}"
                )
            raw_hex = obj.get("rawHex") or ""
            buf.extend(_raw_hex_to_bytes(raw_hex))
    return capture_id, bytes(buf), events


def decode_capture_file(path: Path) -> OfflineDecodeResult:
    """Run existing framing/CRC decoder over captured bytes (offline only)."""
    capture_id, blob, serial_events = load_capture_rx_bytes(path)
    assembler = LegacyIgemStreamAssembler()
    candidates: list[CandidateFrame] = []
    noise: list[str] = []
    warnings: list[str] = [
        "Offline decode only; no serial I/O performed.",
        "Addresses and application fields are candidate interpretations.",
    ]
    offset = 0
    crc_valid = 0
    crc_invalid = 0
    control_n = 0
    data_n = 0
    rejected = 0
    addresses: set[int] = set()

    for event in assembler.feed(blob):
        raw = event.raw
        if event.kind is AssemblerEventKind.NOISE:
            noise.append(raw.hex(" "))
            offset += len(raw)
            continue
        if event.kind is AssemblerEventKind.OVERFLOW:
            warnings.append(f"assembler overflow at offset {offset}: {event.message}")
            noise.append(raw.hex(" "))
            offset += len(raw)
            continue
        if event.kind is AssemblerEventKind.REJECTED:
            rejected += 1
            candidates.append(
                CandidateFrame(
                    offset=offset,
                    byte_count=len(raw),
                    raw_hex=raw.hex(" "),
                    control_type=None,
                    address=None,
                    sequence=None,
                    crc_valid=None,
                    certainty="uncertain",
                    notes=[
                        "rejected_by_parser",
                        event.message or "parse rejected",
                        "Do not treat as a confirmed DART frame.",
                    ],
                )
            )
            offset += len(raw)
            continue

        frame = event.frame
        assert frame is not None
        notes = ["candidate_frame_from_stream_assembler"]
        certainty = "confirmed_by_structure"
        if frame.unknown_control:
            certainty = "uncertain"
            notes.append("unknown_control_byte")
        app: dict[str, Any] | None = None
        if frame.control_type is ControlType.DATA:
            data_n += 1
            if frame.crc_valid is True:
                crc_valid += 1
            elif frame.crc_valid is False:
                crc_invalid += 1
                certainty = "uncertain"
                notes.append("crc_invalid")
            else:
                notes.append("crc_not_applicable_or_unknown")
            if frame.crc_valid is True:
                try:
                    bundle = decode_data_payload(
                        frame.payload,
                        pump_address=frame.address,
                        line_sequence=frame.sequence,
                        source_frame_raw_hex=frame.raw_frame.hex(" "),
                    )
                    app = {
                        "certainty": "uncertain_application_decode",
                        "trailingBytesHex": bundle.trailing_bytes.hex(" "),
                        "warnings": list(bundle.warnings),
                        "transactions": [
                            {
                                "type": t.transaction_type.value,
                                "decodeStatus": t.decode_status.value,
                                "decodedBody": t.decoded_body,
                                "warnings": list(t.warnings),
                            }
                            for t in bundle.transactions
                        ],
                    }
                    notes.append(
                        "application decode is candidate only; decimals/direction may be unknown"
                    )
                except Exception as exc:
                    notes.append(f"application_decode_error:{type(exc).__name__}:{exc}")
                    certainty = "uncertain"
        else:
            control_n += 1

        try:
            logical = decode_wire_address(frame.address)
        except AddressMappingError:
            logical = None
        if logical is not None:
            addresses.add(logical)
            notes.append(
                f"logicalAddress={logical}; wireAddress=0x{frame.address:02X}"
            )
        else:
            addresses.add(frame.address)
            notes.append(f"wireAddress=0x{frame.address:02X}")

        if event.captured is not None:
            notes.append(f"capturedClass={event.captured.classification.value}")
            if (
                event.captured.classification
                is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK
            ):
                notes.append(
                    f"sequenceNibble={event.captured.sequence_nibble}; "
                    "possibleAcknowledgement=true; not a nozzle-lift event"
                )
            notes.append(
                f"payloadSemantics={event.captured.payload_semantics.value}"
            )

        candidates.append(
            CandidateFrame(
                offset=offset,
                byte_count=len(raw),
                raw_hex=raw.hex(" "),
                control_type=(
                    event.captured.classification.value
                    if event.captured is not None
                    else frame.control_type.value
                ),
                address=logical if logical is not None else frame.address,
                sequence=frame.sequence,
                crc_valid=frame.crc_valid,
                certainty=certainty,
                notes=notes,
                application=app,
            )
        )
        offset += len(raw)

    trailing = assembler.reset()
    return OfflineDecodeResult(
        capture_id=capture_id if isinstance(capture_id, str) else None,
        total_rx_bytes=len(blob),
        undecoded_trailing_hex=trailing.hex(" "),
        undecoded_noise_hex=noise,
        candidate_frames=candidates,
        crc_valid_count=crc_valid,
        crc_invalid_count=crc_invalid,
        possible_addresses=sorted(addresses),
        control_frame_count=control_n,
        data_frame_count=data_n,
        rejected_count=rejected,
        warnings=warnings,
        serial_events=[
            {
                "event": e.get("event"),
                "timestampUtc": e.get("timestampUtc"),
                "notes": e.get("notes"),
                "serialState": e.get("serialState"),
            }
            for e in serial_events
        ],
    )
