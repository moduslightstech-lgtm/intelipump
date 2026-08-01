"""Passive capture session loop (LISTEN_ONLY — never transmits)."""

from __future__ import annotations

import contextlib
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tools.passive_dart_capture.dart_parser import parse_data_payload
from tools.passive_dart_capture.evidence_writer import (
    DEFAULT_EVIDENCE_DIR,
    EvidenceWriter,
    make_serial_chunk_record,
    session_evidence_path,
)
from tools.passive_dart_capture.frame_assembler import (
    AssembledFrame,
    PassiveFrameAssembler,
    frame_to_record,
)
from tools.passive_dart_capture.markers import OperatorMarker, make_marker_record
from tools.passive_dart_capture.serial_reader import (
    ByteSource,
    PassiveSerialConfig,
    PassiveSerialReader,
    SerialChunk,
)


def new_session_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]


@dataclass(slots=True)
class CaptureStats:
    chunks: int = 0
    bytes_total: int = 0
    frames: int = 0
    complete_frames: int = 0
    crc_invalid: int = 0


class PassiveCaptureSession:
    """Read serial chunks, assemble frames, write JSONL evidence."""

    def __init__(
        self,
        *,
        session_id: str | None = None,
        source: ByteSource | None = None,
        serial_config: PassiveSerialConfig | None = None,
        evidence_dir: Path | None = None,
    ) -> None:
        self.session_id = session_id or new_session_id()
        self._source: ByteSource = source or PassiveSerialReader(serial_config)
        self._evidence_dir = evidence_dir or DEFAULT_EVIDENCE_DIR
        self._path = session_evidence_path(self.session_id, self._evidence_dir)
        self._writer: EvidenceWriter | None = None
        self._assembler = PassiveFrameAssembler()
        self._chunk_sequence = 0
        self._stop = False
        self.stats = CaptureStats()

    @property
    def evidence_path(self) -> Path:
        return self._path

    def request_stop(self, *_args: object) -> None:
        self._stop = True

    def run(self, *, install_sigint: bool = True) -> Path:
        prev_handler = None
        if install_sigint:
            prev_handler = signal.signal(signal.SIGINT, self.request_stop)

        self._writer = EvidenceWriter(self._path)
        try:
            self._write(
                make_marker_record(
                    session_id=self.session_id,
                    marker=OperatorMarker.STARTUP,
                    note="passive capture session started",
                )
            )
            self._source.open()
            while not self._stop:
                chunk = self._source.read_chunk()
                self._handle_chunk(chunk)
            self._shutdown(reason="SIGINT_OR_STOP")
        except KeyboardInterrupt:
            self._stop = True
            self._shutdown(reason="KeyboardInterrupt")
        finally:
            with contextlib.suppress(Exception):
                self._source.close()
            if self._writer is not None:
                self._writer.close()
                self._writer = None
            if install_sigint and prev_handler is not None:
                signal.signal(signal.SIGINT, prev_handler)
        return self._path

    def process_chunk_for_tests(self, chunk: SerialChunk) -> list[dict[str, Any]]:
        """Process one chunk without opening serial (unit tests)."""
        if self._writer is None:
            self._writer = EvidenceWriter(self._path)
        return self._handle_chunk(chunk)

    def flush_and_stop_for_tests(self) -> None:
        if self._writer is None:
            self._writer = EvidenceWriter(self._path)
        self._shutdown(reason="test_stop")
        self._writer.close()
        self._writer = None

    def _handle_chunk(self, chunk: SerialChunk) -> list[dict[str, Any]]:
        written: list[dict[str, Any]] = []
        if not chunk.data:
            return written

        self.stats.chunks += 1
        self.stats.bytes_total += len(chunk.data)
        seq = self._chunk_sequence
        self._chunk_sequence += 1
        record = make_serial_chunk_record(
            session_id=self.session_id,
            chunk_sequence=seq,
            data=chunk.data,
            capture_timestamp_utc=chunk.capture_timestamp_utc,
            monotonic_timestamp_ns=chunk.monotonic_timestamp_ns,
            serial_device=chunk.serial_device,
            baud=chunk.baud,
            parity=chunk.parity,
            stop_bits=chunk.stop_bits,
        )
        self._write(record)
        written.append(record)

        frames = self._assembler.feed_chunk(chunk)
        for assembled in frames:
            frame_rec = self._emit_frame(assembled)
            written.append(frame_rec)
        return written

    def _emit_frame(self, assembled: AssembledFrame) -> dict[str, Any]:
        self.stats.frames += 1
        if assembled.complete:
            self.stats.complete_frames += 1
        if assembled.crc_valid is False:
            self.stats.crc_invalid += 1

        transactions: list[dict[str, Any]] = []
        if (
            assembled.complete
            and assembled.payload
            and assembled.frame_class in {"DATA", "DATA_CRC_INVALID"}
        ):
            addr = (
                int(assembled.address_hex, 16) if assembled.address_hex is not None else None
            )
            seq_nibble = None
            if assembled.dart_frame is not None:
                seq_nibble = assembled.dart_frame.sequence
            transactions = parse_data_payload(
                assembled.payload,
                pump_address=addr,
                line_sequence=seq_nibble,
                source_frame_raw_hex=assembled.raw.hex(" ").upper(),
            )

        frame_seq = self._assembler.take_frame_sequence()
        record = frame_to_record(
            session_id=self.session_id,
            frame_sequence=frame_seq,
            assembled=assembled,
            transactions=transactions,
        )
        self._write(record)
        return record

    def _shutdown(self, *, reason: str) -> None:
        now = datetime.now(UTC)
        mono = time.monotonic_ns()
        partial = self._assembler.flush_partial(session_now=now, mono_ns=mono)
        if partial is not None:
            self._emit_frame(partial)
        self._write(
            make_marker_record(
                session_id=self.session_id,
                marker=OperatorMarker.END_CAPTURE,
                note=f"clean shutdown: {reason}",
                timestamp_utc=now,
                monotonic_ns=mono,
            )
        )

    def _write(self, record: dict[str, Any]) -> None:
        if self._writer is None:
            raise RuntimeError("evidence writer not open")
        self._writer.write_record(record)


def run_capture(
    *,
    device: str = "/dev/ttyUSB0",
    baud: int = 9600,
    session_id: str | None = None,
    evidence_dir: Path | None = None,
    source: ByteSource | None = None,
) -> Path:
    """Start a passive capture session until SIGINT."""
    config = PassiveSerialConfig(device=device, baud=baud)
    session = PassiveCaptureSession(
        session_id=session_id,
        source=source,
        serial_config=config,
        evidence_dir=evidence_dir,
    )
    return session.run()
