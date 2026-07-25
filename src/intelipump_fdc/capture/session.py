"""Passive capture session: RX-only loop with JSONL output."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from intelipump_fdc.capture.receive_only import ReceiveOnlySource
from intelipump_fdc.capture.schema import (
    CaptureEvent,
    SerialCaptureState,
    make_event_record,
    make_rx_record,
    new_capture_id,
)

logger = logging.getLogger(__name__)

PASSIVE_BANNER = "PASSIVE_CAPTURE_ONLY"


@dataclass(frozen=True, slots=True)
class PassiveCaptureConfig:
    port: str
    baud: int
    output: Path
    duration_s: float
    read_size: int = 256
    idle_gap_ms: float = 50.0
    reconnect_delay_s: float = 0.5
    capture_id: str | None = None
    format: str = "jsonl"


class PassiveCaptureSession:
    """Receive-only capture. Never calls write on the source."""

    def __init__(
        self,
        source: ReceiveOnlySource,
        config: PassiveCaptureConfig,
        *,
        output_fp: IO[str] | None = None,
    ) -> None:
        if config.format != "jsonl":
            raise ValueError("only --format jsonl is supported")
        if config.duration_s <= 0:
            raise ValueError("duration must be > 0")
        self.source = source
        self.config = config
        self.capture_id = config.capture_id or new_capture_id()
        self._stop = asyncio.Event()
        self._fp = output_fp
        self._owns_fp = output_fp is None
        self.chunk_sequence = 0
        self.total_bytes = 0
        self.write_attempts = 0  # must remain 0; diagnostic for tests
        self._last_rx_mono: float | None = None
        self._serial_state = SerialCaptureState.CLOSED
        self.records_written = 0

    def request_stop(self) -> None:
        self._stop.set()

    def _mono_ns(self) -> int:
        return time.monotonic_ns()

    def _emit(self, line: str) -> None:
        assert self._fp is not None
        self._fp.write(line + "\n")
        self._fp.flush()
        self.records_written += 1

    def _emit_event(
        self,
        event: CaptureEvent,
        *,
        notes: str | None = None,
        state: SerialCaptureState | None = None,
    ) -> None:
        st = state or self._serial_state
        rec = make_event_record(
            capture_id=self.capture_id,
            port=self.config.port,
            baud=self.config.baud,
            event=event,
            monotonic_ns=self._mono_ns(),
            serial_state=st,
            notes=notes,
        )
        self._emit(rec.to_json_line())

    def _emit_rx(self, raw: bytes, *, idle_gap_ms: float | None) -> None:
        self.chunk_sequence += 1
        self.total_bytes += len(raw)
        rec = make_rx_record(
            capture_id=self.capture_id,
            port=self.config.port,
            baud=self.config.baud,
            raw=raw,
            monotonic_ns=self._mono_ns(),
            chunk_sequence=self.chunk_sequence,
            idle_gap_ms=idle_gap_ms,
            serial_state=self._serial_state,
        )
        assert rec.direction == "RX"
        self._emit(rec.to_json_line())

    async def run(self) -> dict[str, object]:
        logger.info(
            "%s port=%s baud=%s output=%s duration_s=%s",
            PASSIVE_BANNER,
            self.config.port,
            self.config.baud,
            self.config.output,
            self.config.duration_s,
        )
        # Exclusive open must succeed before any capture file / capture_started.
        if not self.source.is_open:
            await self.source.open()
        self._serial_state = SerialCaptureState.OPEN

        if self._fp is None:
            self.config.output.parent.mkdir(parents=True, exist_ok=True)
            self._fp = self.config.output.open("w", encoding="utf-8")

        deadline = time.monotonic() + self.config.duration_s
        try:
            self._emit_event(
                CaptureEvent.CAPTURE_STARTED,
                notes=PASSIVE_BANNER,
                state=SerialCaptureState.OPEN,
            )
            while not self._stop.is_set() and time.monotonic() < deadline:
                if not self.source.is_open:
                    await self._reconnect_once()
                    continue
                try:
                    chunk = await self.source.read(self.config.read_size)
                except Exception as exc:
                    self._serial_state = SerialCaptureState.DISCONNECTED
                    self._emit_event(
                        CaptureEvent.SERIAL_DISCONNECTED,
                        notes=f"{type(exc).__name__}:{exc}",
                    )
                    await self._safe_close()
                    await self._reconnect_once()
                    continue
                if not chunk:
                    await asyncio.sleep(0.01)
                    continue
                now = time.monotonic()
                idle_gap_ms: float | None = None
                if self._last_rx_mono is not None:
                    gap_ms = (now - self._last_rx_mono) * 1000.0
                    if gap_ms >= self.config.idle_gap_ms:
                        idle_gap_ms = gap_ms
                self._last_rx_mono = now
                self._emit_rx(chunk, idle_gap_ms=idle_gap_ms)
        finally:
            await self._safe_close()
            self._serial_state = SerialCaptureState.CLOSED
            self._emit_event(
                CaptureEvent.CAPTURE_STOPPED,
                notes=f"total_bytes={self.total_bytes}",
            )
            if self._owns_fp and self._fp is not None:
                self._fp.close()
                self._fp = None

        return {
            "captureId": self.capture_id,
            "totalBytes": self.total_bytes,
            "chunkSequence": self.chunk_sequence,
            "recordsWritten": self.records_written,
            "writeAttempts": self.write_attempts,
            "output": str(self.config.output),
            "mode": PASSIVE_BANNER,
        }

    async def _safe_close(self) -> None:
        try:
            if self.source.is_open:
                await self.source.close()
        except Exception as exc:
            self._emit_event(
                CaptureEvent.ERROR,
                notes=f"close_failed:{type(exc).__name__}:{exc}",
            )

    async def _reconnect_once(self) -> None:
        if self._stop.is_set():
            return
        self._serial_state = SerialCaptureState.RECONNECTING
        await asyncio.sleep(self.config.reconnect_delay_s)
        try:
            await self.source.open()
            self._serial_state = SerialCaptureState.OPEN
            self._emit_event(CaptureEvent.SERIAL_RECONNECTED)
        except Exception as exc:
            self._serial_state = SerialCaptureState.DISCONNECTED
            self._emit_event(
                CaptureEvent.ERROR,
                notes=f"reconnect_failed:{type(exc).__name__}:{exc}",
            )
