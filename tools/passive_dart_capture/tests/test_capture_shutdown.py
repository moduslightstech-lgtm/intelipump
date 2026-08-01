"""Clean SIGINT / stop shutdown for passive capture."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from tools.passive_dart_capture.capture import PassiveCaptureSession
from tools.passive_dart_capture.serial_reader import (
    ByteSource,
    PassiveSerialConfig,
    SerialChunk,
)


class FakeSource:
    """ByteSource that yields scripted chunks then signals stop via empty reads."""

    def __init__(self, chunks: list[bytes], session: PassiveCaptureSession | None = None) -> None:
        self._chunks = list(chunks)
        self._idx = 0
        self._open = False
        self._config = PassiveSerialConfig(device="fake:pty")
        self.session = session
        self.closed = False

    @property
    def config(self) -> PassiveSerialConfig:
        return self._config

    @property
    def is_open(self) -> bool:
        return self._open

    def open(self) -> None:
        self._open = True

    def close(self) -> None:
        self._open = False
        self.closed = True

    def read_chunk(self) -> SerialChunk:
        if self._idx >= len(self._chunks):
            # Request stop after scripted data (simulates SIGINT between reads).
            if self.session is not None:
                self.session.request_stop()
            return SerialChunk(
                data=b"",
                capture_timestamp_utc=datetime.now(UTC),
                monotonic_timestamp_ns=self._idx,
                serial_device="fake:pty",
                baud=9600,
                parity="ODD",
                stop_bits=1,
            )
        data = self._chunks[self._idx]
        self._idx += 1
        return SerialChunk(
            data=data,
            capture_timestamp_utc=datetime.now(UTC),
            monotonic_timestamp_ns=self._idx * 1000,
            serial_device="fake:pty",
            baud=9600,
            parity="ODD",
            stop_bits=1,
        )


def test_clean_sigint_shutdown(tmp_path: Path) -> None:
    source = FakeSource([bytes.fromhex("50 20 FA"), bytes.fromhex("50")])
    session = PassiveCaptureSession(
        session_id="sigint-1",
        source=source,  # type: ignore[arg-type]
        evidence_dir=tmp_path,
    )
    source.session = session
    path = session.run(install_sigint=False)
    assert path.is_file()
    assert source.closed is True
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    markers = [r for r in rows if r.get("recordType") == "operator_marker"]
    assert any(m["marker"] == "STARTUP" for m in markers)
    assert any(m["marker"] == "END_CAPTURE" for m in markers)
    # Partial trailing bytes should be flushed as incomplete frame
    frames = [r for r in rows if r.get("recordType") == "frame"]
    assert any(f.get("frameClass") == "PARTIAL_FRAME" for f in frames)
    assert any(f.get("frameClass") == "POLL" for f in frames)


def test_fake_source_satisfies_protocol() -> None:
    # Structural check that FakeSource matches ByteSource usage.
    src: ByteSource = FakeSource([])  # type: ignore[assignment]
    src.open()
    chunk = src.read_chunk()
    assert chunk.data == b""
    src.close()
