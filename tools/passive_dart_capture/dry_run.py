"""Hardware-free dry-run of capture → evidence → offline analysis.

Reads a hex fixture file (not a serial device). Never opens ``/dev/tty*``
or any pyserial port.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from tools.passive_dart_capture.analyzer import AnalysisResult, analyze_path
from tools.passive_dart_capture.capture import PassiveCaptureSession
from tools.passive_dart_capture.serial_reader import PassiveSerialConfig, SerialChunk

PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_FIXTURE = PACKAGE_ROOT / "tests" / "fixtures" / "merged_bus_sample.hex"


def parse_hex_fixture(text: str) -> list[bytes]:
    """Parse a hex fixture into chunk payloads (one chunk per non-empty data line).

    Lines starting with ``#`` are comments. Empty lines are skipped.
    Each remaining line is whitespace-separated hex bytes → one OS-read chunk.
    """
    chunks: list[bytes] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        # Allow inline comments after hex: "50 20 FA  # poll"
        if "#" in line:
            line = line.split("#", 1)[0].strip()
        if not line:
            continue
        chunks.append(bytes.fromhex(line))
    if not chunks:
        raise ValueError("fixture contains no hex data lines")
    return chunks


def load_fixture_chunks(path: Path) -> list[bytes]:
    return parse_hex_fixture(path.read_text(encoding="utf-8"))


@dataclass(slots=True)
class FixtureByteSource:
    """ByteSource backed by scripted fixture chunks — no serial I/O."""

    chunks: list[bytes]
    device_label: str = "fixture:dry-run"
    _idx: int = 0
    _open: bool = False
    _stop_requested: Callable[[], None] | None = None
    _config: PassiveSerialConfig = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._config = PassiveSerialConfig(device=self.device_label)

    @property
    def config(self) -> PassiveSerialConfig:
        return self._config

    @property
    def is_open(self) -> bool:
        return self._open

    def set_stop_callback(self, callback: Callable[[], None]) -> None:
        self._stop_requested = callback

    def open(self) -> None:
        # Intentionally does not touch any serial device path.
        if self.device_label.startswith("/dev/"):
            raise RuntimeError(
                "FixtureByteSource refused device_label that looks like a "
                f"serial path: {self.device_label!r}"
            )
        self._open = True
        self._idx = 0

    def close(self) -> None:
        self._open = False

    def read_chunk(self) -> SerialChunk:
        if not self._open:
            raise RuntimeError("fixture source is not open")
        if self._idx >= len(self.chunks):
            if self._stop_requested is not None:
                self._stop_requested()
            return SerialChunk(
                data=b"",
                capture_timestamp_utc=datetime.now(UTC),
                monotonic_timestamp_ns=self._idx,
                serial_device=self.device_label,
                baud=9600,
                parity="ODD",
                stop_bits=1,
            )
        data = self.chunks[self._idx]
        self._idx += 1
        # Mirror production: stamp immediately after the "read" returns.
        mono_ns = self._idx * 1_000_000
        utc = datetime.now(UTC)
        return SerialChunk(
            data=data,
            capture_timestamp_utc=utc,
            monotonic_timestamp_ns=mono_ns,
            serial_device=self.device_label,
            baud=9600,
            parity="ODD",
            stop_bits=1,
        )


@dataclass(slots=True)
class DryRunResult:
    session_id: str
    evidence_path: Path
    reports_dir: Path
    analysis: AnalysisResult
    work_dir: Path
    stats_frames: int
    stats_chunks: int


def run_dry_run(
    *,
    fixture_path: Path | None = None,
    work_dir: Path | None = None,
    session_id: str = "dry-run",
) -> DryRunResult:
    """Run full capture→analysis pipeline from a hex fixture (no hardware).

    When ``work_dir`` is None, creates a directory under the system temp area
    via ``tempfile.mkdtemp`` (caller may delete ``result.work_dir`` when done).
    """
    fixture = fixture_path or DEFAULT_FIXTURE
    if not fixture.is_file():
        raise FileNotFoundError(f"dry-run fixture not found: {fixture}")

    if work_dir is None:
        root = Path(tempfile.mkdtemp(prefix="passive_dart_dry_run_"))
    else:
        root = work_dir
        root.mkdir(parents=True, exist_ok=True)

    evidence_dir = root / "evidence"
    reports_dir = root / "reports"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    chunks = load_fixture_chunks(fixture)
    source = FixtureByteSource(chunks=chunks)
    session = PassiveCaptureSession(
        session_id=session_id,
        source=source,  # type: ignore[arg-type]
        evidence_dir=evidence_dir,
    )
    source.set_stop_callback(session.request_stop)
    evidence_path = session.run(install_sigint=False)

    analysis = analyze_path(
        evidence_path,
        reports_dir=reports_dir,
        session_id=session_id,
    )
    return DryRunResult(
        session_id=session_id,
        evidence_path=evidence_path,
        reports_dir=reports_dir,
        analysis=analysis,
        work_dir=root,
        stats_frames=session.stats.frames,
        stats_chunks=session.stats.chunks,
    )
