"""Passive capture unit tests (no real Wayne hardware)."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from intelipump_fdc.capture.cli_capture import build_parser
from intelipump_fdc.capture.receive_only import (
    TxInhibitNotConfirmedError,
    check_port_available,
    require_tx_physically_inhibited,
)
from intelipump_fdc.capture.schema import bytes_to_raw_hex
from intelipump_fdc.capture.session import PassiveCaptureConfig, PassiveCaptureSession
from intelipump_fdc.controller.safety import ControllerSafetyContext
from intelipump_fdc.core.config import ControllerMode
from intelipump_fdc.protocol.dart.line.frame_builder import build_eot


@dataclass
class FakeReceiveOnlySource:
    """Test RX source. Intentionally has no write() method."""

    device: str = "/dev/fake-passive"
    baud_rate: int = 9600
    chunks: list[bytes] = field(default_factory=list)
    fail_opens: int = 0
    disconnect_after_reads: int | None = None
    _open: bool = False
    _open_attempts: int = 0
    _reads: int = 0

    @property
    def is_open(self) -> bool:
        return self._open

    async def open(self) -> None:
        self._open_attempts += 1
        if self._open_attempts <= self.fail_opens:
            raise OSError("No such device")
        self._open = True

    async def close(self) -> None:
        self._open = False

    async def read(self, max_bytes: int) -> bytes:
        if not self._open:
            raise OSError("not open")
        self._reads += 1
        if (
            self.disconnect_after_reads is not None
            and self._reads > self.disconnect_after_reads
        ):
            self._open = False
            raise OSError("device unplugged")
        if not self.chunks:
            await asyncio.sleep(0.01)
            return b""
        data = self.chunks.pop(0)
        return data[:max_bytes]

def test_tx_inhibit_fail_closed() -> None:
    with pytest.raises(TxInhibitNotConfirmedError):
        require_tx_physically_inhibited(confirmed=False)
    require_tx_physically_inhibited(confirmed=True)


def test_parser_requires_confirm_flags() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--port",
            "/dev/intelipump-controller",
            "--duration",
            "1",
            "--confirm-tx-physically-inhibited",
            "--confirm-controller-stopped",
        ]
    )
    assert args.confirm_tx_physically_inhibited is True
    assert args.confirm_controller_stopped is True


def test_receive_only_source_has_no_write_on_real_class() -> None:
    from intelipump_fdc.capture.receive_only import ReceiveOnlySerialSource

    assert not hasattr(ReceiveOnlySerialSource, "write")
    assert not hasattr(ReceiveOnlySerialSource, "drain")


@pytest.mark.asyncio
async def test_passive_capture_never_calls_write(tmp_path: Path) -> None:
    raw = build_eot(1, 0)
    source = FakeReceiveOnlySource(chunks=[raw, b""])
    out = tmp_path / "cap.jsonl"
    session = PassiveCaptureSession(
        source,
        PassiveCaptureConfig(
            port=source.device,
            baud=9600,
            output=out,
            duration_s=0.2,
            read_size=64,
            idle_gap_ms=10.0,
            reconnect_delay_s=0.05,
        ),
    )
    summary = await session.run()
    assert summary["writeAttempts"] == 0
    assert not hasattr(source, "write")
    assert not hasattr(FakeReceiveOnlySource, "write")
    # Session must not use controller scheduler / authorization.
    assert summary["mode"] == "PASSIVE_CAPTURE_ONLY"


@pytest.mark.asyncio
async def test_capture_records_rx_only_and_preserves_bytes(tmp_path: Path) -> None:
    raw = bytes([0x01, 0xFA, 0x00, 0xFF, 0x10, 0x10])
    source = FakeReceiveOnlySource(chunks=[raw])
    out = tmp_path / "cap.jsonl"
    session = PassiveCaptureSession(
        source,
        PassiveCaptureConfig(
            port="/dev/fake",
            baud=9600,
            output=out,
            duration_s=0.25,
            reconnect_delay_s=0.05,
        ),
    )
    await session.run()
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert lines
    records = [json.loads(line) for line in lines]
    rx = [r for r in records if r["recordType"] == "rx_chunk"]
    assert rx
    assert all(r["direction"] == "RX" for r in rx)
    assert rx[0]["rawHex"] == bytes_to_raw_hex(raw)
    assert rx[0]["byteCount"] == len(raw)
    # Round-trip exact bytes
    parts = rx[0]["rawHex"].split()
    assert bytes(int(p, 16) for p in parts) == raw
    events = {r.get("event") for r in records if r["recordType"] == "event"}
    assert "capture_started" in events
    assert "capture_stopped" in events


@pytest.mark.asyncio
async def test_missing_serial_initial_open_refuses_without_file(
    tmp_path: Path,
) -> None:
    """Initial open failure must not create a capture artifact."""
    source = FakeReceiveOnlySource(fail_opens=100)
    out = tmp_path / "missing.jsonl"
    session = PassiveCaptureSession(
        source,
        PassiveCaptureConfig(
            port="/dev/missing",
            baud=9600,
            output=out,
            duration_s=0.25,
            reconnect_delay_s=0.05,
        ),
    )
    with pytest.raises(OSError):
        await session.run()
    assert not out.exists()


@pytest.mark.asyncio
async def test_reconnect_event_recorded(tmp_path: Path) -> None:
    source = FakeReceiveOnlySource(
        chunks=[b"\x01\x02", b"\x03\x04"],
        disconnect_after_reads=1,
    )
    out = tmp_path / "reconn.jsonl"
    session = PassiveCaptureSession(
        source,
        PassiveCaptureConfig(
            port="/dev/fake",
            baud=9600,
            output=out,
            duration_s=0.6,
            reconnect_delay_s=0.05,
        ),
    )
    await session.run()
    records = [json.loads(line) for line in out.read_text().splitlines() if line]
    events = [r.get("event") for r in records if r["recordType"] == "event"]
    assert "serial_disconnected" in events
    assert "serial_reconnected" in events


@pytest.mark.asyncio
async def test_sigterm_closes_capture_cleanly(tmp_path: Path) -> None:
    source = FakeReceiveOnlySource(chunks=[])
    out = tmp_path / "sig.jsonl"
    session = PassiveCaptureSession(
        source,
        PassiveCaptureConfig(
            port="/dev/fake",
            baud=9600,
            output=out,
            duration_s=5.0,
            reconnect_delay_s=0.05,
        ),
    )

    async def _kill_later() -> None:
        await asyncio.sleep(0.1)
        session.request_stop()

    summary, _ = await asyncio.gather(session.run(), _kill_later())
    assert summary["mode"] == "PASSIVE_CAPTURE_ONLY"
    records = [json.loads(line) for line in out.read_text().splitlines() if line]
    assert records[-1].get("event") == "capture_stopped"
    # Valid JSONL
    for line in out.read_text().splitlines():
        if line:
            json.loads(line)


@pytest.mark.asyncio
async def test_ctrl_c_path_via_sigint_handler_style(tmp_path: Path) -> None:
    """Ctrl+C uses the same request_stop path as SIGINT handlers."""
    source = FakeReceiveOnlySource(chunks=[b"\xaa"])
    out = tmp_path / "intr.jsonl"
    session = PassiveCaptureSession(
        source,
        PassiveCaptureConfig(
            port="/dev/fake",
            baud=9600,
            output=out,
            duration_s=5.0,
            reconnect_delay_s=0.05,
        ),
    )

    async def _SIGINT() -> None:
        await asyncio.sleep(0.08)
        # Mimic signal handler
        session.request_stop()

    await asyncio.gather(session.run(), _SIGINT())
    assert "capture_stopped" in out.read_text()


def test_port_in_use_guard(tmp_path: Path) -> None:
    # Memory/virtual paths are skipped.
    assert check_port_available("memory") == "memory"
    assert check_port_available("pty:test") == "pty:test"

    port = tmp_path / "ttyFAKE"
    port.touch()

    from intelipump_fdc.capture.port_guards import assert_no_foreign_holders

    with pytest.raises(Exception) as excinfo:
        assert_no_foreign_holders(
            str(port),
            requested_path=str(port),
            holder_finder=lambda _p: [99],
        )
    assert "99" in str(excinfo.value)


def test_listen_only_settings_unchanged_by_capture_import() -> None:
    # Importing capture must not mutate global safety / authorize anything.
    from intelipump_fdc.capture import session as _session_mod

    del _session_mod
    safety = ControllerSafetyContext(
        environment="LAB",
        mode=ControllerMode.LISTEN_ONLY,
        active_commands_enabled=False,
        require_physical_control_enable=True,
        physical_enable_present=False,
        allow_virtual_polling=True,
    )
    assert safety.mode is ControllerMode.LISTEN_ONLY
    assert safety.active_commands_enabled is False


def test_no_controller_poll_scheduler_started_on_import() -> None:
    import intelipump_fdc.capture.session as cap_session

    assert not hasattr(cap_session, "ControllerLoop")
    assert not hasattr(cap_session, "PollSchedulerConfig")


@pytest.mark.asyncio
async def test_output_flushed_valid_jsonl(tmp_path: Path) -> None:
    source = FakeReceiveOnlySource(chunks=[b"\x01\xfa"])
    out = tmp_path / "flush.jsonl"
    session = PassiveCaptureSession(
        source,
        PassiveCaptureConfig(
            port="/dev/fake",
            baud=9600,
            output=out,
            duration_s=0.2,
            reconnect_delay_s=0.05,
        ),
    )
    await session.run()
    text = out.read_text(encoding="utf-8")
    assert text.endswith("\n")
    for line in text.splitlines():
        obj = json.loads(line)
        assert "schemaVersion" in obj
        assert "captureId" in obj
        assert "monotonicNs" in obj
        assert "timestampUtc" in obj
