"""Port ownership / exclusive-open guard tests for passive capture."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from intelipump_fdc.capture.cli_capture import run as capture_run
from intelipump_fdc.capture.port_guards import (
    AppDeviceLock,
    PassiveCaptureRefusedError,
    PassiveCaptureRefuseReason,
    assert_controller_service_inactive,
    assert_no_foreign_holders,
    lock_path_for_canonical,
    resolve_canonical_device,
    run_preflight_guards,
    same_serial_device,
    tiocexcl_request,
)
from intelipump_fdc.capture.receive_only import (
    ReceiveOnlySerialConfig,
    ReceiveOnlySerialSource,
    require_tx_physically_inhibited,
)
from intelipump_fdc.capture.session import PassiveCaptureConfig, PassiveCaptureSession


@dataclass
class _FailOpenSource:
    device: str = "/dev/missing"
    baud_rate: int = 9600
    _open: bool = False

    @property
    def is_open(self) -> bool:
        return self._open

    async def open(self) -> None:
        raise OSError("No such device")

    async def close(self) -> None:
        self._open = False

    async def read(self, max_bytes: int) -> bytes:
        del max_bytes
        raise OSError("not open")


def test_symlink_and_canonical_treated_as_same_device(tmp_path: Path) -> None:
    tty = tmp_path / "ttyUSB0"
    tty.touch()
    alias = tmp_path / "intelipump-controller"
    alias.symlink_to(tty)
    assert resolve_canonical_device(str(alias)) == os.path.realpath(str(tty))
    assert same_serial_device(str(alias), str(tty))


def test_tmp_alias_symlink_resolves_and_service_active_refuses(
    tmp_path: Path,
) -> None:
    """Linux pytest uses /tmp; /tmp paths must not be treated as virtual."""
    # Force a /tmp-prefixed tree even on macOS (/private/tmp is fine via realpath,
    # but the requested path must start with /tmp/ to cover the regression).
    base = Path("/tmp") / f"intelipump-pytest-{os.getpid()}-{tmp_path.name}"
    base.mkdir(parents=True, exist_ok=True)
    try:
        tty = base / "ttyUSB0"
        tty.touch()
        alias = base / "intelipump-controller"
        alias.symlink_to(tty)
        assert str(alias).startswith("/tmp/")
        assert resolve_canonical_device(str(alias)) == os.path.realpath(str(tty))

        def runner(*_a: object, **_k: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                args=["systemctl"], returncode=0, stdout="", stderr=""
            )

        with pytest.raises(PassiveCaptureRefusedError) as excinfo:
            run_preflight_guards(
                port=str(alias),
                confirm_tx_physically_inhibited=True,
                confirm_controller_stopped=True,
                lock_dir=base / "locks",
                systemctl_runner=runner,
                holder_finder=lambda _p: [],
            )
        assert excinfo.value.reason is PassiveCaptureRefuseReason.SERVICE_ACTIVE
    finally:
        for child in sorted(base.rglob("*"), reverse=True):
            if child.is_symlink() or child.is_file():
                child.unlink(missing_ok=True)
            elif child.is_dir():
                child.rmdir()
        base.rmdir()


def test_holder_on_canonical_refuses_alias_request(tmp_path: Path) -> None:
    tty = tmp_path / "ttyUSB0"
    tty.touch()
    alias = tmp_path / "intelipump-controller"
    alias.symlink_to(tty)
    canonical = resolve_canonical_device(str(alias))

    def holders(path: str) -> list[int]:
        assert path == canonical
        return [7482]

    with pytest.raises(PassiveCaptureRefusedError) as excinfo:
        assert_no_foreign_holders(
            canonical,
            requested_path=str(alias),
            holder_finder=holders,
        )
    assert excinfo.value.reason is PassiveCaptureRefuseReason.DEVICE_BUSY
    assert "7482" in str(excinfo.value)


def test_active_service_refuses_capture() -> None:
    def runner(*_a: object, **_k: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["systemctl"], returncode=0, stdout="", stderr=""
        )

    with pytest.raises(PassiveCaptureRefusedError) as excinfo:
        assert_controller_service_inactive(
            "intelipump.service", runner=runner
        )
    assert excinfo.value.reason is PassiveCaptureRefuseReason.SERVICE_ACTIVE
    assert "PASSIVE_CAPTURE_REFUSED" in excinfo.value.exit_message()


def test_inactive_service_allows_check() -> None:
    def runner(*_a: object, **_k: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["systemctl"], returncode=3, stdout="inactive\n", stderr=""
        )

    assert_controller_service_inactive("intelipump.service", runner=runner)


def test_lock_file_contention_refuses(tmp_path: Path) -> None:
    canonical = str(tmp_path / "ttyUSB0")
    Path(canonical).touch()
    first = AppDeviceLock.acquire(
        requested_path="/dev/alias",
        canonical_path=canonical,
        lock_dir=tmp_path / "locks",
    )
    try:
        with pytest.raises(PassiveCaptureRefusedError) as excinfo:
            AppDeviceLock.acquire(
                requested_path="/dev/alias2",
                canonical_path=canonical,
                lock_dir=tmp_path / "locks",
            )
        assert excinfo.value.reason is PassiveCaptureRefuseReason.LOCK_CONTENTION
    finally:
        first.release()


def test_stale_lock_metadata_does_not_block_flock(tmp_path: Path) -> None:
    canonical = str(tmp_path / "ttyUSB9")
    Path(canonical).touch()
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    stale = lock_path_for_canonical(canonical, lock_dir=lock_dir)
    stale.write_text(
        json.dumps(
            {
                "pid": 1,
                "processName": "dead",
                "requestedPath": "/old",
                "canonicalPath": canonical,
                "timestampUtc": "2000-01-01T00:00:00+00:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    lock = AppDeviceLock.acquire(
        requested_path="/dev/new",
        canonical_path=canonical,
        lock_dir=lock_dir,
        pid=4242,
        process_name="intelipump-capture-passive",
    )
    try:
        meta = json.loads(lock.path.read_text(encoding="utf-8").strip())
        assert meta["pid"] == 4242
        assert meta["canonicalPath"] == canonical
        assert meta["requestedPath"] == "/dev/new"
    finally:
        lock.release()


@pytest.mark.asyncio
async def test_exclusive_open_typeerror_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    port = tmp_path / "ttyUSB0"
    port.touch()

    def _boom(**kwargs: object) -> object:
        if "exclusive" in kwargs:
            raise TypeError("unexpected keyword argument 'exclusive'")
        raise AssertionError("must not fall back to shared open")

    monkeypatch.setattr("serial.Serial", _boom)
    source = ReceiveOnlySerialSource(
        ReceiveOnlySerialConfig(
            device=str(port),
            exclusive_open=True,
            apply_tiocexcl=True,
            requested_path=str(tmp_path / "alias"),
        )
    )
    with pytest.raises(PassiveCaptureRefusedError) as excinfo:
        await source.open()
    assert excinfo.value.reason is PassiveCaptureRefuseReason.EXCLUSIVE_UNSUPPORTED


@pytest.mark.asyncio
async def test_tiocexcl_failure_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    port = tmp_path / "ttyUSB0"
    port.touch()

    class FakeSer:
        def fileno(self) -> int:
            return 3

        def close(self) -> None:
            return None

    def _open(**_kwargs: object) -> FakeSer:
        return FakeSer()

    monkeypatch.setattr("serial.Serial", _open)

    def _tio_fail(_fd: int) -> None:
        raise PassiveCaptureRefusedError(
            "TIOCEXCL failed",
            reason=PassiveCaptureRefuseReason.TIOCEXCL_FAILED,
        )

    monkeypatch.setattr(
        "intelipump_fdc.capture.receive_only.tiocexcl_request", _tio_fail
    )
    source = ReceiveOnlySerialSource(
        ReceiveOnlySerialConfig(device=str(port), exclusive_open=True)
    )
    with pytest.raises(PassiveCaptureRefusedError) as excinfo:
        await source.open()
    assert excinfo.value.reason is PassiveCaptureRefuseReason.TIOCEXCL_FAILED


def test_failed_guard_creates_no_capture_started(tmp_path: Path) -> None:
    out = tmp_path / "should-not-exist.jsonl"
    with pytest.raises(PassiveCaptureRefusedError):
        run_preflight_guards(
            port=str(tmp_path / "missing-device"),
            confirm_tx_physically_inhibited=True,
            confirm_controller_stopped=True,
            skip_service_check=True,
            lock_dir=tmp_path / "locks",
        )
    assert not out.exists()


@pytest.mark.asyncio
async def test_failed_initial_open_creates_no_capture_file(tmp_path: Path) -> None:
    source = _FailOpenSource()
    out = tmp_path / "nope.jsonl"
    session = PassiveCaptureSession(
        source,
        PassiveCaptureConfig(
            port="/dev/missing",
            baud=9600,
            output=out,
            duration_s=0.2,
            reconnect_delay_s=0.05,
        ),
    )
    with pytest.raises(OSError):
        await session.run()
    assert not out.exists()


def test_cli_refuses_active_service_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    tty = tmp_path / "ttyUSB0"
    tty.touch()
    alias = tmp_path / "intelipump-controller"
    alias.symlink_to(tty)
    out = tmp_path / "cap.jsonl"

    def runner(*_a: object, **_k: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["systemctl"], returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(
        "intelipump_fdc.capture.port_guards.subprocess.run",
        runner,
    )
    with pytest.raises(SystemExit) as excinfo:
        capture_run(
            [
                "--port",
                str(alias),
                "--duration",
                "1",
                "--output",
                str(out),
                "--confirm-tx-physically-inhibited",
                "--confirm-controller-stopped",
                "--lock-dir",
                str(tmp_path / "locks"),
            ]
        )
    assert excinfo.value.code == 2
    assert not out.exists()


def test_preflight_inactive_service_and_free_port(tmp_path: Path) -> None:
    tty = tmp_path / "ttyUSB0"
    tty.touch()
    alias = tmp_path / "intelipump-controller"
    alias.symlink_to(tty)

    def runner(*_a: object, **_k: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["systemctl"], returncode=3, stdout="inactive\n", stderr=""
        )

    requested, canonical, lock = run_preflight_guards(
        port=str(alias),
        confirm_tx_physically_inhibited=True,
        confirm_controller_stopped=True,
        lock_dir=tmp_path / "locks",
        systemctl_runner=runner,
        holder_finder=lambda _p: [],
    )
    assert requested == str(alias)
    assert canonical == os.path.realpath(str(tty))
    assert lock is not None
    lock.release()


def test_receive_only_guarantees_intact() -> None:
    assert not hasattr(ReceiveOnlySerialSource, "write")
    require_tx_physically_inhibited(confirmed=True)


def test_tiocexcl_request_uses_ioctl(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[int, int]] = []

    def fake_ioctl(fd: int, req: int, *args: object) -> int:
        del args
        calls.append((fd, req))
        return 0

    monkeypatch.setattr("intelipump_fdc.capture.port_guards.fcntl.ioctl", fake_ioctl)
    tiocexcl_request(7)
    assert calls and calls[0][0] == 7


def test_preflight_holder_refuses_before_session(tmp_path: Path) -> None:
    tty = tmp_path / "ttyUSB0"
    tty.touch()
    alias = tmp_path / "intelipump-controller"
    alias.symlink_to(tty)

    def runner(*_a: object, **_k: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["systemctl"], returncode=3, stdout="inactive\n", stderr=""
        )

    with pytest.raises(PassiveCaptureRefusedError) as excinfo:
        run_preflight_guards(
            port=str(alias),
            confirm_tx_physically_inhibited=True,
            confirm_controller_stopped=True,
            lock_dir=tmp_path / "locks",
            systemctl_runner=runner,
            holder_finder=lambda _p: [7482],
        )
    assert excinfo.value.reason is PassiveCaptureRefuseReason.DEVICE_BUSY
