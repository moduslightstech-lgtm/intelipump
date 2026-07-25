"""Fail-closed port ownership guards for passive capture.

Primary protection: kernel exclusive serial open (pyserial exclusive=True +
TIOCEXCL). Secondary: application flock on a lock derived from the canonical
device path. Confirmation flags alone are never trusted.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

DEFAULT_CONTROLLER_SERVICE = "intelipump.service"
DEFAULT_LOCK_DIR = Path("/run/lock")
# Linux TIOCEXCL; macOS also exposes termios.TIOCEXCL when available.
_TIOCEXCL_FALLBACK = 0x540C


class PassiveCaptureRefuseReason(StrEnum):
    DEVICE_BUSY = "device_busy"
    PERMISSION_DENIED = "permission_denied"
    DEVICE_MISSING = "device_missing"
    EXCLUSIVE_UNSUPPORTED = "exclusive_locking_unsupported"
    SERVICE_ACTIVE = "controller_service_active"
    LOCK_CONTENTION = "lock_file_contention"
    TIOCEXCL_FAILED = "tiocexcl_failed"
    UNKNOWN_OPEN_FAILURE = "unknown_open_failure"
    TX_NOT_CONFIRMED = "tx_inhibit_not_confirmed"
    CONTROLLER_STOP_NOT_CONFIRMED = "controller_stop_not_confirmed"


class PassiveCaptureRefusedError(RuntimeError):
    """Capture must not start; guards failed before session/file creation."""

    def __init__(
        self,
        message: str,
        *,
        reason: PassiveCaptureRefuseReason,
        requested_path: str | None = None,
        canonical_path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.requested_path = requested_path
        self.canonical_path = canonical_path

    def exit_message(self) -> str:
        return f"PASSIVE_CAPTURE_REFUSED: {self}"


def is_virtual_or_test_port(device: str) -> bool:
    return device in {"memory", "in-memory"} or device.startswith(
        ("pty:", "/tmp/")
    )


def resolve_canonical_device(device: str) -> str:
    """Resolve symlinks (e.g. udev alias → /dev/ttyUSB0)."""
    if is_virtual_or_test_port(device):
        return device
    path = Path(device)
    if not path.exists():
        raise PassiveCaptureRefusedError(
            f"serial device missing: {device}",
            reason=PassiveCaptureRefuseReason.DEVICE_MISSING,
            requested_path=device,
        )
    try:
        canonical = os.path.realpath(device)
    except OSError as exc:
        raise PassiveCaptureRefusedError(
            f"cannot resolve canonical path for {device}: {exc}",
            reason=PassiveCaptureRefuseReason.UNKNOWN_OPEN_FAILURE,
            requested_path=device,
        ) from exc
    return canonical


def lock_basename_for_canonical(canonical: str) -> str:
    name = Path(canonical).name
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in name)
    return f"intelipump-{safe}.lock"


def lock_path_for_canonical(
    canonical: str, *, lock_dir: Path = DEFAULT_LOCK_DIR
) -> Path:
    return lock_dir / lock_basename_for_canonical(canonical)


@dataclass
class AppDeviceLock:
    """Non-blocking flock on a lock file keyed by canonical device."""

    path: Path
    requested_path: str
    canonical_path: str
    _fp: Any = None

    @classmethod
    def acquire(
        cls,
        *,
        requested_path: str,
        canonical_path: str,
        lock_dir: Path = DEFAULT_LOCK_DIR,
        pid: int | None = None,
        process_name: str | None = None,
    ) -> AppDeviceLock:
        lock_dir.mkdir(parents=True, exist_ok=True)
        path = lock_path_for_canonical(canonical_path, lock_dir=lock_dir)
        fp = path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            fp.close()
            stale_meta = _read_lock_metadata(path)
            detail = f" existing_lock={stale_meta}" if stale_meta else ""
            raise PassiveCaptureRefusedError(
                f"application lock contended for {canonical_path} "
                f"(requested={requested_path}){detail}",
                reason=PassiveCaptureRefuseReason.LOCK_CONTENTION,
                requested_path=requested_path,
                canonical_path=canonical_path,
            ) from exc

        meta = {
            "pid": pid if pid is not None else os.getpid(),
            "processName": process_name or Path(sys.argv[0]).name,
            "requestedPath": requested_path,
            "canonicalPath": canonical_path,
            "timestampUtc": datetime.now(UTC).isoformat(),
        }
        fp.seek(0)
        fp.truncate()
        fp.write(json.dumps(meta, separators=(",", ":")) + "\n")
        fp.flush()
        return cls(
            path=path,
            requested_path=requested_path,
            canonical_path=canonical_path,
            _fp=fp,
        )

    def release(self) -> None:
        fp = self._fp
        self._fp = None
        if fp is None:
            return
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
        finally:
            fp.close()

    def __enter__(self) -> AppDeviceLock:
        return self

    def __exit__(self, *exc: object) -> None:
        del exc
        self.release()


def _read_lock_metadata(path: Path) -> dict[str, Any] | None:
    try:
        text = path.read_text(encoding="utf-8").strip().splitlines()
        if not text:
            return None
        data = json.loads(text[0])
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


SystemctlRunner = Callable[..., subprocess.CompletedProcess[str]]


def assert_controller_service_inactive(
    service_name: str = DEFAULT_CONTROLLER_SERVICE,
    *,
    runner: SystemctlRunner | None = None,
    timeout_s: float = 2.0,
) -> None:
    """Refuse when systemd reports the controller service as active."""
    run = runner or subprocess.run
    try:
        result = run(
            ["systemctl", "is-active", "--quiet", service_name],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except FileNotFoundError as exc:
        raise PassiveCaptureRefusedError(
            "systemctl not available; cannot verify controller service is stopped",
            reason=PassiveCaptureRefuseReason.UNKNOWN_OPEN_FAILURE,
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise PassiveCaptureRefusedError(
            f"systemctl timed out checking {service_name}",
            reason=PassiveCaptureRefuseReason.UNKNOWN_OPEN_FAILURE,
        ) from exc

    # systemctl is-active --quiet: 0 = active, non-zero = inactive/failed/unknown
    if result.returncode == 0:
        raise PassiveCaptureRefusedError(
            f"{service_name} is active and owns or may own the serial interface.",
            reason=PassiveCaptureRefuseReason.SERVICE_ACTIVE,
        )


def find_device_holder_pids(canonical_path: str) -> list[int]:
    """Best-effort: PIDs with an open fd resolving to ``canonical_path``."""
    if is_virtual_or_test_port(canonical_path):
        return []
    holders: set[int] = set()
    proc = Path("/proc")
    if not proc.is_dir():
        return _fuser_pids(canonical_path)

    self_pid = os.getpid()
    try:
        target = os.path.realpath(canonical_path)
    except OSError:
        return []

    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == self_pid:
            continue
        fd_dir = entry / "fd"
        try:
            for fd in fd_dir.iterdir():
                try:
                    link = os.readlink(fd)
                except OSError:
                    continue
                try:
                    if os.path.realpath(link) == target:
                        holders.add(pid)
                        continue
                except OSError:
                    pass
                if link == target:
                    holders.add(pid)
        except OSError:
            continue
    return sorted(holders)


def _fuser_pids(canonical_path: str) -> list[int]:
    try:
        result = subprocess.run(
            ["fuser", canonical_path],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []
    blob = f"{result.stdout} {result.stderr}"
    pids: list[int] = []
    for token in blob.replace(":", " ").split():
        if token.isdigit():
            pids.append(int(token))
    return sorted(set(pids))


def assert_no_foreign_holders(
    canonical_path: str,
    *,
    requested_path: str,
    holder_finder: Callable[[str], list[int]] | None = None,
) -> None:
    finder = holder_finder or find_device_holder_pids
    holders = finder(canonical_path)
    if not holders:
        return
    raise PassiveCaptureRefusedError(
        f"serial device busy: canonical={canonical_path} "
        f"requested={requested_path} holder_pids={holders}",
        reason=PassiveCaptureRefuseReason.DEVICE_BUSY,
        requested_path=requested_path,
        canonical_path=canonical_path,
    )


def tiocexcl_request(fd: int) -> None:
    """Apply TIOCEXCL; fail closed if exclusivity cannot be established."""
    try:
        import termios

        req = getattr(termios, "TIOCEXCL", _TIOCEXCL_FALLBACK)
    except ImportError:
        req = _TIOCEXCL_FALLBACK
    try:
        fcntl.ioctl(fd, req)
    except OSError as exc:
        raise PassiveCaptureRefusedError(
            f"TIOCEXCL failed; exclusive serial ownership unavailable: {exc}",
            reason=PassiveCaptureRefuseReason.TIOCEXCL_FAILED,
        ) from exc


def classify_open_failure(
    exc: BaseException,
    *,
    requested_path: str,
    canonical_path: str,
) -> PassiveCaptureRefusedError:
    err = getattr(exc, "errno", None)
    msg = str(exc).lower()
    if isinstance(exc, PassiveCaptureRefusedError):
        return exc
    if err == errno.ENOENT or "no such file" in msg:
        reason = PassiveCaptureRefuseReason.DEVICE_MISSING
    elif err in {errno.EACCES, errno.EPERM} or "permission" in msg:
        reason = PassiveCaptureRefuseReason.PERMISSION_DENIED
    elif err == errno.EBUSY or "busy" in msg or "exclusive" in msg:
        reason = PassiveCaptureRefuseReason.DEVICE_BUSY
    elif "exclusive" in msg and ("not support" in msg or "unexpected keyword" in msg):
        reason = PassiveCaptureRefuseReason.EXCLUSIVE_UNSUPPORTED
    else:
        reason = PassiveCaptureRefuseReason.UNKNOWN_OPEN_FAILURE
    return PassiveCaptureRefusedError(
        f"{reason.value}: requested={requested_path} "
        f"canonical={canonical_path} error={exc}",
        reason=reason,
        requested_path=requested_path,
        canonical_path=canonical_path,
    )


def run_preflight_guards(
    *,
    port: str,
    confirm_tx_physically_inhibited: bool,
    confirm_controller_stopped: bool,
    controller_service: str = DEFAULT_CONTROLLER_SERVICE,
    lock_dir: Path = DEFAULT_LOCK_DIR,
    skip_port_in_use_check: bool = False,
    skip_service_check: bool = False,
    systemctl_runner: SystemctlRunner | None = None,
    holder_finder: Callable[[str], list[int]] | None = None,
    require_app_lock: bool = True,
) -> tuple[str, str, AppDeviceLock | None]:
    """Run all pre-open guards. Does not create a capture file.

    Returns ``(requested_path, canonical_path, app_lock_or_none)``.
    """
    from intelipump_fdc.capture.receive_only import require_tx_physically_inhibited

    try:
        require_tx_physically_inhibited(confirmed=confirm_tx_physically_inhibited)
    except Exception as exc:
        raise PassiveCaptureRefusedError(
            str(exc),
            reason=PassiveCaptureRefuseReason.TX_NOT_CONFIRMED,
            requested_path=port,
        ) from exc

    virtual = is_virtual_or_test_port(port)
    if not confirm_controller_stopped and not virtual:
        raise PassiveCaptureRefusedError(
            "pass --confirm-controller-stopped after stopping "
            f"{controller_service} and verifying the port is free.",
            reason=PassiveCaptureRefuseReason.CONTROLLER_STOP_NOT_CONFIRMED,
            requested_path=port,
        )

    if confirm_controller_stopped and not skip_service_check and not virtual:
        assert_controller_service_inactive(
            controller_service, runner=systemctl_runner
        )

    canonical = resolve_canonical_device(port)

    app_lock: AppDeviceLock | None = None
    if require_app_lock and not virtual and not skip_port_in_use_check:
        app_lock = AppDeviceLock.acquire(
            requested_path=port,
            canonical_path=canonical,
            lock_dir=lock_dir,
        )

    if not skip_port_in_use_check and not virtual:
        try:
            assert_no_foreign_holders(
                canonical,
                requested_path=port,
                holder_finder=holder_finder,
            )
        except PassiveCaptureRefusedError:
            if app_lock is not None:
                app_lock.release()
            raise

    return port, canonical, app_lock


def same_serial_device(path_a: str, path_b: str) -> bool:
    """True when two paths resolve to the same canonical device."""
    if is_virtual_or_test_port(path_a) or is_virtual_or_test_port(path_b):
        return path_a == path_b
    try:
        return resolve_canonical_device(path_a) == resolve_canonical_device(path_b)
    except PassiveCaptureRefusedError:
        return os.path.realpath(path_a) == os.path.realpath(path_b)
