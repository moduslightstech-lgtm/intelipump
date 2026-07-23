"""Physical port identity checks for RS-485 bench."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

from intelipump_fdc.hardware.errors import (
    AdapterBusyError,
    AdapterPermissionError,
    AdapterValidationError,
    BenchConfigError,
    HardwareError,
    OddParityUnsupportedError,
    PortNotFoundError,
    SamePhysicalAdapterError,
    StaleFileDescriptorError,
    WriteTimeoutError,
)
from intelipump_fdc.hardware.models import SerialDeviceInfo
from intelipump_fdc.hardware.serial_discovery import (
    list_serial_devices,
    prefer_stable_path,
    resolve_device,
)


def physical_identity_key(device: SerialDeviceInfo) -> str:
    """Stable key for detecting two paths that are the same adapter."""
    if device.by_id_path:
        return f"by-id:{Path(device.by_id_path).name}"
    if device.vid is not None and device.pid is not None and device.serial_number:
        return f"usb:{device.vid:04x}:{device.pid:04x}:{device.serial_number}"
    if device.usb_location:
        return f"loc:{device.usb_location}"
    return f"realpath:{os.path.realpath(device.device_path)}"


def assert_distinct_physical_adapters(
    controller: SerialDeviceInfo, simulator: SerialDeviceInfo
) -> None:
    if physical_identity_key(controller) == physical_identity_key(simulator):
        raise SamePhysicalAdapterError(
            "controller_port and simulator_port resolve to the same physical adapter "
            f"({physical_identity_key(controller)})"
        )
    if os.path.realpath(controller.device_path) == os.path.realpath(
        simulator.device_path
    ):
        raise SamePhysicalAdapterError(
            "controller_port and simulator_port resolve to the same realpath"
        )


def classify_open_error(exc: BaseException, *, port: str) -> HardwareError:
    """Map OS/pyserial failures to explicit hardware errors (no 8N1 fallback)."""
    msg = str(exc).lower()
    if isinstance(exc, FileNotFoundError) or "no such file" in msg:
        return PortNotFoundError(f"port not found: {port}")
    if isinstance(exc, PermissionError) or "permission" in msg:
        return AdapterPermissionError(f"permission denied opening {port}")
    if "busy" in msg or "resource temporarily unavailable" in msg or "errno 16" in msg:
        return AdapterBusyError(f"port busy: {port}")
    if "parity" in msg or "odd" in msg:
        return OddParityUnsupportedError(f"odd parity unsupported on {port}: {exc}")
    if "timeout" in msg and "write" in msg:
        return WriteTimeoutError(f"write timeout on {port}: {exc}")
    if "bad file descriptor" in msg or "errno 9" in msg:
        return StaleFileDescriptorError(f"stale file descriptor on {port}: {exc}")
    if "disconnect" in msg or "device not configured" in msg or "errno 6" in msg:
        return StaleFileDescriptorError(f"disconnect during I/O on {port}: {exc}")
    return AdapterValidationError(f"failed to open {port}: {exc}")


def resolve_bench_ports(
    *,
    controller_port: str,
    simulator_port: str,
    controller_stable_id: str | None = None,
    simulator_stable_id: str | None = None,
) -> tuple[SerialDeviceInfo, SerialDeviceInfo]:
    """Resolve and prefer by-id paths; reject same physical adapter."""
    if not controller_port or not simulator_port:
        raise BenchConfigError("controller_port and simulator_port are required")
    devices = list_serial_devices(usb_only=False)
    controller = resolve_device(
        path=prefer_stable_path(controller_port),
        stable_id=controller_stable_id,
        devices=devices,
    )
    simulator = resolve_device(
        path=prefer_stable_path(simulator_port),
        stable_id=simulator_stable_id,
        devices=devices,
    )
    if controller.by_id_path:
        controller = replace(controller, device_path=controller.by_id_path)
    if simulator.by_id_path:
        simulator = replace(simulator, device_path=simulator.by_id_path)
    assert_distinct_physical_adapters(controller, simulator)
    for label, path in (
        ("controller", controller.device_path),
        ("simulator", simulator.device_path),
    ):
        if path.startswith("/tmp/") or path.startswith("pty:"):
            continue
        if "/serial/by-id/" in path and not Path(path).exists():
            raise PortNotFoundError(f"{label} port not found: {path}")
    return controller, simulator


def ensure_port_present(path: str) -> None:
    if path.startswith("/tmp/") or path.startswith("pty:"):
        return
    if not Path(path).exists():
        raise PortNotFoundError(f"port not found: {path}")
