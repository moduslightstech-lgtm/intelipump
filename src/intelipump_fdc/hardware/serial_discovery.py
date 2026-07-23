"""USB/serial adapter discovery (prefer stable by-id paths)."""

from __future__ import annotations

import os
import platform
import re
from pathlib import Path
from typing import Any

from intelipump_fdc.hardware.errors import SerialDiscoveryError
from intelipump_fdc.hardware.models import SerialDeviceInfo

_MAC_CU_RE = re.compile(
    r"^/dev/cu\.(usbserial|wchusbserial|usbmodem|SLAB_USBtoUART|usb)",
    re.IGNORECASE,
)
_LINUX_TTY_RE = re.compile(r"^/dev/tty(USB|ACM)\d+$")


def _hex_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text, 16) if text.lower().startswith("0x") else int(text, 0)
    except ValueError:
        try:
            return int(text, 16)
        except ValueError:
            return None


def _linux_by_id_map() -> dict[str, str]:
    """Map real device path -> /dev/serial/by-id symlink path."""
    by_id = Path("/dev/serial/by-id")
    mapping: dict[str, str] = {}
    if not by_id.is_dir():
        return mapping
    for link in by_id.iterdir():
        try:
            target = link.resolve()
            mapping[str(target)] = str(link)
            mapping[os.path.realpath(str(link))] = str(link)
        except OSError:
            continue
    return mapping


def _stable_id_for(port: Any, by_id_path: str | None) -> str:
    if by_id_path:
        return Path(by_id_path).name
    parts: list[str] = []
    vid = _hex_int(getattr(port, "vid", None))
    pid = _hex_int(getattr(port, "pid", None))
    if vid is not None and pid is not None:
        parts.append(f"{vid:04x}:{pid:04x}")
    serial = getattr(port, "serial_number", None)
    if serial:
        parts.append(str(serial))
    location = getattr(port, "location", None)
    if location:
        parts.append(str(location))
    device = getattr(port, "device", "") or "unknown"
    if not parts:
        return Path(device).name
    return "-".join(parts)


def _is_candidate_device(device: str) -> bool:
    system = platform.system()
    if system == "Darwin":
        return bool(_MAC_CU_RE.match(device)) or device.startswith("/dev/cu.")
    if system == "Linux":
        if device.startswith("/dev/serial/by-id/"):
            return True
        return bool(_LINUX_TTY_RE.match(device))
    # Other platforms: accept anything pyserial reports.
    return bool(device)


def list_serial_devices(*, usb_only: bool = True) -> tuple[SerialDeviceInfo, ...]:
    """Enumerate serial ports via pyserial; prefer by-id paths on Linux."""
    try:
        from serial.tools import list_ports  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover
        raise SerialDiscoveryError("pyserial is required for discovery") from exc

    by_id = _linux_by_id_map()
    found: list[SerialDeviceInfo] = []
    seen_stable: set[str] = set()

    for port in list_ports.comports():
        device = str(port.device or "")
        if not device:
            continue
        if (
            usb_only
            and platform.system() in {"Linux", "Darwin"}
            and not _is_candidate_device(device)
            and not getattr(port, "vid", None)
        ):
            continue
        real = os.path.realpath(device)
        by_id_path = by_id.get(real) or by_id.get(device)
        preferred = by_id_path or device
        info = SerialDeviceInfo(
            device_path=preferred,
            stable_id=_stable_id_for(port, by_id_path),
            vid=_hex_int(getattr(port, "vid", None)),
            pid=_hex_int(getattr(port, "pid", None)),
            serial_number=getattr(port, "serial_number", None),
            manufacturer=getattr(port, "manufacturer", None),
            product=getattr(port, "product", None) or getattr(port, "description", None),
            interface=getattr(port, "interface", None),
            usb_location=getattr(port, "location", None),
            hardware_id=getattr(port, "hwid", None),
            detected_driver=None,
            description=getattr(port, "description", None),
            by_id_path=by_id_path,
        )
        if info.stable_id in seen_stable:
            continue
        seen_stable.add(info.stable_id)
        found.append(info)

    # Prefer by-id entries first in sort order.
    found.sort(key=lambda d: (0 if d.by_id_path else 1, d.stable_id, d.device_path))
    return tuple(found)


def resolve_device(
    *,
    path: str | None = None,
    stable_id: str | None = None,
    devices: tuple[SerialDeviceInfo, ...] | None = None,
) -> SerialDeviceInfo:
    """Resolve a configured path or stable ID to a discovered device."""
    devices = devices if devices is not None else list_serial_devices()
    if path:
        real = os.path.realpath(path)
        for dev in devices:
            if (
                path == dev.device_path
                or path == dev.by_id_path
                or real == os.path.realpath(dev.device_path)
            ):
                return dev
        # Path may be valid even if not currently enumerated (hotplug race).
        return SerialDeviceInfo(
            device_path=path,
            stable_id=Path(path).name,
            by_id_path=path if "/by-id/" in path else None,
        )
    if stable_id:
        for dev in devices:
            if (
                dev.stable_id == stable_id
                or (dev.by_id_path and Path(dev.by_id_path).name == stable_id)
                or Path(dev.device_path).name == stable_id
            ):
                return dev
        raise SerialDiscoveryError(f"no serial device matches stable_id={stable_id!r}")
    raise SerialDiscoveryError("path or stable_id is required")


def prefer_stable_path(device_path: str) -> str:
    """Return by-id path when available for a tty path."""
    if "/by-id/" in device_path:
        return device_path
    by_id = _linux_by_id_map()
    real = os.path.realpath(device_path)
    return by_id.get(real) or by_id.get(device_path) or device_path
