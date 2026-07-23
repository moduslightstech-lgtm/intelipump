"""Hardware / RS-485 office bench package."""

from intelipump_fdc.hardware.errors import (
    AdapterValidationError,
    BenchConfigError,
    HardwareError,
    OddParityUnsupportedError,
    PortNotFoundError,
    SamePhysicalAdapterError,
    SerialDiscoveryError,
)
from intelipump_fdc.hardware.models import BenchConfig, SerialDeviceInfo
from intelipump_fdc.hardware.serial_discovery import list_serial_devices, resolve_device

__all__ = [
    "AdapterValidationError",
    "BenchConfig",
    "BenchConfigError",
    "HardwareError",
    "OddParityUnsupportedError",
    "PortNotFoundError",
    "SamePhysicalAdapterError",
    "SerialDeviceInfo",
    "SerialDiscoveryError",
    "list_serial_devices",
    "resolve_device",
]
