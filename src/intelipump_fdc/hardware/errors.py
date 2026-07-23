"""Hardware / RS-485 bench errors."""

from __future__ import annotations


class HardwareError(Exception):
    """Base hardware/bench error."""


class SerialDiscoveryError(HardwareError):
    """Serial port enumeration or resolution failed."""


class AdapterValidationError(HardwareError):
    """Adapter cannot satisfy required DART serial settings."""


class OddParityUnsupportedError(AdapterValidationError):
    """OS/adapter cannot open with odd parity — never silently fall back to 8N1."""


class BenchConfigError(HardwareError):
    """Invalid or incomplete bench configuration."""


class BenchRuntimeError(HardwareError):
    """Bench harness runtime failure."""


class PortNotFoundError(HardwareError):
    """Configured serial path does not exist."""


class AdapterPermissionError(HardwareError):
    """Permission denied opening the serial adapter."""


class AdapterBusyError(HardwareError):
    """Serial port is busy / exclusively locked."""


class SamePhysicalAdapterError(BenchConfigError):
    """Controller and simulator paths resolve to one physical adapter."""


class WriteTimeoutError(HardwareError):
    """Serial write timed out."""


class StaleFileDescriptorError(HardwareError):
    """Stale FD or disconnect during I/O."""


class DisconnectDuringIOError(StaleFileDescriptorError):
    """Adapter disconnected during read/write."""
