"""Receive-only serial reader for passive DART capture.

Hard rules:
- Never transmit on the serial port (no pyserial TX API calls)
- Never assert RS-485 driver-enable or force RTS for TX
- Raw binary reads only; timestamp immediately after each OS read

Open limitations (documented, not silently assumed safe):
- pyserial ``Serial()`` typically opens the device read/write at the OS level.
  This class never exposes or calls write/RTS/DE; behavioral RX-only is enforced
  in software, not by a proven kernel TX disable.
- Physical TX inhibit (listen-only adapter, TX line disconnected, DE tied off)
  cannot be proven in software. Prefer hardware that cannot drive the bus.
- ``exclusive=True`` is requested when supported; if the pyserial build rejects
  that kwarg, open proceeds without exclusive ownership (platform limitation).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from intelipump_fdc.protocol.dart.transport.serial import (
    SerialParity,
    _parity_constant,
    read_serial_chunk,
)

# Operator-facing summary of open limitations (also in README).
PASSIVE_SERIAL_OPEN_LIMITATIONS = (
    "pyserial opens the port read/write at the OS level; this reader never "
    "calls write/RTS/DE. Physical TX inhibit is not proven in software — use a "
    "listen-only adapter or disconnect TX/DE before hardware capture."
)


@dataclass(frozen=True, slots=True)
class SerialChunk:
    """One OS-level read result with capture timestamps."""

    data: bytes
    capture_timestamp_utc: datetime
    monotonic_timestamp_ns: int
    serial_device: str
    baud: int
    parity: str
    stop_bits: int


@dataclass(frozen=True, slots=True)
class PassiveSerialConfig:
    device: str = "/dev/ttyUSB0"
    baud: int = 9600
    data_bits: int = 8
    parity: SerialParity = SerialParity.ODD
    stop_bits: int = 1
    read_timeout_s: float = 0.05
    read_chunk_size: int = 256
    exclusive_open: bool = True

    def validate(self) -> None:
        if not self.device:
            raise ValueError("device path is required")
        if self.baud not in {9600, 19200}:
            raise ValueError(f"baud must be 9600 or 19200; got {self.baud}")
        if self.data_bits != 8:
            raise ValueError("data_bits must be 8")
        if self.parity is not SerialParity.ODD:
            raise ValueError("parity must be ODD")
        if self.stop_bits != 1:
            raise ValueError("stop_bits must be 1")
        if self.read_timeout_s <= 0:
            raise ValueError("read_timeout_s must be > 0")
        if self.read_chunk_size < 1:
            raise ValueError("read_chunk_size must be >= 1")


class ByteSource(Protocol):
    """Minimal read-only byte source (tests inject fakes)."""

    def open(self) -> None: ...

    def close(self) -> None: ...

    def read_chunk(self) -> SerialChunk: ...

    @property
    def is_open(self) -> bool: ...

    @property
    def config(self) -> PassiveSerialConfig: ...


class PassiveSerialReader:
    """pyserial-backed RX-only reader. No write / RTS / DE methods exist.

    See ``PASSIVE_SERIAL_OPEN_LIMITATIONS``: OS open may be R/W; this class
    never transmits and does not assert TX-enable modem lines.
    """

    def __init__(self, config: PassiveSerialConfig | None = None) -> None:
        self._config = config or PassiveSerialConfig()
        self._config.validate()
        self._ser: object | None = None
        self._open = False
        self._exclusive_applied = False

    @property
    def config(self) -> PassiveSerialConfig:
        return self._config

    @property
    def is_open(self) -> bool:
        return self._open and self._ser is not None

    @property
    def exclusive_applied(self) -> bool:
        """True when the open used pyserial ``exclusive=True`` successfully."""
        return self._exclusive_applied

    def open(self) -> None:
        if self._open:
            return
        import serial  # type: ignore[import-untyped]

        kwargs: dict[str, object] = {
            "port": self._config.device,
            "baudrate": self._config.baud,
            "bytesize": self._config.data_bits,
            "parity": _parity_constant(self._config.parity),
            "stopbits": self._config.stop_bits,
            "timeout": self._config.read_timeout_s,
            # Constructor only; this class never calls write().
            "write_timeout": 0.05,
            "xonxoff": False,
            "rtscts": False,
            "dsrdtr": False,
        }
        exclusive_requested = self._config.exclusive_open
        if exclusive_requested:
            kwargs["exclusive"] = True
        self._exclusive_applied = False
        try:
            ser = serial.Serial(**kwargs)
            self._exclusive_applied = exclusive_requested
        except TypeError:
            # Platform/pyserial without exclusive= — still RX-only in software.
            kwargs.pop("exclusive", None)
            ser = serial.Serial(**kwargs)
            self._exclusive_applied = False
        # Leave modem-control lines untouched (no TX-enable assert).
        # Prefer a listen-only adapter; software cannot prove DE is off.
        self._ser = ser
        self._open = True

    def close(self) -> None:
        ser = self._ser
        self._open = False
        self._ser = None
        self._exclusive_applied = False
        if ser is not None:
            ser.close()  # type: ignore[attr-defined]

    def read_chunk(self) -> SerialChunk:
        if not self.is_open or self._ser is None:
            raise RuntimeError("passive serial reader is not open")
        # Read path only — never write / flush TX / toggle RTS/DE.
        data = read_serial_chunk(self._ser, self._config.read_chunk_size)
        # Timestamp immediately after the OS read returns.
        mono_ns = time.monotonic_ns()
        utc = datetime.now(UTC)
        return SerialChunk(
            data=data,
            capture_timestamp_utc=utc,
            monotonic_timestamp_ns=mono_ns,
            serial_device=self._config.device,
            baud=self._config.baud,
            parity=self._config.parity.value,
            stop_bits=self._config.stop_bits,
        )
