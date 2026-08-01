"""Receive-only serial reader for passive DART capture.

Hard rules:
- Never transmit on the serial port (no pyserial TX API calls)
- Never assert RS-485 driver-enable or force RTS for TX
- Raw binary reads only; timestamp immediately after each OS read
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
    """pyserial-backed RX-only reader. No write / RTS / DE methods exist."""

    def __init__(self, config: PassiveSerialConfig | None = None) -> None:
        self._config = config or PassiveSerialConfig()
        self._config.validate()
        self._ser: object | None = None
        self._open = False

    @property
    def config(self) -> PassiveSerialConfig:
        return self._config

    @property
    def is_open(self) -> bool:
        return self._open and self._ser is not None

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
            # pyserial constructor default only; this class never transmits.
            "write_timeout": 0.05,
            "xonxoff": False,
            "rtscts": False,
            "dsrdtr": False,
        }
        if self._config.exclusive_open:
            kwargs["exclusive"] = True
        try:
            ser = serial.Serial(**kwargs)
        except TypeError:
            kwargs.pop("exclusive", None)
            ser = serial.Serial(**kwargs)
        # Leave modem-control lines untouched (no TX-enable assert).
        self._ser = ser
        self._open = True

    def close(self) -> None:
        ser = self._ser
        self._open = False
        self._ser = None
        if ser is not None:
            ser.close()  # type: ignore[attr-defined]

    def read_chunk(self) -> SerialChunk:
        if not self.is_open or self._ser is None:
            raise RuntimeError("passive serial reader is not open")
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
