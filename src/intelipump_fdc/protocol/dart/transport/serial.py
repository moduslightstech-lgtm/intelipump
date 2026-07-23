"""Serial port configuration and pyserial-backed async transport."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum

from intelipump_fdc.protocol.dart.transport.base import ByteTransport, TransportMetadata
from intelipump_fdc.protocol.dart.transport.errors import (
    TransportConfigError,
    TransportNotOpenError,
)


class SerialParity(StrEnum):
    ODD = "ODD"
    EVEN = "EVEN"
    NONE = "NONE"
    MARK = "MARK"
    SPACE = "SPACE"


@dataclass(frozen=True, slots=True)
class SerialConfig:
    """DART serial settings. Defaults match the line-level specification."""

    device: str
    baud_rate: int = 9600
    data_bits: int = 8
    parity: SerialParity = SerialParity.ODD
    stop_bits: int = 1
    read_chunk_size: int = 256
    open_timeout_s: float = 2.0
    reconnect_delay_s: float = 1.0
    read_timeout_s: float = 0.02
    write_timeout_s: float = 2.0
    exclusive_open: bool = False
    flow_control: bool = False

    def validate(self) -> None:
        if not self.device:
            raise TransportConfigError("device path is required")
        if self.baud_rate not in {9600, 19200}:
            raise TransportConfigError(
                f"baud_rate must be 9600 or 19200 (DART); got {self.baud_rate}"
            )
        if self.data_bits != 8:
            raise TransportConfigError(
                f"data_bits must be 8 (DART); got {self.data_bits}"
            )
        if self.parity is not SerialParity.ODD:
            raise TransportConfigError(
                f"parity must be ODD (DART); got {self.parity.value}"
            )
        if self.stop_bits != 1:
            raise TransportConfigError(
                f"stop_bits must be 1 (DART); got {self.stop_bits}"
            )
        if self.read_chunk_size < 1:
            raise TransportConfigError("read_chunk_size must be >= 1")
        if self.open_timeout_s <= 0:
            raise TransportConfigError("open_timeout_s must be > 0")
        if self.reconnect_delay_s < 0:
            raise TransportConfigError("reconnect_delay_s must be >= 0")
        if self.write_timeout_s <= 0:
            raise TransportConfigError("write_timeout_s must be > 0")
        if self.flow_control:
            raise TransportConfigError("flow_control must be disabled for DART")


def _parity_constant(parity: SerialParity) -> str:
    # pyserial uses single-letter codes.
    return {
        SerialParity.ODD: "O",
        SerialParity.EVEN: "E",
        SerialParity.NONE: "N",
        SerialParity.MARK: "M",
        SerialParity.SPACE: "S",
    }[parity]


class SerialTransport(ByteTransport):
    """Async wrapper around pyserial. Protocol code must not import pyserial."""

    def __init__(self, config: SerialConfig) -> None:
        config.validate()
        self._config = config
        self._ser: object | None = None
        self._open = False

    @property
    def is_open(self) -> bool:
        return self._open and self._ser is not None

    @property
    def metadata(self) -> TransportMetadata:
        c = self._config
        device = c.device
        kind = "serial_physical"
        if device.startswith("/tmp/") or device.startswith("pty:"):
            kind = "serial_virtual"
        return TransportMetadata(
            name="serial",
            device=device,
            baud_rate=c.baud_rate,
            parity=c.parity.value,
            data_bits=c.data_bits,
            stop_bits=c.stop_bits,
            kind=kind,
            notes=(
                ("virtual PTY",) if kind == "serial_virtual" else ("physical serial",)
            ),
        )

    async def open(self) -> None:
        if self._open:
            return

        def _open() -> object:
            import serial  # type: ignore[import-untyped]

            kwargs: dict[str, object] = {
                "port": self._config.device,
                "baudrate": self._config.baud_rate,
                "bytesize": self._config.data_bits,
                "parity": _parity_constant(self._config.parity),
                "stopbits": self._config.stop_bits,
                "timeout": self._config.read_timeout_s,
                "write_timeout": self._config.write_timeout_s,
                "xonxoff": False,
                "rtscts": False,
                "dsrdtr": False,
            }
            if self._config.exclusive_open:
                kwargs["exclusive"] = True
            try:
                return serial.Serial(**kwargs)
            except TypeError:
                kwargs.pop("exclusive", None)
                return serial.Serial(**kwargs)

        self._ser = await asyncio.wait_for(
            asyncio.to_thread(_open),
            timeout=self._config.open_timeout_s,
        )
        self._open = True

    async def close(self) -> None:
        ser = self._ser
        self._open = False
        self._ser = None
        if ser is not None:
            await asyncio.to_thread(ser.close)  # type: ignore[attr-defined]

    async def read(self, max_bytes: int) -> bytes:
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("serial transport not open")
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        n = min(max_bytes, self._config.read_chunk_size)
        return await asyncio.to_thread(self._ser.read, n)  # type: ignore[attr-defined]

    async def write(self, data: bytes) -> int:
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("serial transport not open")
        written = await asyncio.to_thread(self._ser.write, data)  # type: ignore[attr-defined]
        return int(written or 0)

    async def drain(self) -> None:
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("serial transport not open")
        await asyncio.to_thread(self._ser.flush)  # type: ignore[attr-defined]
