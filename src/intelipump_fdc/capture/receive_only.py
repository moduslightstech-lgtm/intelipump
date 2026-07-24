"""Receive-only serial source for passive capture.

Architectural guarantee: this module never exposes a write/drain API.
Physical TX inhibit cannot be proven in software; the CLI fails closed unless
the operator confirms TX is physically inhibited.
"""

from __future__ import annotations

import asyncio
import errno
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from intelipump_fdc.protocol.dart.transport.errors import (
    TransportConfigError,
    TransportNotOpenError,
)
from intelipump_fdc.protocol.dart.transport.serial import (
    SerialParity,
    _parity_constant,
    read_serial_chunk,
)


class PortInUseError(OSError):
    """Serial device is already open by another process."""


class TxInhibitNotConfirmedError(RuntimeError):
    """Software cannot prove TX is inhibited; refuse to start capture."""


@runtime_checkable
class ReceiveOnlySource(Protocol):
    """Byte source with no write capability (passive capture only)."""

    @property
    def is_open(self) -> bool: ...

    @property
    def device(self) -> str: ...

    @property
    def baud_rate(self) -> int: ...

    async def open(self) -> None: ...

    async def close(self) -> None: ...

    async def read(self, max_bytes: int) -> bytes: ...


@dataclass(frozen=True, slots=True)
class ReceiveOnlySerialConfig:
    device: str
    baud_rate: int = 9600
    data_bits: int = 8
    parity: SerialParity = SerialParity.ODD
    stop_bits: int = 1
    read_chunk_size: int = 256
    open_timeout_s: float = 2.0
    read_timeout_s: float = 0.05
    exclusive_open: bool = True

    def validate(self) -> None:
        if not self.device:
            raise TransportConfigError("device path is required")
        if self.baud_rate not in {9600, 19200}:
            raise TransportConfigError(
                f"baud_rate must be 9600 or 19200 (DART); got {self.baud_rate}"
            )
        if self.data_bits != 8:
            raise TransportConfigError("data_bits must be 8 (DART)")
        if self.parity is not SerialParity.ODD:
            raise TransportConfigError("parity must be ODD (DART)")
        if self.stop_bits != 1:
            raise TransportConfigError("stop_bits must be 1 (DART)")
        if self.read_chunk_size < 1:
            raise TransportConfigError("read_chunk_size must be >= 1")


def require_tx_physically_inhibited(*, confirmed: bool) -> None:
    """Fail closed: software cannot guarantee TX is disabled."""
    if not confirmed:
        raise TxInhibitNotConfirmedError(
            "PASSIVE_CAPTURE refused: software cannot prove TX is inhibited. "
            "Physically disconnect/inhibit TX, then pass "
            "--confirm-tx-physically-inhibited."
        )


def check_port_available(device: str) -> None:
    """Best-effort guard: refuse when the device path is already busy.

    Opens the port briefly with exclusive=True where supported, then closes.
    Virtual/memory paths used in tests may skip filesystem checks.
    """
    if device in {"memory", "in-memory"} or device.startswith("pty:"):
        return
    path = Path(device)
    if not path.exists():
        # Missing device is handled by the capture reconnect loop, not here.
        return
    try:
        import serial  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover
        raise TransportConfigError("pyserial is required") from exc

    serial_exc = getattr(serial, "SerialException", OSError)
    try:
        kwargs: dict[str, object] = {
            "port": device,
            "baudrate": 9600,
            "bytesize": 8,
            "parity": "O",
            "stopbits": 1,
            "timeout": 0.05,
            "write_timeout": 0.05,
            "xonxoff": False,
            "rtscts": False,
            "dsrdtr": False,
            "exclusive": True,
        }
        try:
            ser = serial.Serial(**kwargs)
        except TypeError:
            kwargs.pop("exclusive", None)
            ser = serial.Serial(**kwargs)
        try:
            # Touch without writing: ensure we never TX during the probe.
            _ = ser.in_waiting
        finally:
            ser.close()
    except (OSError, serial_exc) as exc:
        err = getattr(exc, "errno", None)
        msg = str(exc).lower()
        if err in {errno.EBUSY, errno.EACCES, errno.EPERM} or "busy" in msg or (
            "could not exclusive" in msg
        ):
            raise PortInUseError(
                f"serial port already in use: {device} ({exc})"
            ) from exc
        # Other open errors (permissions, missing mid-race) are deferred to capture open.
        if "permission" in msg or err == errno.EACCES:
            raise PortInUseError(
                f"serial port not available exclusively: {device} ({exc})"
            ) from exc


class ReceiveOnlySerialSource:
    """pyserial-backed RX-only source. Has no write/drain methods."""

    def __init__(self, config: ReceiveOnlySerialConfig) -> None:
        config.validate()
        self._config = config
        self._ser: object | None = None
        self._open = False

    @property
    def is_open(self) -> bool:
        return self._open and self._ser is not None

    @property
    def device(self) -> str:
        return self._config.device

    @property
    def baud_rate(self) -> int:
        return self._config.baud_rate

    async def open(self) -> None:
        if self._open:
            return

        def _open() -> object:
            import serial

            serial_exc = getattr(serial, "SerialException", OSError)
            kwargs: dict[str, object] = {
                "port": self._config.device,
                "baudrate": self._config.baud_rate,
                "bytesize": self._config.data_bits,
                "parity": _parity_constant(self._config.parity),
                "stopbits": self._config.stop_bits,
                "timeout": self._config.read_timeout_s,
                # write_timeout set but write is never invoked by this class.
                "write_timeout": 0.05,
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
            except (OSError, serial_exc) as exc:
                err = getattr(exc, "errno", None)
                msg = str(exc).lower()
                if err == errno.EBUSY or "busy" in msg or "exclusive" in msg:
                    raise PortInUseError(
                        f"serial port already in use: {self._config.device}"
                    ) from exc
                raise

        try:
            self._ser = await asyncio.wait_for(
                asyncio.to_thread(_open),
                timeout=self._config.open_timeout_s,
            )
        except PortInUseError:
            raise
        except Exception as exc:
            err = getattr(exc, "errno", None)
            msg = str(exc).lower()
            if err == errno.EBUSY or "busy" in msg:
                raise PortInUseError(
                    f"serial port already in use: {self._config.device}"
                ) from exc
            raise
        self._open = True

    async def close(self) -> None:
        ser = self._ser
        self._open = False
        self._ser = None
        if ser is not None:
            await asyncio.to_thread(ser.close)  # type: ignore[attr-defined]

    async def read(self, max_bytes: int) -> bytes:
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("receive-only serial not open")
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        n = min(max_bytes, self._config.read_chunk_size)
        ser = self._ser
        return await asyncio.to_thread(read_serial_chunk, ser, n)
