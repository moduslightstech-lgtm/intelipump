"""Receive-only serial source for passive capture.

Architectural guarantee: this module never exposes a write/drain API.
Physical TX inhibit cannot be proven in software; the CLI fails closed unless
the operator confirms TX is physically inhibited.

Kernel exclusive ownership is mandatory for physical ports: pyserial
``exclusive=True`` with no shared-open fallback, plus ``TIOCEXCL``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from intelipump_fdc.capture.port_guards import (
    PassiveCaptureRefusedError,
    PassiveCaptureRefuseReason,
    classify_open_failure,
    is_virtual_or_test_port,
    resolve_canonical_device,
    tiocexcl_request,
)
from intelipump_fdc.protocol.dart.transport.errors import (
    TransportConfigError,
    TransportNotOpenError,
)
from intelipump_fdc.protocol.dart.transport.serial import (
    SerialParity,
    _parity_constant,
    read_serial_chunk,
)


class PortInUseError(PassiveCaptureRefusedError):
    """Serial device is already open by another process."""

    def __init__(
        self,
        message: str,
        *,
        requested_path: str | None = None,
        canonical_path: str | None = None,
    ) -> None:
        super().__init__(
            message,
            reason=PassiveCaptureRefuseReason.DEVICE_BUSY,
            requested_path=requested_path,
            canonical_path=canonical_path,
        )


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
    # Physical ports always require exclusive; never silently share.
    exclusive_open: bool = True
    apply_tiocexcl: bool = True
    requested_path: str | None = None

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
        if not self.exclusive_open and not is_virtual_or_test_port(self.device):
            raise TransportConfigError(
                "exclusive_open is required for physical passive capture ports"
            )


def require_tx_physically_inhibited(*, confirmed: bool) -> None:
    """Fail closed: software cannot guarantee TX is disabled."""
    if not confirmed:
        raise TxInhibitNotConfirmedError(
            "PASSIVE_CAPTURE refused: software cannot prove TX is inhibited. "
            "Physically disconnect/inhibit TX, then pass "
            "--confirm-tx-physically-inhibited."
        )


def check_port_available(
    device: str,
    *,
    holder_finder: object | None = None,
) -> str:
    """Resolve canonical path and refuse if foreign holders are visible.

    Returns the canonical device path. Does not open the port (opening is
    performed once by ``ReceiveOnlySerialSource.open`` with exclusive locks).
    """
    del holder_finder  # reserved for call-site injection via port_guards
    if is_virtual_or_test_port(device):
        return device
    from intelipump_fdc.capture.port_guards import assert_no_foreign_holders

    canonical = resolve_canonical_device(device)
    assert_no_foreign_holders(canonical, requested_path=device)
    return canonical


class ReceiveOnlySerialSource:
    """pyserial-backed RX-only source. Has no write/drain methods."""

    def __init__(self, config: ReceiveOnlySerialConfig) -> None:
        config.validate()
        self._requested = config.requested_path or config.device
        if is_virtual_or_test_port(config.device):
            canonical = config.device
        else:
            canonical = resolve_canonical_device(config.device)
        self._config = ReceiveOnlySerialConfig(
            device=canonical,
            baud_rate=config.baud_rate,
            data_bits=config.data_bits,
            parity=config.parity,
            stop_bits=config.stop_bits,
            read_chunk_size=config.read_chunk_size,
            open_timeout_s=config.open_timeout_s,
            read_timeout_s=config.read_timeout_s,
            exclusive_open=config.exclusive_open,
            apply_tiocexcl=config.apply_tiocexcl,
            requested_path=self._requested,
        )
        self._ser: object | None = None
        self._open = False

    @property
    def is_open(self) -> bool:
        return self._open and self._ser is not None

    @property
    def device(self) -> str:
        return self._config.device

    @property
    def requested_path(self) -> str:
        return self._requested

    @property
    def baud_rate(self) -> int:
        return self._config.baud_rate

    async def open(self) -> None:
        if self._open:
            return

        def _open() -> object:
            import serial  # type: ignore[import-untyped]

            serial_exc = getattr(serial, "SerialException", OSError)
            kwargs: dict[str, object] = {
                "port": self._config.device,
                "baudrate": self._config.baud_rate,
                "bytesize": self._config.data_bits,
                "parity": _parity_constant(self._config.parity),
                "stopbits": self._config.stop_bits,
                "timeout": self._config.read_timeout_s,
                "write_timeout": 0.05,
                "xonxoff": False,
                "rtscts": False,
                "dsrdtr": False,
            }
            if self._config.exclusive_open:
                kwargs["exclusive"] = True
            try:
                ser = serial.Serial(**kwargs)
            except TypeError as exc:
                # Never fall back to a shared open.
                raise PassiveCaptureRefusedError(
                    "pyserial exclusive=True unsupported; refusing shared open "
                    f"(requested={self._requested} canonical={self._config.device})",
                    reason=PassiveCaptureRefuseReason.EXCLUSIVE_UNSUPPORTED,
                    requested_path=self._requested,
                    canonical_path=self._config.device,
                ) from exc
            except (OSError, serial_exc) as exc:
                raise classify_open_failure(
                    exc,
                    requested_path=self._requested,
                    canonical_path=self._config.device,
                ) from exc

            if self._config.apply_tiocexcl and self._config.exclusive_open:
                try:
                    fileno = ser.fileno()
                    tiocexcl_request(int(fileno))
                except PassiveCaptureRefusedError:
                    ser.close()
                    raise
                except Exception as exc:
                    ser.close()
                    raise PassiveCaptureRefusedError(
                        f"TIOCEXCL failed; exclusive serial ownership unavailable: {exc}",
                        reason=PassiveCaptureRefuseReason.TIOCEXCL_FAILED,
                        requested_path=self._requested,
                        canonical_path=self._config.device,
                    ) from exc
            return ser

        try:
            self._ser = await asyncio.wait_for(
                asyncio.to_thread(_open),
                timeout=self._config.open_timeout_s,
            )
        except PassiveCaptureRefusedError:
            raise
        except Exception as exc:
            raise classify_open_failure(
                exc,
                requested_path=self._requested,
                canonical_path=self._config.device,
            ) from exc
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
