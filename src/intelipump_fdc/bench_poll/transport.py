"""Exclusive serial transport for poll-bench (write allowed only for verified polls)."""

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
from intelipump_fdc.protocol.dart.transport.errors import TransportNotOpenError
from intelipump_fdc.protocol.dart.transport.serial import (
    SerialParity,
    _parity_constant,
    read_serial_chunk,
)


@runtime_checkable
class BenchByteTransport(Protocol):
    @property
    def is_open(self) -> bool: ...

    @property
    def device(self) -> str: ...

    async def open(self) -> None: ...

    async def close(self) -> None: ...

    async def read(self, max_bytes: int) -> bytes: ...

    async def write(self, data: bytes) -> int: ...


@dataclass(frozen=True, slots=True)
class BenchPollSerialConfig:
    device: str
    baud_rate: int = 9600
    read_chunk_size: int = 256
    open_timeout_s: float = 2.0
    read_timeout_s: float = 0.05
    requested_path: str | None = None
    apply_tiocexcl: bool = True


class BenchPollSerialTransport:
    """pyserial transport with mandatory exclusive open; write is for POLL only."""

    def __init__(self, config: BenchPollSerialConfig) -> None:
        self._requested = config.requested_path or config.device
        if is_virtual_or_test_port(config.device):
            canonical = config.device
        else:
            canonical = resolve_canonical_device(config.device)
        self._config = BenchPollSerialConfig(
            device=canonical,
            baud_rate=config.baud_rate,
            read_chunk_size=config.read_chunk_size,
            open_timeout_s=config.open_timeout_s,
            read_timeout_s=config.read_timeout_s,
            requested_path=self._requested,
            apply_tiocexcl=config.apply_tiocexcl,
        )
        self._ser: object | None = None
        self._open = False
        self.write_count = 0

    @property
    def is_open(self) -> bool:
        return self._open and self._ser is not None

    @property
    def device(self) -> str:
        return self._config.device

    async def open(self) -> None:
        if self._open:
            return

        def _open() -> object:
            import serial  # type: ignore[import-untyped]

            serial_exc = getattr(serial, "SerialException", OSError)
            kwargs: dict[str, object] = {
                "port": self._config.device,
                "baudrate": self._config.baud_rate,
                "bytesize": 8,
                "parity": _parity_constant(SerialParity.ODD),
                "stopbits": 1,
                "timeout": self._config.read_timeout_s,
                "write_timeout": 2.0,
                "xonxoff": False,
                "rtscts": False,
                "dsrdtr": False,
                "exclusive": True,
            }
            try:
                ser = serial.Serial(**kwargs)
            except TypeError as exc:
                raise PassiveCaptureRefusedError(
                    "pyserial exclusive=True unsupported; refusing shared open",
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
            if self._config.apply_tiocexcl:
                try:
                    tiocexcl_request(int(ser.fileno()))
                except PassiveCaptureRefusedError:
                    ser.close()
                    raise
                except Exception as exc:
                    ser.close()
                    raise PassiveCaptureRefusedError(
                        f"TIOCEXCL failed: {exc}",
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
            raise TransportNotOpenError("bench poll serial not open")
        n = min(max_bytes, self._config.read_chunk_size)
        return await asyncio.to_thread(read_serial_chunk, self._ser, n)

    async def write(self, data: bytes) -> int:
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("bench poll serial not open")
        self.write_count += 1
        written = await asyncio.to_thread(self._ser.write, data)  # type: ignore[attr-defined]
        return int(written or 0)
