"""Exclusive serial transport for poll-bench (write allowed only for verified polls)."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

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

logger = logging.getLogger(__name__)


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
    write_timeout_s: float = 2.0
    requested_path: str | None = None
    apply_tiocexcl: bool = True


def format_serial_config(snapshot: dict[str, Any]) -> str:
    keys = (
        "baudrate",
        "bytesize",
        "parity",
        "stopbits",
        "timeout",
        "write_timeout",
        "xonxoff",
        "rtscts",
        "dsrdtr",
        "inter_byte_timeout",
    )
    parts = [f"{k}={snapshot.get(k)!r}" for k in keys]
    return " ".join(parts)


class BenchPollSerialTransport:
    """pyserial transport with mandatory exclusive open; write is for POLL only.

    Only one reader task may consume the port. In-flight UART bytes from a
    cancelled ``read`` await are parked and returned on the next ``read`` so
    ``asyncio.wait_for`` cannot silently drop response bytes.
    """

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
            write_timeout_s=config.write_timeout_s,
            requested_path=self._requested,
            apply_tiocexcl=config.apply_tiocexcl,
        )
        self._ser: object | None = None
        self._open = False
        self.write_count = 0
        self._rx_park = bytearray()
        self._read_lock = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        return self._open and self._ser is not None

    @property
    def device(self) -> str:
        return self._config.device

    def serial_config_snapshot(self) -> dict[str, Any]:
        """Effective pyserial settings for evidence logging."""
        ser = self._ser
        if ser is None:
            return {
                "baudrate": self._config.baud_rate,
                "bytesize": 8,
                "parity": "ODD",
                "stopbits": 1,
                "timeout": self._config.read_timeout_s,
                "write_timeout": self._config.write_timeout_s,
                "xonxoff": False,
                "rtscts": False,
                "dsrdtr": False,
                "inter_byte_timeout": None,
                "open": False,
            }
        return {
            "baudrate": getattr(ser, "baudrate", self._config.baud_rate),
            "bytesize": getattr(ser, "bytesize", 8),
            "parity": getattr(ser, "parity", None),
            "stopbits": getattr(ser, "stopbits", 1),
            "timeout": getattr(ser, "timeout", self._config.read_timeout_s),
            "write_timeout": getattr(
                ser, "write_timeout", self._config.write_timeout_s
            ),
            "xonxoff": getattr(ser, "xonxoff", False),
            "rtscts": getattr(ser, "rtscts", False),
            "dsrdtr": getattr(ser, "dsrdtr", False),
            "inter_byte_timeout": getattr(ser, "inter_byte_timeout", None),
            "open": True,
        }

    def device_path_exists(self) -> bool:
        if is_virtual_or_test_port(self._config.device):
            return True
        from pathlib import Path

        return Path(self._config.device).exists()

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
                "write_timeout": self._config.write_timeout_s,
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
        self._rx_park.clear()

    async def close(self) -> None:
        ser = self._ser
        self._open = False
        self._ser = None
        self._rx_park.clear()
        if ser is not None:
            await asyncio.to_thread(ser.close)  # type: ignore[attr-defined]

    async def read(self, max_bytes: int) -> bytes:
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("bench poll serial not open")
        # Single-reader lock: only one task may consume the UART at a time.
        # Keep the lock until any in-flight worker finishes so wait_for cancel
        # cannot start a second concurrent pyserial read.
        async with self._read_lock:
            if self._rx_park:
                n = min(max_bytes, len(self._rx_park))
                out = bytes(self._rx_park[:n])
                del self._rx_park[:n]
                return out
            n = min(max_bytes, self._config.read_chunk_size)
            loop = asyncio.get_running_loop()
            fut = loop.run_in_executor(None, read_serial_chunk, self._ser, n)
            try:
                return await fut
            except asyncio.CancelledError:
                data = await asyncio.shield(fut)
                if data:
                    self._rx_park.extend(data)
                    logger.debug(
                        "parked %d in-flight serial byte(s) after cancel",
                        len(data),
                    )
                raise

    async def flush(self) -> None:
        """Flush TX so the POLL is fully on the wire before the response window."""
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("bench poll serial not open")
        await asyncio.to_thread(self._ser.flush)  # type: ignore[attr-defined]

    async def write(self, data: bytes) -> int:
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("bench poll serial not open")
        self.write_count += 1
        written = await asyncio.to_thread(self._ser.write, data)  # type: ignore[attr-defined]
        return int(written or 0)
