"""Exclusive serial transport for poll-bench (write allowed only for verified polls).

A permanent reader thread is the only code that calls ``serial.read()``. The
asyncio poll collector consumes :class:`SerialChunk` objects from that thread's
queue and never cancels an in-flight UART read.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from intelipump_fdc.bench_poll.serial_reader import PermanentSerialReader, SerialChunk
from intelipump_fdc.capture.port_guards import (
    PassiveCaptureRefusedError,
    PassiveCaptureRefuseReason,
    classify_open_failure,
    is_virtual_or_test_port,
    resolve_canonical_device,
    tiocexcl_request,
)
from intelipump_fdc.controller.price_safety import (
    ActiveFrameKind,
    RealWayneActiveCommandRefusedError,
    assert_real_wayne_write_allowed,
    classify_active_data_frame,
    is_verified_status_poll,
)
from intelipump_fdc.protocol.dart.transport.errors import TransportNotOpenError
from intelipump_fdc.protocol.dart.transport.serial import (
    SerialParity,
    _parity_constant,
)

logger = logging.getLogger(__name__)

# Short bounded OS read timeout — not the full poll response window.
DEFAULT_SERIAL_READ_TIMEOUT_S = 0.015


@runtime_checkable
class BenchByteTransport(Protocol):
    @property
    def is_open(self) -> bool: ...

    @property
    def device(self) -> str: ...

    async def open(self) -> None: ...

    async def close(self) -> None: ...

    async def write(self, data: bytes) -> int: ...

    async def get_chunk(self, timeout_s: float) -> SerialChunk | None: ...


@dataclass(frozen=True, slots=True)
class BenchPollSerialConfig:
    device: str
    baud_rate: int = 9600
    read_chunk_size: int = 256
    open_timeout_s: float = 2.0
    read_timeout_s: float = DEFAULT_SERIAL_READ_TIMEOUT_S
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

    Starts one :class:`PermanentSerialReader` for the session lifetime. Poll
    collectors must use :meth:`get_chunk` — never call pyserial ``read`` here.
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
        self._reader: PermanentSerialReader | None = None
        # Single-shot active DATA approval: exact frame bytes, at most one write.
        self._approved_active_frame: bytes | None = None
        self._active_writes_remaining: int = 0
        self._approved_kind: ActiveFrameKind | None = None
        self.active_write_count: int = 0
        # Per-kind counters for evidence.
        self.cd5_write_count: int = 0
        self.cd1_return_status_write_count: int = 0
        self.cd1_reset_write_count: int = 0
        self.cd1_authorize_write_count: int = 0
        self.cd2_reset_write_count: int = 0

    def authorize_single_active_write(
        self,
        frame: bytes,
        *,
        kind: ActiveFrameKind,
    ) -> None:
        """Allow exactly one future write of this exact active DATA frame."""
        if not frame:
            raise RealWayneActiveCommandRefusedError(
                "empty active frame cannot be approved"
            )
        if is_verified_status_poll(frame):
            raise RealWayneActiveCommandRefusedError(
                "status poll cannot be registered as active approval"
            )
        classified = classify_active_data_frame(frame)
        if classified is None or classified is not kind:
            raise RealWayneActiveCommandRefusedError(
                f"approved frame is not a {kind.value} DATA candidate"
            )
        self._approved_active_frame = bytes(frame)
        self._active_writes_remaining = 1
        self._approved_kind = kind

    def authorize_single_cd5_write(self, frame: bytes) -> None:
        """Allow exactly one future write of this exact CD5 DATA frame."""
        self.authorize_single_active_write(frame, kind=ActiveFrameKind.CD5_PRICE)

    def clear_cd5_write_authorization(self) -> None:
        self.clear_active_write_authorization()

    def clear_active_write_authorization(self) -> None:
        self._approved_active_frame = None
        self._active_writes_remaining = 0
        self._approved_kind = None

    @property
    def is_open(self) -> bool:
        return self._open and self._ser is not None

    @property
    def device(self) -> str:
        return self._config.device

    @property
    def reader(self) -> PermanentSerialReader | None:
        return self._reader

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
        assert self._ser is not None
        self._reader = PermanentSerialReader(
            self._ser,
            read_chunk_size=self._config.read_chunk_size,
        )
        self._reader.start()

    async def close(self) -> None:
        reader = self._reader
        self._reader = None
        if reader is not None:
            await asyncio.to_thread(reader.stop)
        ser = self._ser
        self._open = False
        self._ser = None
        if ser is not None:
            await asyncio.to_thread(ser.close)  # type: ignore[attr-defined]

    async def get_chunk(self, timeout_s: float) -> SerialChunk | None:
        """Consume one capture-timestamped chunk from the permanent reader."""
        if not self.is_open or self._reader is None:
            raise TransportNotOpenError("bench poll serial not open")
        reader = self._reader
        # Do not cancel the underlying UART read — only wait on the queue.
        return await asyncio.to_thread(reader.get, timeout_s)

    async def flush(self) -> None:
        """Flush TX so the POLL is fully on the wire before the response window."""
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("bench poll serial not open")
        await asyncio.to_thread(self._ser.flush)  # type: ignore[attr-defined]

    async def write(self, data: bytes) -> int:
        if not self.is_open or self._ser is None:
            raise TransportNotOpenError("bench poll serial not open")
        # Hard refusal: status poll, or one pre-approved active candidate only.
        assert_real_wayne_write_allowed(
            data,
            approved_active_frame=self._approved_active_frame,
            active_writes_remaining=self._active_writes_remaining,
        )
        is_active = (
            self._approved_active_frame is not None
            and data == self._approved_active_frame
            and self._active_writes_remaining > 0
        )
        if is_active:
            kind = self._approved_kind
            self._active_writes_remaining -= 1
            if self._active_writes_remaining <= 0:
                self._approved_active_frame = None
                self._approved_kind = None
            self.active_write_count += 1
            if kind is ActiveFrameKind.CD5_PRICE:
                self.cd5_write_count += 1
            elif kind is ActiveFrameKind.CD1_RETURN_STATUS:
                self.cd1_return_status_write_count += 1
            elif kind is ActiveFrameKind.CD1_RESET:
                self.cd1_reset_write_count += 1
            elif kind is ActiveFrameKind.CD1_AUTHORIZE:
                self.cd1_authorize_write_count += 1
            elif kind is ActiveFrameKind.CD2_AND_CD1_RESET:
                self.cd2_reset_write_count += 1
        self.write_count += 1
        written = await asyncio.to_thread(self._ser.write, data)  # type: ignore[attr-defined]
        return int(written or 0)
