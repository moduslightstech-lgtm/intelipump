"""Abstract async byte transport (independent of pyserial)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from types import TracebackType
from typing import Self


@dataclass(frozen=True, slots=True)
class TransportMetadata:
    name: str
    device: str | None = None
    baud_rate: int | None = None
    parity: str | None = None
    data_bits: int | None = None
    stop_bits: int | None = None
    notes: tuple[str, ...] = ()
    # memory | serial_virtual | serial_physical | unknown
    kind: str = "unknown"

    @property
    def is_virtual_or_memory(self) -> bool:
        return self.kind in {"memory", "serial_virtual"}


class ByteTransport(ABC):
    """Async byte pipe used by the DART controller and simulator bridge."""

    @abstractmethod
    async def open(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def read(self, max_bytes: int) -> bytes:
        """Read up to ``max_bytes`` (may return fewer or empty)."""

    @abstractmethod
    async def write(self, data: bytes) -> int:
        """Write all ``data``; return number of bytes accepted."""

    @abstractmethod
    async def drain(self) -> None:
        """Wait until buffered output is flushed (no-op if unsupported)."""

    @property
    @abstractmethod
    def is_open(self) -> bool:
        ...

    @property
    @abstractmethod
    def metadata(self) -> TransportMetadata:
        ...

    async def __aenter__(self) -> Self:
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        del exc_type, exc, tb
        await self.close()
