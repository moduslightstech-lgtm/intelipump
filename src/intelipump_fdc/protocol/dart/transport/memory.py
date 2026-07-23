"""In-memory paired byte transports for LAB tests (no serial hardware)."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

from intelipump_fdc.protocol.dart.transport.base import ByteTransport, TransportMetadata
from intelipump_fdc.protocol.dart.transport.errors import TransportNotOpenError


@dataclass
class _Pipe:
    queue: asyncio.Queue[bytes | None]
    closed: bool = False


class MemoryTransport(ByteTransport):
    """One end of an in-memory full-duplex byte pipe."""

    def __init__(
        self,
        *,
        name: str,
        inbound: _Pipe,
        outbound: _Pipe,
    ) -> None:
        self._name = name
        self._inbound = inbound
        self._outbound = outbound
        self._open = False
        self._read_buf = bytearray()

    @property
    def is_open(self) -> bool:
        return self._open

    @property
    def metadata(self) -> TransportMetadata:
        return TransportMetadata(
            name=self._name,
            device="memory",
            notes=("in-memory LAB transport",),
            kind="memory",
        )

    async def open(self) -> None:
        self._open = True

    async def close(self) -> None:
        self._open = False
        self._outbound.closed = True
        # Wake peer readers.
        with contextlib.suppress(asyncio.QueueFull):
            self._outbound.queue.put_nowait(None)

    async def read(self, max_bytes: int) -> bytes:
        if not self._open:
            raise TransportNotOpenError("memory transport not open")
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        if self._read_buf:
            out = bytes(self._read_buf[:max_bytes])
            del self._read_buf[:max_bytes]
            return out
        try:
            item = self._inbound.queue.get_nowait()
        except asyncio.QueueEmpty:
            try:
                item = await asyncio.wait_for(self._inbound.queue.get(), timeout=0.01)
            except TimeoutError:
                return b""
        if item is None:
            return b""
        if len(item) <= max_bytes:
            return item
        self._read_buf.extend(item[max_bytes:])
        return item[:max_bytes]

    async def write(self, data: bytes) -> int:
        if not self._open:
            raise TransportNotOpenError("memory transport not open")
        if self._outbound.closed:
            raise TransportNotOpenError("peer closed")
        await self._outbound.queue.put(bytes(data))
        return len(data)

    async def drain(self) -> None:
        return None


def create_memory_transport_pair(
    *,
    left_name: str = "memory-a",
    right_name: str = "memory-b",
) -> tuple[MemoryTransport, MemoryTransport]:
    """Create two connected memory transports (A.write → B.read, B.write → A.read)."""
    a_to_b = _Pipe(queue=asyncio.Queue())
    b_to_a = _Pipe(queue=asyncio.Queue())
    left = MemoryTransport(name=left_name, inbound=b_to_a, outbound=a_to_b)
    right = MemoryTransport(name=right_name, inbound=a_to_b, outbound=b_to_a)
    return left, right
