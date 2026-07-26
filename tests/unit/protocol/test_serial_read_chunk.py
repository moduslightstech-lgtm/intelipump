"""Unit tests for latency-safe serial read helper (no physical port)."""

from __future__ import annotations

from intelipump_fdc.protocol.dart.transport.serial import read_serial_chunk


class _FakeSerial:
    """Minimal pyserial stand-in for read_serial_chunk."""

    def __init__(self, initial: bytes = b"") -> None:
        self._buf = bytearray(initial)
        self.read_sizes: list[int] = []

    @property
    def in_waiting(self) -> int:
        return len(self._buf)

    def read(self, size: int = 1) -> bytes:
        self.read_sizes.append(size)
        if not self._buf:
            return b""
        take = bytes(self._buf[:size])
        del self._buf[:size]
        return take

    def arrive(self, data: bytes) -> None:
        self._buf.extend(data)


def test_read_serial_chunk_drains_waiting_without_oversized_block() -> None:
    ser = _FakeSerial(b"\x10\x02\x03")
    out = read_serial_chunk(ser, 256)
    assert out == b"\x10\x02\x03"
    # Drained via in_waiting — read(available), never read(256).
    assert ser.read_sizes == [3]


def test_read_serial_chunk_first_byte_then_drain() -> None:
    class _EmptyThenData(_FakeSerial):
        def __init__(self) -> None:
            super().__init__()
            self._pending = b"\xaa\xbb\xcc"

        @property
        def in_waiting(self) -> int:
            return 0 if self._pending else len(self._buf)

        def read(self, size: int = 1) -> bytes:
            if self._pending:
                self._buf.extend(self._pending)
                self._pending = b""
            return super().read(size)

    ser = _EmptyThenData()
    out = read_serial_chunk(ser, 64)
    assert out == b"\xaa\xbb\xcc"
    assert ser.read_sizes[0] == 1
    assert sum(ser.read_sizes) == 1 + 2  # read(1) then drain remaining 2


def test_read_serial_chunk_idle_returns_empty() -> None:
    ser = _FakeSerial()
    assert read_serial_chunk(ser, 64) == b""
    assert ser.read_sizes == [1]
