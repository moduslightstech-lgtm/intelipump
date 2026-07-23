"""Transport unit tests."""

from __future__ import annotations

import pytest

from intelipump_fdc.protocol.dart.transport.errors import TransportConfigError
from intelipump_fdc.protocol.dart.transport.memory import create_memory_transport_pair
from intelipump_fdc.protocol.dart.transport.serial import SerialConfig, SerialParity


@pytest.mark.asyncio
async def test_memory_transport_read_write() -> None:
    a, b = create_memory_transport_pair()
    async with a, b:
        assert a.is_open and b.is_open
        n = await a.write(b"\x01\x02\x03")
        assert n == 3
        await a.drain()
        got = await b.read(10)
        assert got == b"\x01\x02\x03"
    assert not a.is_open


@pytest.mark.asyncio
async def test_memory_transport_open_close() -> None:
    a, b = create_memory_transport_pair()
    await a.open()
    assert a.is_open
    await a.close()
    assert not a.is_open
    await b.open()
    await b.close()


def test_serial_config_validation() -> None:
    SerialConfig(device="/tmp/dart-controller").validate()
    with pytest.raises(TransportConfigError):
        SerialConfig(device="/tmp/x", baud_rate=115200).validate()
    with pytest.raises(TransportConfigError):
        SerialConfig(device="/tmp/x", parity=SerialParity.EVEN).validate()
    with pytest.raises(TransportConfigError):
        SerialConfig(device="/tmp/x", data_bits=7).validate()
    with pytest.raises(TransportConfigError):
        SerialConfig(device="/tmp/x", stop_bits=2).validate()
    with pytest.raises(TransportConfigError):
        SerialConfig(device="").validate()
