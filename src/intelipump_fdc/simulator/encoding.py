"""Encode Wayne application transactions for the virtual simulator only.

Layouts follow Pump Interface Rev 2.11 as used by Phase 3 decoders.
This module does not transmit to real hardware.
"""

from __future__ import annotations

from intelipump_fdc.protocol.dart.application.bcd import encode_packed_bcd
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand


def _tx(trans: int, data: bytes) -> bytes:
    if len(data) > 255:
        raise ValueError("application DATA length exceeds 255")
    return bytes((trans, len(data))) + data


def encode_cd1_command(command: PumpControlCommand | int) -> bytes:
    code = int(command)
    return _tx(0x01, bytes((code,)))


def encode_dc1_status(status: int) -> bytes:
    return _tx(0x01, bytes((status & 0xFF,)))


def encode_dc2_volume_amount(*, volume_raw: int, amount_raw: int) -> bytes:
    return _tx(
        0x02,
        encode_packed_bcd(volume_raw, length=4) + encode_packed_bcd(amount_raw, length=4),
    )


def encode_dc3_nozzle_price(
    *,
    price_raw: int,
    logical_nozzle: int,
    nozzle_out: bool,
) -> bytes:
    nozio = (logical_nozzle & 0x0F) | (0x10 if nozzle_out else 0x00)
    return _tx(0x03, encode_packed_bcd(price_raw, length=3) + bytes((nozio,)))


def encode_cd3_preset_volume(volume_raw: int) -> bytes:
    return _tx(0x03, encode_packed_bcd(volume_raw, length=4))


def encode_cd4_preset_amount(amount_raw: int) -> bytes:
    return _tx(0x04, encode_packed_bcd(amount_raw, length=4))


def encode_cd5_price_update(
    *,
    logical_nozzle: int = 1,
    price_raw: int | None = None,
    prices_raw: list[int] | None = None,
) -> bytes:
    """Documented CD5: TRANS=0x05, LNG=3*N, each price 3-byte packed BCD.

    Single-nozzle: ``encode_cd5_price_update(logical_nozzle=1, price_raw=1175)``.
    Multi-nozzle: ``encode_cd5_price_update(prices_raw=[1175, 1175])`` (PRI order).
    """
    if prices_raw is not None:
        if price_raw is not None:
            raise ValueError("pass price_raw or prices_raw, not both")
        chunks = b"".join(encode_packed_bcd(p, length=3) for p in prices_raw)
        return _tx(0x05, chunks)
    if price_raw is None:
        raise ValueError("price_raw or prices_raw required")
    # Ordinal encoded by chunk position; single-price helper keeps nozzle arg
    # for call-site clarity even though the wire has no nozzle byte.
    del logical_nozzle
    return _tx(0x05, encode_packed_bcd(price_raw, length=3))


def encode_dc5_alarm(alarm_code: int) -> bytes:
    return _tx(0x05, bytes((alarm_code & 0xFF,)))


def encode_cd101_request_totals(counter_select: int = 1) -> bytes:
    return _tx(0x65, bytes((counter_select & 0xFF,)))


def encode_dc101_totals(
    *,
    counter_select: int,
    total_value_raw: int,
    total_meter1_raw: int,
    total_meter2_raw: int = 0,
) -> bytes:
    data = (
        bytes((counter_select & 0xFF,))
        + encode_packed_bcd(total_value_raw, length=5)
        + encode_packed_bcd(total_meter1_raw, length=5)
        + encode_packed_bcd(total_meter2_raw, length=5)
    )
    return _tx(0x65, data)


def encode_dc9_identity(*, identity: bytes = b"SIM-WP01") -> bytes:
    """Minimal identity stub (DC9). Layout not fully exercised in captures."""
    return _tx(0x09, identity[:16].ljust(8, b"\x00"))
