"""Documented CD2 allowed-nozzle numbers (Pump Interface Rev 2.11).

TRANS=0x02, LNG=N, NOZ1..NOZn (logical nozzle numbers 1..0x0F).
"""

from __future__ import annotations

from dataclasses import dataclass


class CD2Error(ValueError):
    """Invalid CD2 construction."""


@dataclass(frozen=True, slots=True)
class CD2AllowedNozzles:
    logical_nozzles: tuple[int, ...]
    application_payload: bytes
    source_reference: str = "Pump Interface Rev 2.11, page 13-14, CD2"

    @property
    def payload_hex(self) -> str:
        return self.application_payload.hex(" ")


def build_cd2_allowed_nozzles(logical_nozzles: list[int] | tuple[int, ...]) -> CD2AllowedNozzles:
    nozzles = tuple(int(n) for n in logical_nozzles)
    if not nozzles:
        raise CD2Error("at least one allowed logical nozzle required")
    if len(nozzles) != len(set(nozzles)):
        raise CD2Error("duplicate logical nozzle numbers are not allowed")
    for n in nozzles:
        if not 1 <= n <= 0x0F:
            raise CD2Error(f"logical nozzle out of range 1..15: {n}")
    payload = bytes((0x02, len(nozzles), *nozzles))
    return CD2AllowedNozzles(logical_nozzles=nozzles, application_payload=payload)
