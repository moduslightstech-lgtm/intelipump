"""Software-only fault injection for LAB RS-485 bench (never electrical)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from intelipump_fdc.hardware.fault_injection import (
    corrupt_frame_byte,
    inject_noise,
    truncate_frame,
)


class BenchFaultKind(StrEnum):
    DELAYED_RESPONSE = "DELAYED_RESPONSE"
    DROPPED_RESPONSE = "DROPPED_RESPONSE"
    CORRUPTED_CRC = "CORRUPTED_CRC"
    DUPLICATE_DATA = "DUPLICATE_DATA"
    WRONG_SEQUENCE = "WRONG_SEQUENCE"
    NAK = "NAK"
    PARTIAL_FRAME = "PARTIAL_FRAME"
    INSERTED_NOISE = "INSERTED_NOISE"
    DLE_SF_SPLIT = "DLE_SF_SPLIT"
    SIMULATOR_RESTART = "SIMULATOR_RESTART"
    ONE_ADDRESS_OFFLINE = "ONE_ADDRESS_OFFLINE"
    ALL_ADDRESSES_OFFLINE = "ALL_ADDRESSES_OFFLINE"


@dataclass
class BenchFaultPlan:
    """Ordered software fault plan for a bench/simulator session."""

    kinds: list[BenchFaultKind] = field(default_factory=list)
    delay_ms: float = 50.0
    offline_addresses: tuple[int, ...] = ()
    remaining: dict[BenchFaultKind, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.remaining and self.kinds:
            for kind in self.kinds:
                self.remaining[kind] = self.remaining.get(kind, 0) + 1

    def take(self, kind: BenchFaultKind) -> bool:
        left = self.remaining.get(kind, 0)
        if left <= 0:
            return False
        self.remaining[kind] = left - 1
        return True


WriteFn = Callable[[bytes], Awaitable[Any]]


@dataclass
class SimulatorFaultInjector:
    """
    Transform simulator TX responses for LAB fault tests.

    Electrical fault injection is intentionally not supported.
    """

    plan: BenchFaultPlan = field(default_factory=BenchFaultPlan)
    applied: list[str] = field(default_factory=list)

    async def apply_responses(
        self,
        responses: list[bytes],
        *,
        write: WriteFn,
        drain: Callable[[], Awaitable[None]],
    ) -> None:
        for response in responses:
            await self._emit_one(response, write=write, drain=drain)

    async def _emit_one(
        self,
        response: bytes,
        *,
        write: WriteFn,
        drain: Callable[[], Awaitable[None]],
    ) -> None:
        # Apply at most one fault mode per response.
        if self.plan.take(BenchFaultKind.DROPPED_RESPONSE):
            self.applied.append(BenchFaultKind.DROPPED_RESPONSE.value)
            return
        if self.plan.take(BenchFaultKind.DELAYED_RESPONSE):
            self.applied.append(BenchFaultKind.DELAYED_RESPONSE.value)
            await asyncio.sleep(self.plan.delay_ms / 1000.0)

        payload = response
        if self.plan.take(BenchFaultKind.CORRUPTED_CRC):
            self.applied.append(BenchFaultKind.CORRUPTED_CRC.value)
            payload = corrupt_frame_byte(payload)
            await write(payload)
            await drain()
            return
        if self.plan.take(BenchFaultKind.INSERTED_NOISE):
            self.applied.append(BenchFaultKind.INSERTED_NOISE.value)
            payload = inject_noise(payload, noise_bytes=3)
            await write(payload)
            await drain()
            return
        if self.plan.take(BenchFaultKind.PARTIAL_FRAME):
            self.applied.append(BenchFaultKind.PARTIAL_FRAME.value)
            payload = truncate_frame(payload, keep=max(1, len(payload) // 2))
            await write(payload)
            await drain()
            return
        if self.plan.take(BenchFaultKind.DLE_SF_SPLIT):
            self.applied.append(BenchFaultKind.DLE_SF_SPLIT.value)
            mid = max(1, len(payload) // 2)
            await write(payload[:mid])
            await drain()
            await asyncio.sleep(0.005)
            await write(payload[mid:])
            await drain()
            return
        if self.plan.take(BenchFaultKind.DUPLICATE_DATA):
            self.applied.append(BenchFaultKind.DUPLICATE_DATA.value)
            await write(payload)
            await drain()
            await write(payload)
            await drain()
            return
        if self.plan.take(BenchFaultKind.WRONG_SEQUENCE) and len(payload) >= 2:
            self.applied.append(BenchFaultKind.WRONG_SEQUENCE.value)
            mutated = bytearray(payload)
            mutated[1] ^= 0x0F
            payload = bytes(mutated)
            await write(payload)
            await drain()
            return
        if self.plan.take(BenchFaultKind.NAK):
            self.applied.append(BenchFaultKind.NAK.value)
            if len(payload) >= 3:
                mutated = bytearray(payload)
                mutated[1] = (mutated[1] & 0xF0) | 0x05
                payload = bytes(mutated)
            else:
                payload = corrupt_frame_byte(payload)
            await write(payload)
            await drain()
            return
        await write(payload)
        await drain()


def split_dle_sf_chunks(frame: bytes) -> tuple[bytes, bytes]:
    """Split a frame so DLE/SF boundary can cross write calls."""
    if len(frame) < 2:
        return frame, b""
    mid = len(frame) // 2
    return frame[:mid], frame[mid:]


# Re-export pure helpers for callers/tests.
__all__ = [
    "BenchFaultKind",
    "BenchFaultPlan",
    "SimulatorFaultInjector",
    "corrupt_frame_byte",
    "inject_noise",
    "split_dle_sf_chunks",
    "truncate_frame",
]
