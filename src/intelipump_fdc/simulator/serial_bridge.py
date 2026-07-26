"""Bridge Phase-5 SimulatorSession to an async byte transport."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from intelipump_fdc.hardware.bench_faults import SimulatorFaultInjector
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)
from intelipump_fdc.protocol.dart.transport.base import ByteTransport
from intelipump_fdc.simulator.config import SimulatorConfig
from intelipump_fdc.simulator.session import SimulatorSession


@dataclass
class SerialBridgeConfig:
    read_chunk_size: int = 64
    # When the transport read already blocks on read_timeout_s, keep this at 0
    # so an empty read does not add a second sleep before the next POLL is seen.
    idle_sleep_ms: int = 0
    sim_time_step_ms: int = 50
    log_frames: bool = False
    cold_start_on_open: bool = True


class SimulatorSerialBridge:
    """Expose SimulatorSession over a ByteTransport (LAB virtual serial)."""

    def __init__(
        self,
        transport: ByteTransport,
        *,
        simulator: SimulatorSession | None = None,
        config: SerialBridgeConfig | None = None,
        fault_injector: SimulatorFaultInjector | None = None,
    ) -> None:
        self.transport = transport
        self.simulator = simulator or SimulatorSession(SimulatorConfig())
        self.config = config or SerialBridgeConfig()
        self.assembler = LegacyIgemStreamAssembler()
        self.fault_injector = fault_injector
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    def soft_restart(self) -> None:
        """Simulator-side restart without electrical unplug (LAB fault)."""
        self.assembler = LegacyIgemStreamAssembler()
        for pump in self.simulator.pumps.values():
            pump.communication_enabled = False
            pump.cold_start_to_ready()

    async def run(self) -> None:
        await self.transport.open()
        if self.config.cold_start_on_open:
            for pump in self.simulator.pumps.values():
                if not pump.communication_enabled:
                    pump.cold_start_to_ready()
        try:
            while not self._stop.is_set():
                chunk = await self.transport.read(self.config.read_chunk_size)
                if chunk:
                    if self.config.log_frames:
                        print(f"SIM RX {chunk.hex(' ')}")
                    for event in self.assembler.feed(chunk):
                        if (
                            event.kind is AssemblerEventKind.FRAME
                            and event.frame is not None
                        ):
                            await self._handle_frame(event.raw)
                        elif self.config.log_frames:
                            print(f"SIM reject {event.kind}: {event.raw.hex(' ')}")
                elif self.config.idle_sleep_ms > 0:
                    await asyncio.sleep(self.config.idle_sleep_ms / 1000)
                else:
                    # Transport already waited on read timeout; yield to peer tasks.
                    await asyncio.sleep(0)
                # Sim clock advance is independent of wire reply timing; responses
                # are written in _handle_frame before this tick.
                self.simulator.advance(self.config.sim_time_step_ms)
        finally:
            await self.transport.close()

    async def _handle_frame(self, raw: bytes) -> None:
        result = self.simulator.receive(raw)
        responses = list(result.responses)
        if self.fault_injector is not None:
            await self.fault_injector.apply_responses(
                responses,
                write=self.transport.write,
                drain=self.transport.drain,
            )
            return
        for response in responses:
            if self.config.log_frames:
                print(f"SIM TX {response.hex(' ')}")
            await self.transport.write(response)
            await self.transport.drain()
