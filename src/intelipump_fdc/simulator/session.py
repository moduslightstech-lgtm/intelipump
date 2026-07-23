"""Abstract byte-facing DART simulator session (no real serial I/O)."""

from __future__ import annotations

from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.line.frame_builder import (
    build_ack,
    build_data_frame,
    build_eot,
    build_nak,
)
from intelipump_fdc.protocol.dart.line.frame_parser import parse_frame
from intelipump_fdc.protocol.dart.line.models import DartLineFrame, ParseError
from intelipump_fdc.simulator.clock import SimulatedClock
from intelipump_fdc.simulator.config import SimulatorConfig, next_sequence
from intelipump_fdc.simulator.faults import ProtocolFault, ProtocolFaultKind
from intelipump_fdc.simulator.models import FrameExchangeResult, SimulatorSnapshot
from intelipump_fdc.simulator.pump import SimulatedPump


class SimulatorSession:
    """Multi-position virtual dispenser on an abstract byte interface.

    Controllers call :meth:`receive` with complete wire frames and get zero or
    more response frames. This session never opens OS serial devices or PTYs.
    """

    def __init__(self, config: SimulatorConfig | None = None) -> None:
        self.config = config or SimulatorConfig()
        self.clock = SimulatedClock()
        self.pumps: dict[int, SimulatedPump] = {
            p.dart_address: SimulatedPump(
                config=p,
                clock=self.clock,
                simulator_config=self.config,
            )
            for p in self.config.pumps
        }
        self.faults: list[ProtocolFault] = []
        self.notes: list[str] = []

    def get_pump(self, address: int) -> SimulatedPump:
        return self.pumps[address]

    def pump_by_id(self, pump_id: str) -> SimulatedPump:
        for pump in self.pumps.values():
            if pump.config.pump_id == pump_id:
                return pump
        raise KeyError(pump_id)

    def advance(self, milliseconds: int) -> None:
        """Advance simulated time and filling engines; check response timeouts."""
        self.clock.advance(milliseconds)
        for pump in self.pumps.values():
            pump.on_time_advance()
            self._check_response_timeout(pump)

    def run_until_idle(self, *, max_ms: int = 1_000_000) -> int:
        return self.clock.run_until_idle(max_ms=max_ms)

    def snapshot(self) -> SimulatorSnapshot:
        return SimulatorSnapshot(
            clock_ms=self.clock.now(),
            pumps=tuple(p.snapshot() for p in self.pumps.values()),
            faults=tuple(self.faults),
            notes=tuple(self.notes),
        )

    def receive(self, frame_bytes: bytes) -> FrameExchangeResult:
        """Parse one controller frame and return simulator responses."""
        parsed = parse_frame(frame_bytes)
        if isinstance(parsed, ParseError):
            fault = ProtocolFault(
                ProtocolFaultKind.MALFORMED_FRAME,
                parsed.message,
                raw_frame_hex=frame_bytes.hex(" "),
                at_ms=self.clock.now(),
            )
            self.faults.append(fault)
            return FrameExchangeResult(responses=(), faults=(fault,))

        pump = self.pumps.get(parsed.address)
        if pump is None:
            fault = ProtocolFault(
                ProtocolFaultKind.UNKNOWN_ADDRESS,
                f"no simulated pump at address {parsed.address}",
                address=parsed.address,
                raw_frame_hex=frame_bytes.hex(" "),
                at_ms=self.clock.now(),
            )
            self.faults.append(fault)
            return FrameExchangeResult(responses=(), faults=(fault,))

        if not pump.communication_enabled:
            note = f"address {parsed.address} communication disabled; no response"
            self.notes.append(note)
            return FrameExchangeResult(responses=(), notes=(note,))

        if parsed.control_type is ControlType.POLL:
            return self._on_poll(pump)
        if parsed.control_type is ControlType.DATA:
            return self._on_data(pump, parsed)
        if parsed.control_type is ControlType.ACK:
            return self._on_ack(pump, parsed)
        if parsed.control_type is ControlType.NAK:
            return self._on_nak(pump, parsed)
        if parsed.control_type is ControlType.IAP:
            pump.reset_protocol_sequences()
            note = "IAP: protocol sequence restart"
            self.notes.append(note)
            return FrameExchangeResult(responses=(), notes=(note,))

        note = f"ignored control type {parsed.control_type}"
        self.notes.append(note)
        return FrameExchangeResult(responses=(), notes=(note,))

    def _on_poll(self, pump: SimulatedPump) -> FrameExchangeResult:
        if pump.awaiting_ack_sequence is not None and pump.last_outbound_wire is not None:
            return FrameExchangeResult(
                responses=(pump.last_outbound_wire,),
                notes=("POLL→DATA retransmit awaiting ACK",),
            )
        if not pump.has_pending():
            eot = build_eot(pump.config.dart_address, sequence=pump.tx_sequence)
            return FrameExchangeResult(responses=(eot,), notes=("POLL→EOT",))
        payload = pump.pop_pending_payload()
        assert payload is not None
        seq = pump.tx_sequence
        wire = build_data_frame(pump.config.dart_address, seq, payload)
        pump.awaiting_ack_sequence = seq
        pump.data_pending_since_ms = self.clock.now()
        pump.last_outbound_wire = wire
        return FrameExchangeResult(responses=(wire,), notes=(f"POLL→DATA seq={seq}",))

    def _on_data(self, pump: SimulatedPump, frame: DartLineFrame) -> FrameExchangeResult:
        faults: list[ProtocolFault] = []
        if frame.crc_valid is False:
            fault = ProtocolFault(
                ProtocolFaultKind.INVALID_CRC,
                "invalid CRC; no application response",
                address=pump.config.dart_address,
                sequence=frame.sequence,
                raw_frame_hex=frame.raw_frame.hex(" "),
                at_ms=self.clock.now(),
            )
            self.faults.append(fault)
            return FrameExchangeResult(responses=(), faults=(fault,))

        if (
            pump.last_accepted_controller_sequence is not None
            and frame.sequence == pump.last_accepted_controller_sequence
        ):
            fault = ProtocolFault(
                ProtocolFaultKind.DUPLICATE_DATA,
                f"duplicate DATA seq={frame.sequence}; ACK without reprocess",
                address=pump.config.dart_address,
                sequence=frame.sequence,
                at_ms=self.clock.now(),
            )
            self.faults.append(fault)
            ack = build_ack(pump.config.dart_address, frame.sequence)
            return FrameExchangeResult(
                responses=(ack,), faults=(fault,), notes=("dup DATA",)
            )

        if frame.sequence != pump.expected_controller_sequence:
            fault = ProtocolFault(
                ProtocolFaultKind.UNEXPECTED_SEQUENCE,
                f"expected seq={pump.expected_controller_sequence} got {frame.sequence}",
                address=pump.config.dart_address,
                sequence=frame.sequence,
                at_ms=self.clock.now(),
            )
            self.faults.append(fault)
            nak = build_nak(pump.config.dart_address, frame.sequence)
            return FrameExchangeResult(
                responses=(nak,), faults=(fault,), notes=("DATA→NAK",)
            )

        app_faults = pump.handle_application_payload(frame.payload)
        for f in app_faults:
            self.faults.append(f)
        faults.extend(app_faults)

        pump.last_accepted_controller_sequence = frame.sequence
        pump.expected_controller_sequence = next_sequence(
            frame.sequence, self.config.sequence_policy
        )
        ack = build_ack(pump.config.dart_address, frame.sequence)
        notes = ["DATA→ACK"]
        if app_faults:
            notes.append("application faults recorded; ACK still sent for valid CRC/seq")
        return FrameExchangeResult(
            responses=(ack,), faults=tuple(faults), notes=tuple(notes)
        )

    def _on_ack(self, pump: SimulatedPump, frame: DartLineFrame) -> FrameExchangeResult:
        if pump.awaiting_ack_sequence is None:
            return FrameExchangeResult(responses=(), notes=("spurious ACK ignored",))
        if frame.sequence != pump.awaiting_ack_sequence:
            fault = ProtocolFault(
                ProtocolFaultKind.UNEXPECTED_SEQUENCE,
                f"ACK seq {frame.sequence} != awaiting {pump.awaiting_ack_sequence}",
                address=pump.config.dart_address,
                sequence=frame.sequence,
                at_ms=self.clock.now(),
            )
            self.faults.append(fault)
            return FrameExchangeResult(responses=(), faults=(fault,))
        pump.tx_sequence = next_sequence(pump.tx_sequence, self.config.sequence_policy)
        pump.awaiting_ack_sequence = None
        pump.data_pending_since_ms = None
        pump.last_outbound_wire = None
        return FrameExchangeResult(responses=(), notes=("ACK accepted; TX# advanced",))

    def _on_nak(self, pump: SimulatedPump, frame: DartLineFrame) -> FrameExchangeResult:
        del pump
        note = f"NAK seq={frame.sequence}; will retransmit on next POLL"
        self.notes.append(note)
        return FrameExchangeResult(responses=(), notes=(note,))

    def _check_response_timeout(self, pump: SimulatedPump) -> None:
        if pump.awaiting_ack_sequence is None or pump.data_pending_since_ms is None:
            return
        elapsed = self.clock.now() - pump.data_pending_since_ms
        if elapsed >= self.config.response_timeout_ms:
            fault = ProtocolFault(
                ProtocolFaultKind.RESPONSE_TIMEOUT,
                f"no ACK within {self.config.response_timeout_ms} ms",
                address=pump.config.dart_address,
                sequence=pump.awaiting_ack_sequence,
                at_ms=self.clock.now(),
            )
            self.faults.append(fault)
            # Avoid repeating the same timeout fault every tick.
            pump.data_pending_since_ms = self.clock.now()
