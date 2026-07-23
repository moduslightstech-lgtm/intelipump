"""Simulator shared models and snapshots."""

from __future__ import annotations

from dataclasses import dataclass, field

from intelipump_fdc.domain.pump_command import PumpCommand
from intelipump_fdc.domain.pump_state import PumpState
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.simulator.faults import ProtocolFault


@dataclass(frozen=True, slots=True)
class OutboundApplicationTransaction:
    """Queued slave→master application payload (TRANS+LNG+DATA bytes)."""

    payload: bytes
    label: str


@dataclass(frozen=True, slots=True)
class PumpSnapshot:
    pump_id: str
    dart_address: int
    normalized_state: PumpState
    wayne_status: WaynePumpStatus
    selected_nozzle: int | None
    prices_raw: dict[int, int]
    price_verified: bool
    volume_raw: int
    amount_raw: int
    total_volume_raw: int
    total_amount_raw: int
    active_transaction_id: str | None
    tx_sequence: int
    expected_controller_sequence: int
    pending_outbound_count: int
    communication_enabled: bool
    fault_code: int | None
    last_command: PumpCommand | None
    nozzle_out: bool
    suspended: bool
    filling_active: bool
    clock_ms: int


@dataclass(frozen=True, slots=True)
class FrameExchangeResult:
    """Result of processing one inbound controller frame."""

    responses: tuple[bytes, ...]
    faults: tuple[ProtocolFault, ...] = ()
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SimulatorSnapshot:
    clock_ms: int
    pumps: tuple[PumpSnapshot, ...]
    faults: tuple[ProtocolFault, ...] = ()
    notes: tuple[str, ...] = field(default_factory=tuple)
