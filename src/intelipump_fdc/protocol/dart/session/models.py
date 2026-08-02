"""Observe-only DART per-address protocol session models.

Pure models for direction-aware exchange correlation. No serial I/O, no
frame construction, no state-machine transitions, no active commands.

Derived from lab-003/004 ePump passive learning: advance eligibility only
from correlated, validated pump responses — never from ambiguous TRANS bytes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from intelipump_fdc.protocol.dart.application.constants import MessageDirection
from intelipump_fdc.protocol.dart.line.control import ControlType


class LinkPhase(StrEnum):
    """Explicit observe-session link lifecycle (not application pump state).

    DISCONNECTED → INITIALIZING → SESSION_ACTIVE → IDLE

    Do not declare SESSION_ACTIVE after a single response.
    """

    DISCONNECTED = "DISCONNECTED"
    INITIALIZING = "INITIALIZING"
    SESSION_ACTIVE = "SESSION_ACTIVE"
    IDLE = "IDLE"


class DirectionConfidence(StrEnum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    NONE = "NONE"


class ExchangeRole(StrEnum):
    """What the pending DATA is expected to be, based on preceding control."""

    POLL_RESPONSE = "POLL_RESPONSE"  # POLL → DATA → ACK
    AFTER_EOT = "AFTER_EOT"  # EOT → DATA → ACK (often controller DATA)
    UNKNOWN = "UNKNOWN"


class Trans01Kind(StrEnum):
    """Direction-resolved interpretation of TRANS 01 / LNG 1."""

    DC1_STATUS = "DC1_STATUS"
    CD1_COMMAND = "CD1_COMMAND"
    AMBIGUOUS = "AMBIGUOUS"


class NozioEdge(StrEnum):
    """Authoritative NOZIO position edges only (01↔11 style IN/OUT)."""

    NOZZLE_OUT = "NOZZLE_OUT"  # IN → OUT
    NOZZLE_IN = "NOZZLE_IN"  # OUT → IN


@dataclass(frozen=True, slots=True)
class ObservedLineEvent:
    """One complete line-level frame observation for a single wire address.

    Callers supply assembled frames (from transport or offline capture).
    This module never builds or transmits frames.
    """

    wire_address: int
    control_type: ControlType
    sequence: int
    frame_id: int
    complete: bool = True
    crc_valid: bool | None = None
    # Application payload bytes for DATA frames (unescaped body after CTRL).
    payload: bytes = b""
    # Optional monotonic / wall markers for last-activity tracking.
    monotonic_ns: int | None = None


@dataclass(frozen=True, slots=True)
class Trans01Resolution:
    kind: Trans01Kind
    raw_code: int | None
    direction: MessageDirection
    confidence: DirectionConfidence
    reason: str


@dataclass(frozen=True, slots=True)
class SmAdvanceGate:
    """Whether validated observation content may feed the pump state machine.

    Line ACK completion is required for exchange validation; ACK itself is
    never application acceptance.
    """

    may_advance_application_sm: bool
    may_emit_dc1: bool
    may_emit_nozio_edge: bool
    last_seen_only: bool
    reason: str


@dataclass(frozen=True, slots=True)
class CompletedExchange:
    """One correlated POLL→DATA→ACK or EOT→DATA→ACK exchange."""

    role: ExchangeRole
    direction: MessageDirection
    confidence: DirectionConfidence
    poll_or_eot_frame_id: int | None
    data_frame_id: int
    ack_frame_id: int
    data_sequence: int
    crc_valid: bool
    payload: bytes


@dataclass(frozen=True, slots=True)
class NozioEdgeEvent:
    edge: NozioEdge
    wire_address: int
    previous_out: bool
    nozzle_out: bool
    nozio_raw: int
    frame_id: int


@dataclass(frozen=True, slots=True)
class RejectedAddressDiagnostic:
    """Corrupt / recovery-artifact address — diagnostics only, never SM."""

    wire_address: int
    frame_id: int
    control_type: ControlType
    reason: str


@dataclass(frozen=True, slots=True)
class ObserveTickResult:
    """Outcome of ingesting one line event into the observe session hub."""

    wire_address: int
    link_phase: LinkPhase | None
    direction: MessageDirection
    confidence: DirectionConfidence
    inference_reason: str
    exchange_completed: CompletedExchange | None = None
    trans01: Trans01Resolution | None = None
    nozio_edge: NozioEdgeEvent | None = None
    sm_gate: SmAdvanceGate | None = None
    rejected: RejectedAddressDiagnostic | None = None
    warnings: tuple[str, ...] = ()


@dataclass
class PendingData:
    """DATA awaiting a same-address ACK to complete the exchange."""

    role: ExchangeRole
    direction: MessageDirection
    confidence: DirectionConfidence
    data_frame_id: int
    data_sequence: int
    crc_valid: bool
    payload: bytes
    preceding_frame_id: int | None
    inference_reason: str


@dataclass
class PerAddressSessionSnapshot:
    """Mutable observe-session fields for one legacy iGEM wire address."""

    wire_address: int
    link_phase: LinkPhase = LinkPhase.DISCONNECTED
    last_poll_frame_id: int | None = None
    last_poll_sequence: int | None = None
    last_eot_frame_id: int | None = None
    last_ack_frame_id: int | None = None
    last_ack_sequence: int | None = None
    last_dc1_status: int | None = None
    last_nozio_raw: int | None = None
    last_nozio_out: bool | None = None
    pending: PendingData | None = None
    complete_valid_pump_exchanges: int = 0
    last_activity_frame_id: int | None = None
    last_activity_monotonic_ns: int | None = None
    timeout_count: int = 0
    diagnostic_notes: list[str] = field(default_factory=list)
