"""Incremental observe-only per-address DART session tracker.

Maintains one session per legacy iGEM wire address (0x50 / 0x51). Correlates
POLL→DATA→ACK and EOT→DATA→ACK, resolves TRANS 01 only with direction
confidence, and gates state-machine eligibility. Never transmits.

Does not import tools/passive_dart_capture — concepts are reimplemented here
for the production protocol path.
"""

from __future__ import annotations

from intelipump_fdc.protocol.dart.application.constants import (
    MessageDirection,
    PumpControlCommand,
)
from intelipump_fdc.protocol.dart.application.nozio import decode_nozio
from intelipump_fdc.protocol.dart.line.addressing import is_legacy_igem_wire_address
from intelipump_fdc.protocol.dart.line.control import ControlType
from intelipump_fdc.protocol.dart.session.models import (
    CompletedExchange,
    DirectionConfidence,
    ExchangeRole,
    LinkPhase,
    NozioEdge,
    NozioEdgeEvent,
    ObservedLineEvent,
    ObserveTickResult,
    PendingData,
    PerAddressSessionSnapshot,
    RejectedAddressDiagnostic,
    SmAdvanceGate,
    Trans01Kind,
    Trans01Resolution,
)

# TODO(lab-validate): confirm minimum complete pump exchanges before
# SESSION_ACTIVE against longer passive captures; 2 matches "not after one".
MIN_COMPLETE_PUMP_EXCHANGES_FOR_ACTIVE = 2

_EOT_TYPES = frozenset({ControlType.EOT})


class AddressNotLegacyError(ValueError):
    """Wire address is not in the captured legacy iGEM profile."""


def resolve_trans01(
    *,
    direction: MessageDirection,
    confidence: DirectionConfidence,
    payload: bytes,
) -> Trans01Resolution | None:
    """Resolve TRANS 01 / LNG 1 when present; None if payload is not that shape."""
    if len(payload) < 3:
        return None
    # Application DATA body: TRANS LNG DATA...
    tid, lng = payload[0], payload[1]
    if tid != 0x01 or lng != 1 or len(payload) < 3:
        return None
    code = payload[2]
    if (
        direction is MessageDirection.SLAVE_TO_MASTER
        and confidence is DirectionConfidence.HIGH
    ):
        return Trans01Resolution(
            kind=Trans01Kind.DC1_STATUS,
            raw_code=code,
            direction=direction,
            confidence=confidence,
            reason=(
                "TRANS 01 LNG=1 on high-confidence PUMP_TO_CONTROLLER DATA"
            ),
        )
    if direction is MessageDirection.MASTER_TO_SLAVE:
        try:
            PumpControlCommand(code)
            known = True
        except ValueError:
            known = False
        return Trans01Resolution(
            kind=Trans01Kind.CD1_COMMAND,
            raw_code=code,
            direction=direction,
            confidence=confidence,
            reason=(
                "TRANS 01 LNG=1 on CONTROLLER_TO_PUMP DATA (CD1, not DC1)"
                + ("" if known else "; unknown DCC")
            ),
        )
    return Trans01Resolution(
        kind=Trans01Kind.AMBIGUOUS,
        raw_code=code,
        direction=MessageDirection.UNKNOWN,
        confidence=DirectionConfidence.LOW
        if confidence is DirectionConfidence.NONE
        else confidence,
        reason=(
            "TRANS 01 LNG=1 unresolved: direction confidence insufficient "
            "to choose DC1 vs CD1"
        ),
    )


def _extract_dc3_nozio_bytes(payload: bytes) -> list[int]:
    """Return NOZIO bytes from ordered DC3 (03 04 ....) transactions.

    Preserves multi-DC3 order. Skips malformed lengths. Does not invent fields.
    """
    out: list[int] = []
    i = 0
    while i + 2 <= len(payload):
        tid = payload[i]
        lng = payload[i + 1]
        data_start = i + 2
        data_end = data_start + lng
        if data_end > len(payload):
            break
        if tid == 0x03 and lng == 4:
            # PP PP PP NOZIO
            out.append(payload[data_start + 3])
        i = data_end
    return out


class PerAddressObserveSession:
    """Direction-aware observe session for one legacy wire address."""

    def __init__(self, wire_address: int) -> None:
        if not is_legacy_igem_wire_address(wire_address):
            raise AddressNotLegacyError(
                f"observe session requires legacy iGEM wire address "
                f"0x50/0x51; got 0x{wire_address:02X}"
            )
        self._s = PerAddressSessionSnapshot(wire_address=wire_address)

    @property
    def wire_address(self) -> int:
        return self._s.wire_address

    @property
    def snapshot(self) -> PerAddressSessionSnapshot:
        return self._s

    @property
    def link_phase(self) -> LinkPhase:
        return self._s.link_phase

    def ingest(self, event: ObservedLineEvent) -> ObserveTickResult:
        if event.wire_address != self._s.wire_address:
            raise ValueError(
                f"event address 0x{event.wire_address:02X} does not match "
                f"session 0x{self._s.wire_address:02X}"
            )
        self._touch(event)

        if not event.complete:
            return self._result(
                direction=MessageDirection.UNKNOWN,
                confidence=DirectionConfidence.NONE,
                inference_reason="incomplete frame; ignored for correlation",
                warnings=("incomplete frame ignored",),
            )

        ctype = event.control_type
        if ctype is ControlType.POLL:
            return self._on_poll(event)
        if ctype in _EOT_TYPES:
            return self._on_eot(event)
        if ctype is ControlType.DATA:
            return self._on_data(event)
        if ctype is ControlType.ACK:
            return self._on_ack(event)
        return self._result(
            direction=MessageDirection.UNKNOWN,
            confidence=DirectionConfidence.NONE,
            inference_reason=f"control type {ctype.value} has no observe rule",
        )

    def _touch(self, event: ObservedLineEvent) -> None:
        self._s.last_activity_frame_id = event.frame_id
        if event.monotonic_ns is not None:
            self._s.last_activity_monotonic_ns = event.monotonic_ns
        if self._s.link_phase is LinkPhase.DISCONNECTED:
            self._s.link_phase = LinkPhase.INITIALIZING
        elif (
            self._s.link_phase is LinkPhase.IDLE
            and self._s.complete_valid_pump_exchanges
            >= MIN_COMPLETE_PUMP_EXCHANGES_FOR_ACTIVE
        ):
            # Activity while idle: return to SESSION_ACTIVE once proven.
            self._s.link_phase = LinkPhase.SESSION_ACTIVE

    def _on_poll(self, event: ObservedLineEvent) -> ObserveTickResult:
        self._s.last_poll_frame_id = event.frame_id
        self._s.last_poll_sequence = event.sequence
        # A new POLL supersedes an un-ACKed pending DATA on this address.
        if self._s.pending is not None:
            self._s.diagnostic_notes.append(
                f"pending DATA frame_id={self._s.pending.data_frame_id} "
                "dropped on new POLL (incomplete exchange)"
            )
            self._s.pending = None
        return self._result(
            direction=MessageDirection.MASTER_TO_SLAVE,
            confidence=DirectionConfidence.HIGH,
            inference_reason="POLL is controller-originated",
        )

    def _on_eot(self, event: ObservedLineEvent) -> ObserveTickResult:
        self._s.last_eot_frame_id = event.frame_id
        return self._result(
            direction=MessageDirection.SLAVE_TO_MASTER,
            confidence=DirectionConfidence.MEDIUM,
            inference_reason="EOT/turnaround short control (pump or line turnaround)",
        )

    def _on_data(self, event: ObservedLineEvent) -> ObserveTickResult:
        if event.crc_valid is False:
            self._s.pending = None
            return self._result(
                direction=MessageDirection.UNKNOWN,
                confidence=DirectionConfidence.NONE,
                inference_reason="DATA CRC invalid; not eligible for SM",
                sm_gate=SmAdvanceGate(
                    may_advance_application_sm=False,
                    may_emit_dc1=False,
                    may_emit_nozio_edge=False,
                    last_seen_only=False,
                    reason="CRC invalid",
                ),
                warnings=("crc_invalid",),
            )

        role, direction, confidence, reason, preceding = self._infer_data_direction(
            event
        )
        self._s.pending = PendingData(
            role=role,
            direction=direction,
            confidence=confidence,
            data_frame_id=event.frame_id,
            data_sequence=event.sequence,
            crc_valid=bool(event.crc_valid),
            payload=bytes(event.payload),
            preceding_frame_id=preceding,
            inference_reason=reason,
        )
        # Buffer until ACK completes the exchange — do not advance SM yet.
        return self._result(
            direction=direction,
            confidence=confidence,
            inference_reason=reason + "; awaiting ACK to complete exchange",
            sm_gate=SmAdvanceGate(
                may_advance_application_sm=False,
                may_emit_dc1=False,
                may_emit_nozio_edge=False,
                last_seen_only=False,
                reason="exchange incomplete until ACK",
            ),
        )

    def _infer_data_direction(
        self, event: ObservedLineEvent
    ) -> tuple[
        ExchangeRole,
        MessageDirection,
        DirectionConfidence,
        str,
        int | None,
    ]:
        """Infer DATA direction from most recent same-address POLL or EOT."""
        del event  # address already validated; inference uses session memory
        poll_id = self._s.last_poll_frame_id
        eot_id = self._s.last_eot_frame_id
        if poll_id is not None and (eot_id is None or poll_id > eot_id):
            return (
                ExchangeRole.POLL_RESPONSE,
                MessageDirection.SLAVE_TO_MASTER,
                DirectionConfidence.HIGH,
                "DATA after same-address POLL (high-confidence pump response)",
                poll_id,
            )
        if eot_id is not None and (poll_id is None or eot_id > poll_id):
            return (
                ExchangeRole.AFTER_EOT,
                MessageDirection.MASTER_TO_SLAVE,
                DirectionConfidence.HIGH,
                "DATA after EOT/turnaround (candidate controller command)",
                eot_id,
            )
        return (
            ExchangeRole.UNKNOWN,
            MessageDirection.UNKNOWN,
            DirectionConfidence.LOW,
            "DATA without sufficient POLL/EOT sequence context",
            None,
        )

    def _on_ack(self, event: ObservedLineEvent) -> ObserveTickResult:
        pending = self._s.pending
        self._s.last_ack_frame_id = event.frame_id
        self._s.last_ack_sequence = event.sequence
        if pending is None:
            return self._result(
                direction=MessageDirection.UNKNOWN,
                confidence=DirectionConfidence.LOW,
                inference_reason="ACK without correlated preceding DATA on this address",
            )

        # ACK direction is opposite of DATA.
        if pending.direction is MessageDirection.SLAVE_TO_MASTER:
            ack_dir = MessageDirection.MASTER_TO_SLAVE
            ack_reason = "ACK after pump DATA; controller acknowledges"
            ack_conf = DirectionConfidence.HIGH
        elif pending.direction is MessageDirection.MASTER_TO_SLAVE:
            ack_dir = MessageDirection.SLAVE_TO_MASTER
            ack_reason = "ACK after controller DATA; pump acknowledges"
            ack_conf = DirectionConfidence.HIGH
        else:
            ack_dir = MessageDirection.UNKNOWN
            ack_reason = "ACK after DATA with unknown direction"
            ack_conf = DirectionConfidence.LOW

        self._s.pending = None
        exchange = CompletedExchange(
            role=pending.role,
            direction=pending.direction,
            confidence=pending.confidence,
            poll_or_eot_frame_id=pending.preceding_frame_id,
            data_frame_id=pending.data_frame_id,
            ack_frame_id=event.frame_id,
            data_sequence=pending.data_sequence,
            crc_valid=pending.crc_valid,
            payload=pending.payload,
        )

        pump_high = (
            exchange.direction is MessageDirection.SLAVE_TO_MASTER
            and exchange.confidence is DirectionConfidence.HIGH
            and exchange.crc_valid
        )
        just_activated = False
        if pump_high:
            self._s.complete_valid_pump_exchanges += 1
            just_activated = (
                self._s.link_phase is LinkPhase.INITIALIZING
                and self._s.complete_valid_pump_exchanges
                >= MIN_COMPLETE_PUMP_EXCHANGES_FOR_ACTIVE
            )
            self._maybe_promote_link_phase()

        trans01 = resolve_trans01(
            direction=exchange.direction,
            confidence=exchange.confidence,
            payload=exchange.payload,
        )
        nozio_edge: NozioEdgeEvent | None = None
        sm_gate: SmAdvanceGate

        if not pump_high:
            # Controller DATA or ambiguous: never DC1 / NOZIO for SM.
            last_seen_only = (
                trans01 is not None and trans01.kind is Trans01Kind.CD1_COMMAND
            )
            sm_gate = SmAdvanceGate(
                may_advance_application_sm=False,
                may_emit_dc1=False,
                may_emit_nozio_edge=False,
                last_seen_only=last_seen_only,
                reason=(
                    "not high-confidence CRC-valid PUMP_TO_CONTROLLER exchange"
                ),
            )
            if self._s.link_phase is LinkPhase.SESSION_ACTIVE:
                self._s.link_phase = LinkPhase.IDLE
            return self._result(
                direction=ack_dir,
                confidence=ack_conf,
                inference_reason=ack_reason,
                exchange_completed=exchange,
                trans01=trans01,
                sm_gate=sm_gate,
            )

        # High-confidence pump exchange: DC1 / NOZIO eligibility.
        may_dc1 = trans01 is not None and trans01.kind is Trans01Kind.DC1_STATUS
        if may_dc1 and trans01 is not None and trans01.raw_code is not None:
            prev = self._s.last_dc1_status
            self._s.last_dc1_status = trans01.raw_code
            # Same status → last_seen only (no SM lifecycle event).
            if prev is not None and prev == trans01.raw_code:
                may_dc1 = False
                sm_gate = SmAdvanceGate(
                    may_advance_application_sm=False,
                    may_emit_dc1=False,
                    may_emit_nozio_edge=False,
                    last_seen_only=True,
                    reason="DC1 same status; last_seen only",
                )
            else:
                sm_gate = SmAdvanceGate(
                    may_advance_application_sm=True,
                    may_emit_dc1=True,
                    may_emit_nozio_edge=False,
                    last_seen_only=False,
                    reason="complete valid POLL→DATA→ACK pump exchange with DC1",
                )
        else:
            sm_gate = SmAdvanceGate(
                may_advance_application_sm=False,
                may_emit_dc1=False,
                may_emit_nozio_edge=False,
                last_seen_only=False,
                reason="pump exchange complete; no new DC1 in payload",
            )

        # NOZIO edges from ordered DC3 on this exchange only.
        for nozio_raw in _extract_dc3_nozio_bytes(exchange.payload):
            decoded = decode_nozio(nozio_raw)
            prev_out = self._s.last_nozio_out
            self._s.last_nozio_raw = nozio_raw
            self._s.last_nozio_out = decoded.nozzle_out
            if prev_out is None:
                continue
            if prev_out is False and decoded.nozzle_out is True:
                nozio_edge = NozioEdgeEvent(
                    edge=NozioEdge.NOZZLE_OUT,
                    wire_address=self._s.wire_address,
                    previous_out=prev_out,
                    nozzle_out=True,
                    nozio_raw=nozio_raw,
                    frame_id=exchange.data_frame_id,
                )
            elif prev_out is True and decoded.nozzle_out is False:
                nozio_edge = NozioEdgeEvent(
                    edge=NozioEdge.NOZZLE_IN,
                    wire_address=self._s.wire_address,
                    previous_out=prev_out,
                    nozzle_out=False,
                    nozio_raw=nozio_raw,
                    frame_id=exchange.data_frame_id,
                )
            # Duplicate same position: suppress (no edge).

        if nozio_edge is not None:
            sm_gate = SmAdvanceGate(
                may_advance_application_sm=True,
                may_emit_dc1=sm_gate.may_emit_dc1,
                may_emit_nozio_edge=True,
                last_seen_only=False,
                reason=sm_gate.reason + "; NOZIO edge",
            )

        # Leave SESSION_ACTIVE visible on the promoting tick; later quiet
        # completes settle to IDLE (session proven, no pending work).
        if (
            not just_activated
            and self._s.link_phase is LinkPhase.SESSION_ACTIVE
        ):
            self._s.link_phase = LinkPhase.IDLE

        return self._result(
            direction=ack_dir,
            confidence=ack_conf,
            inference_reason=ack_reason,
            exchange_completed=exchange,
            trans01=trans01,
            nozio_edge=nozio_edge,
            sm_gate=sm_gate,
        )

    def _maybe_promote_link_phase(self) -> None:
        if self._s.link_phase is LinkPhase.INITIALIZING and (
            self._s.complete_valid_pump_exchanges
            >= MIN_COMPLETE_PUMP_EXCHANGES_FOR_ACTIVE
        ):
            self._s.link_phase = LinkPhase.SESSION_ACTIVE

    def _result(
        self,
        *,
        direction: MessageDirection,
        confidence: DirectionConfidence,
        inference_reason: str,
        exchange_completed: CompletedExchange | None = None,
        trans01: Trans01Resolution | None = None,
        nozio_edge: NozioEdgeEvent | None = None,
        sm_gate: SmAdvanceGate | None = None,
        warnings: tuple[str, ...] = (),
    ) -> ObserveTickResult:
        return ObserveTickResult(
            wire_address=self._s.wire_address,
            link_phase=self._s.link_phase,
            direction=direction,
            confidence=confidence,
            inference_reason=inference_reason,
            exchange_completed=exchange_completed,
            trans01=trans01,
            nozio_edge=nozio_edge,
            sm_gate=sm_gate,
            warnings=warnings,
        )


class ObserveSessionHub:
    """Routes line events to independent per-address sessions.

    Non-legacy addresses (0x94/0xA8/0xFC/0xD4 etc.) produce diagnostics only
    and never create or mutate a state-machine session.
    """

    def __init__(self) -> None:
        self._sessions: dict[int, PerAddressObserveSession] = {}
        self.rejected: list[RejectedAddressDiagnostic] = []

    def session(self, wire_address: int) -> PerAddressObserveSession | None:
        return self._sessions.get(wire_address)

    def ingest(self, event: ObservedLineEvent) -> ObserveTickResult:
        if not is_legacy_igem_wire_address(event.wire_address):
            diag = RejectedAddressDiagnostic(
                wire_address=event.wire_address,
                frame_id=event.frame_id,
                control_type=event.control_type,
                reason=(
                    "address excluded from observe SM sessions "
                    "(not legacy iGEM 0x50/0x51; likely frame-recovery artifact)"
                ),
            )
            self.rejected.append(diag)
            return ObserveTickResult(
                wire_address=event.wire_address,
                link_phase=None,
                direction=MessageDirection.UNKNOWN,
                confidence=DirectionConfidence.NONE,
                inference_reason=diag.reason,
                rejected=diag,
                sm_gate=SmAdvanceGate(
                    may_advance_application_sm=False,
                    may_emit_dc1=False,
                    may_emit_nozio_edge=False,
                    last_seen_only=False,
                    reason=diag.reason,
                ),
            )
        session = self._sessions.get(event.wire_address)
        if session is None:
            session = PerAddressObserveSession(event.wire_address)
            self._sessions[event.wire_address] = session
        return session.ingest(event)
