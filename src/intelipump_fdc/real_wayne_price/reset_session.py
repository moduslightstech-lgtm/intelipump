"""Technician-supervised real-Wayne CD1 RESET (single-shot)."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from intelipump_fdc.bench_poll.poll_io import (
    StatusPollOutcome,
    send_status_poll_and_read_response,
)
from intelipump_fdc.bench_poll.transport import BenchByteTransport
from intelipump_fdc.controller.price_safety import (
    ActiveFrameKind,
    RealWayneActiveCommandRefusedError,
)
from intelipump_fdc.protocol.cd1 import (
    CD1Error,
    build_cd1_candidate_frame,
    build_cd1_command,
)
from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.control import ControlType, classify_control
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.protocol.dart.line.models import DartLineFrame
from intelipump_fdc.protocol.sequence import WayneSequenceManager
from intelipump_fdc.real_wayne_price.evidence import (
    ActiveWriteEvidenceBundle,
    default_reset_uncertainties,
    new_session_id,
    software_commit,
    write_active_write_evidence,
)
from intelipump_fdc.real_wayne_price.guards import ResetWriteParams
from intelipump_fdc.real_wayne_price.reset_decision import (
    controller_profile_for_reset,
    evaluate_reset_nozzle_gate,
    owned_lab_nozio_unknown_override_confirmed,
)
from intelipump_fdc.real_wayne_price.session_helpers import (
    drain_pending_rx,
    sequence_stale_status_hint,
    wait_for_ack_frame,
)
from intelipump_fdc.real_wayne_price.states import (
    ResetDiagnosticResult,
    ResetWriteState,
)
from intelipump_fdc.real_wayne_price.status_decode import (
    DecodedStatusSnapshot,
    StatusPreconditionError,
    decode_status_frame,
    validate_post_reset_status,
    validate_reset_preconditions,
)


@dataclass
class ResetWriteResult:
    state: ResetWriteState
    summary: dict[str, Any]
    evidence_paths: dict[str, Path]
    transmitted: bool = False
    serial_write_called_for_candidate: bool = False


def _nozio_raw_from_snap(snap: DecodedStatusSnapshot) -> int | None:
    ev = snap.nozio_evidence
    if isinstance(ev, dict):
        raw_hex = ev.get("nozioRawHex")
        if isinstance(raw_hex, str) and raw_hex:
            try:
                return int(raw_hex, 16) & 0xFF
            except ValueError:
                return None
    return None


def _observed_nak(ack_rx_hex: list[str], *, wire_address: int) -> bool:
    for item in ack_rx_hex:
        try:
            raw = bytes.fromhex(item)
        except ValueError:
            continue
        if (
            len(raw) == 3
            and raw[0] == wire_address
            and classify_control(raw[1]) is ControlType.NAK
        ):
            return True
    return False


def map_reset_diagnostic_result(
    *,
    transmitted: bool,
    ack_outcome: str,
    dc1_before: int | None,
    dc1_after: int | None,
    protocol_error: bool,
) -> ResetDiagnosticResult:
    if not transmitted:
        if protocol_error:
            return ResetDiagnosticResult.RESET_DIAGNOSTIC_PROTOCOL_ERROR
        return ResetDiagnosticResult.RESET_DIAGNOSTIC_REFUSED
    if ack_outcome == "NAK":
        return ResetDiagnosticResult.RESET_DIAGNOSTIC_NAK
    if ack_outcome == "ACK_TIMEOUT":
        return ResetDiagnosticResult.RESET_DIAGNOSTIC_TIMEOUT
    if ack_outcome == "ACK_MATCH":
        if (
            dc1_before is not None
            and dc1_after is not None
            and dc1_after != dc1_before
        ):
            return ResetDiagnosticResult.RESET_DIAGNOSTIC_ACK_STATE_CHANGED
        return ResetDiagnosticResult.RESET_DIAGNOSTIC_ACK_STATE_UNCHANGED
    if protocol_error:
        return ResetDiagnosticResult.RESET_DIAGNOSTIC_PROTOCOL_ERROR
    return ResetDiagnosticResult.RESET_DIAGNOSTIC_PROTOCOL_ERROR


class ResetWriteSession:
    """Poll FILLING_COMPLETE → nozzle gate → CD1 RESET → ACK → observe."""

    def __init__(
        self,
        transport: BenchByteTransport,
        params: ResetWriteParams,
        *,
        canonical_port: str | None = None,
    ) -> None:
        self.transport = transport
        self.params = params
        self.canonical_port = canonical_port or params.port
        self.logical_address = params.address
        self.wire_address = encode_wire_address(params.address)
        self.session_id = new_session_id().replace("price-dry-run", "cd1-reset")
        self.commit = software_commit()

    async def _observe_post_reset(
        self,
        *,
        dc1_before: int | None,
        settle_ms: int,
        observation_seconds: float,
        max_attempts: int,
        verification_required: bool,
    ) -> tuple[DecodedStatusSnapshot | None, str, int, list[str]]:
        """Fresh status polls only; never reuse the pre-TX snapshot as after."""
        notes: list[str] = []
        last_snap: DecodedStatusSnapshot | None = None
        last_frame: DartLineFrame | None = None
        status_rx_after = ""
        polls = 0
        deadline = time.monotonic() + max(0.0, observation_seconds)
        first = True
        while True:
            if first and settle_ms > 0:
                await asyncio.sleep(settle_ms / 1000.0)
            first = False
            response = await send_status_poll_and_read_response(
                self.transport,
                self.logical_address,
                self.params.response_timeout_ms,
            )
            polls += 1
            if response.outcome is not StatusPollOutcome.DATA_RESPONSE:
                notes.append(f"observe{polls}:{response.outcome.value}")
            else:
                assert response.frame is not None
                snap = decode_status_frame(
                    response.frame, expected_wire_address=self.wire_address
                )
                last_snap = snap
                last_frame = response.frame
                status_rx_after = response.frame.raw_frame.hex(" ")
                notes.append(
                    f"observe{polls}:dc1={snap.dc1_name}/{snap.dc1_code}"
                    f" before={dc1_before}"
                )
                if (
                    verification_required
                    and snap.dc1_code == int(WaynePumpStatus.RESET)
                    and snap.crc_valid
                ):
                    return last_snap, status_rx_after, polls, notes
            now = time.monotonic()
            if observation_seconds <= 0:
                break
            if now >= deadline:
                break
            if polls >= max_attempts:
                break
            await asyncio.sleep(min(0.05, max(0.0, deadline - now)))

        if verification_required:
            if last_snap is None:
                raise StatusPreconditionError(
                    "post_reset_RESET: no DATA status response",
                    reasons=["post_reset_RESET_no_data", *notes],
                    poll_count=polls,
                )
            raise StatusPreconditionError(
                "post_reset_RESET: status not reached "
                f"(last={last_snap.dc1_name}/{last_snap.dc1_code})",
                reasons=[
                    "post_reset_RESET_not_reached",
                    f"last_dc1={last_snap.dc1_name}/{last_snap.dc1_code}",
                    *notes,
                ],
                last_snap=last_snap,
                last_frame=last_frame,
                poll_count=polls,
            )
        return last_snap, status_rx_after, polls, notes

    async def run(self) -> ResetWriteResult:
        refusal_reasons: list[str] = []
        state = ResetWriteState.REFUSED
        diagnostic = ResetDiagnosticResult.RESET_DIAGNOSTIC_REFUSED
        decoded_before: dict[str, Any] = {}
        decoded_after: dict[str, Any] = {}
        status_tx = build_poll(self.logical_address).hex(" ")
        status_rx_before = ""
        status_rx_after = ""
        payload_hex = ""
        candidate_hex = ""
        crc_hex = ""
        expected_ack = ""
        ack_rx_hex: list[str] = []
        ack_outcome = "NOT_ATTEMPTED"
        serial_cfg: dict[str, Any] = {}
        transmitted = False
        serial_write_called = False
        poll_writes = 0
        active_writes = 0
        warnings: list[str] = []
        protocol_error = False
        dc1_before: int | None = None
        dc1_after: int | None = None
        nozzle_physical: str | None = None
        nozzle_decision: str | None = None
        control_byte_hex = ""
        encoded_sequence: int | None = None
        pre_write_drained: list[str] = []
        write_monotonic_s: float | None = None
        ack_monotonic_s: float | None = None
        ack_latency_ms: float | None = None
        stale_ack_rejected: list[str] = []

        try:
            if not self.transport.is_open:
                await self.transport.open()
            snapshot = getattr(self.transport, "serial_config_snapshot", None)
            if callable(snapshot):
                serial_cfg = snapshot()

            response = await send_status_poll_and_read_response(
                self.transport,
                self.logical_address,
                self.params.response_timeout_ms,
            )
            poll_writes = 1
            status_tx = response.poll_tx.hex(" ")
            if response.outcome is not StatusPollOutcome.DATA_RESPONSE:
                raise StatusPreconditionError(
                    f"status poll outcome={response.outcome.value}",
                    reasons=[f"status_outcome_{response.outcome.value}"],
                )
            assert response.frame is not None
            status_rx_before = response.frame.raw_frame.hex(" ")
            snap = decode_status_frame(
                response.frame, expected_wire_address=self.wire_address
            )
            dc1_before = snap.dc1_code
            validate_reset_preconditions(
                snap, expected_wire_address=self.wire_address
            )
            state = ResetWriteState.FILLING_COMPLETE

            nozio_raw = _nozio_raw_from_snap(snap)
            if nozio_raw is None:
                raise StatusPreconditionError(
                    "NOZIO byte missing from status frame",
                    reasons=["nozio_missing"],
                )
            profile = controller_profile_for_reset(self.params)
            gate = evaluate_reset_nozzle_gate(
                dc1_code=snap.dc1_code,
                nozio=nozio_raw,
                controller_profile=profile,
                owned_lab_override_confirmed=owned_lab_nozio_unknown_override_confirmed(
                    self.params
                ),
            )
            nozzle_physical = gate.nozzle_state.value
            nozzle_decision = gate.decision.value
            decoded_before = snap.to_report_dict()
            decoded_before.setdefault("dc3", {})
            if isinstance(decoded_before.get("dc3"), dict):
                decoded_before["dc3"] = {
                    **decoded_before["dc3"],
                    "physicalState": gate.nozzle_state.value,
                    "controllerProfileSupportsNozioOutBit": (
                        profile.supports_nozio_out_bit
                    ),
                    "resetNozzleDecision": gate.decision.value,
                }
            if gate.warning:
                warnings.append(gate.warning)
            if not gate.allow:
                raise StatusPreconditionError(
                    f"reset nozzle gate refused: {gate.refusal_reason}",
                    reasons=[gate.refusal_reason or "nozzle_gate_refused"],
                )

            cd1 = build_cd1_command(PumpControlCommand.RESET)
            frame, crc, ack = build_cd1_candidate_frame(
                logical_address=self.logical_address,
                sequence=self.params.sequence,
                cd1=cd1,
            )
            payload_hex = cd1.payload_hex
            candidate_hex = frame.hex(" ")
            crc_hex = f"{crc:04X}"
            expected_ack = ack.hex(" ")
            encoded_sequence = int(self.params.sequence) & 0x0F
            control_byte_hex = (
                f"{WayneSequenceManager.message_byte(encoded_sequence):02x}"
            )
            state = ResetWriteState.RESET_BLOCK_BUILT

            authorize = getattr(self.transport, "authorize_single_active_write", None)
            if not callable(authorize):
                raise StatusPreconditionError(
                    "transport cannot authorize CD1 RESET",
                    reasons=["transport_missing_active_authorization"],
                )
            authorize(frame, kind=ActiveFrameKind.CD1_RESET)
            state = ResetWriteState.RESET_AUTHORIZED_FOR_SINGLE_WRITE

            # Discard stale RX (e.g. leftover 50 c0 fa) before the single TX.
            pre_write_drained = await drain_pending_rx(
                self.transport, quiet_ms=20.0
            )
            if pre_write_drained:
                warnings.append(
                    "pre_write_rx_drained:"
                    + ",".join(pre_write_drained[:8])
                )

            write_monotonic_s = time.monotonic()
            await self.transport.write(frame)
            flush = getattr(self.transport, "flush", None)
            if callable(flush):
                await flush()
            transmitted = True
            serial_write_called = True
            active_writes = int(getattr(self.transport, "cd1_reset_write_count", 1))
            state = ResetWriteState.RESET_TRANSMITTED

            ack_result = await wait_for_ack_frame(
                self.transport,
                expected_ack=ack,
                timeout_ms=self.params.ack_timeout_ms,
                not_before_monotonic_s=write_monotonic_s,
            )
            matched = ack_result.matched
            ack_outcome = ack_result.outcome
            ack_rx_hex = list(ack_result.observed_hex)
            ack_monotonic_s = ack_result.ack_monotonic_s
            ack_latency_ms = ack_result.ack_latency_ms
            stale_ack_rejected = list(ack_result.stale_rejected_hex)
            if stale_ack_rejected:
                warnings.append(
                    "stale_ack_rejected:"
                    + ",".join(stale_ack_rejected[:8])
                )
            if matched:
                state = ResetWriteState.ACK_RECEIVED
            elif _observed_nak(ack_rx_hex, wire_address=self.wire_address):
                ack_outcome = "NAK"
                state = ResetWriteState.FAULT
                warnings.append("NAK observed; stopping without retry")
            else:
                state = ResetWriteState.ACK_TIMEOUT
                warnings.append(
                    "ACK not observed within timeout; continuing to status observe"
                )

            if ack_outcome != "NAK":
                verification = (
                    self.params.confirmations.post_write_status_verification_required
                )
                snap_after, status_rx_after, extra_polls, notes = (
                    await self._observe_post_reset(
                        dc1_before=dc1_before,
                        settle_ms=self.params.post_write_settle_ms,
                        observation_seconds=self.params.post_reset_observation_seconds,
                        max_attempts=self.params.post_write_max_status_polls,
                        verification_required=verification,
                    )
                )
                poll_writes += extra_polls
                warnings.extend(notes)
                if snap_after is not None:
                    decoded_after = snap_after.to_report_dict()
                    dc1_after = snap_after.dc1_code
                if verification:
                    assert snap_after is not None
                    validate_post_reset_status(
                        snap_after, expected_wire_address=self.wire_address
                    )
                    state = ResetWriteState.RESET_VERIFIED
                elif not verification:
                    warnings.append("post-write status verification was not required")

        except (StatusPreconditionError, CD1Error, RealWayneActiveCommandRefusedError) as exc:
            refusal_reasons = list(getattr(exc, "reasons", [str(exc)]))
            state = ResetWriteState.FAULT if transmitted else ResetWriteState.REFUSED
            if isinstance(exc, CD1Error):
                protocol_error = True
            last_snap = getattr(exc, "last_snap", None)
            last_frame = getattr(exc, "last_frame", None)
            extra_polls = int(getattr(exc, "poll_count", 0) or 0)
            if extra_polls:
                poll_writes += extra_polls
            if last_snap is not None:
                decoded_after = last_snap.to_report_dict()
                dc1_after = last_snap.dc1_code
            if last_frame is not None:
                status_rx_after = last_frame.raw_frame.hex(" ")
            hint = sequence_stale_status_hint(
                sequence=self.params.sequence,
                ack_outcome=ack_outcome,
                refusal_reasons=refusal_reasons,
            )
            if hint:
                warnings.append(hint)
            clear = getattr(self.transport, "clear_active_write_authorization", None)
            if callable(clear):
                clear()
        except Exception as exc:  # pragma: no cover
            refusal_reasons = [str(exc)]
            state = ResetWriteState.FAULT
            protocol_error = True
            clear = getattr(self.transport, "clear_active_write_authorization", None)
            if callable(clear):
                clear()

        diagnostic = map_reset_diagnostic_result(
            transmitted=transmitted,
            ack_outcome=ack_outcome,
            dc1_before=dc1_before,
            dc1_after=dc1_after,
            protocol_error=protocol_error,
        )
        if state is ResetWriteState.RESET_VERIFIED:
            diagnostic = ResetDiagnosticResult.RESET_DIAGNOSTIC_ACK_STATE_CHANGED

        expected_after = {
            "code": int(WaynePumpStatus.RESET),
            "name": "RESET",
            "basis": "DART_DOCUMENTED_STATE_FLOW",
            "wayneEnum": WaynePumpStatus.RESET.name,
        }
        bundle = ActiveWriteEvidenceBundle(
            session_id=self.session_id,
            commit=self.commit,
            target_type=self.params.target_type,
            command_name="RESET",
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            serial_config=serial_cfg,
            status_poll_tx_hex=status_tx,
            status_response_before_hex=status_rx_before,
            status_response_after_hex=status_rx_after,
            decoded_status_before=decoded_before,
            decoded_status_after=decoded_after,
            confirmations=self.params.confirmations.to_dict(),
            candidate_payload_hex=payload_hex,
            candidate_frame_hex=candidate_hex,
            crc_hex=crc_hex,
            sequence=self.params.sequence,
            expected_ack_hex=expected_ack,
            ack_outcome=ack_outcome,
            ack_observed_hex=ack_rx_hex,
            expected_status_after=expected_after,
            write_state=state.value,
            transmitted=transmitted,
            serial_write_called_for_candidate=serial_write_called,
            poll_write_count=poll_writes,
            active_write_count=active_writes,
            remaining_uncertainties=default_reset_uncertainties(),
            refusal_reasons=refusal_reasons,
            warnings=warnings,
            encoded_sequence=encoded_sequence,
            control_byte_hex=control_byte_hex,
            pre_write_drained_hex=pre_write_drained,
            write_monotonic_s=write_monotonic_s,
            ack_monotonic_s=ack_monotonic_s,
            ack_latency_ms=ack_latency_ms,
            stale_ack_rejected_hex=stale_ack_rejected,
        )
        paths = write_active_write_evidence(
            self.params.evidence_dir, bundle, stem="cd1-reset-write"
        )

        try:
            if self.transport.is_open:
                await self.transport.close()
        except Exception:
            pass

        summary = {
            "sessionId": self.session_id,
            "command": "RESET",
            "state": state.value,
            "diagnosticResult": diagnostic.value,
            "transmitted": transmitted,
            "serialWriteCalledForCandidate": serial_write_called,
            "activeWriteCount": active_writes,
            "pollWriteCount": poll_writes,
            "logicalAddress": self.logical_address,
            "wireAddress": f"0x{self.wire_address:02X}",
            "decodedStatusBefore": decoded_before,
            "decodedStatusAfter": decoded_after,
            "candidatePayloadHex": payload_hex,
            "candidateFrameHex": candidate_hex,
            "crc": crc_hex,
            "sequence": self.params.sequence,
            "encodedSequence": encoded_sequence,
            "controlByteHex": control_byte_hex,
            "expectedAckHypothesis": expected_ack,
            "ackOutcome": ack_outcome,
            "ackObservedHex": ack_rx_hex,
            "writeMonotonicS": write_monotonic_s,
            "ackMonotonicS": ack_monotonic_s,
            "ackLatencyMs": ack_latency_ms,
            "preWriteDrainedHex": pre_write_drained,
            "staleAckRejectedHex": stale_ack_rejected,
            "expectedStatusAfter": expected_after,
            "nozzlePhysicalState": nozzle_physical,
            "resetNozzleDecision": nozzle_decision,
            "postResetObservationSeconds": self.params.post_reset_observation_seconds,
            "refusalReasons": refusal_reasons,
            "warnings": warnings,
            "evidence": {k: str(v) for k, v in paths.items()},
            "softwareCommit": self.commit,
            "targetType": self.params.target_type,
            "timestampUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

        # Extend evidence review JSON with diagnostic fields.
        review_path = paths.get("review")
        if review_path is not None and review_path.exists():
            try:
                review_obj = json.loads(review_path.read_text(encoding="utf-8"))
                review_obj["diagnosticResult"] = diagnostic.value
                review_obj["nozzlePhysicalState"] = nozzle_physical
                review_obj["resetNozzleDecision"] = nozzle_decision
                review_path.write_text(
                    json.dumps(review_obj, indent=2) + "\n", encoding="utf-8"
                )
            except Exception:
                pass

        return ResetWriteResult(
            state=state,
            summary=summary,
            evidence_paths=paths,
            transmitted=transmitted,
            serial_write_called_for_candidate=serial_write_called,
        )
