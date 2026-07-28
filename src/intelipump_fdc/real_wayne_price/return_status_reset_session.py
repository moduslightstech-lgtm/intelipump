"""Technician-supervised real-Wayne CD1 RETURN_STATUS then RESET."""

from __future__ import annotations

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
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.real_wayne_price.evidence import (
    ActiveWriteEvidenceBundle,
    default_return_status_reset_uncertainties,
    new_session_id,
    software_commit,
    write_active_write_evidence,
)
from intelipump_fdc.real_wayne_price.guards import ReturnStatusResetWriteParams
from intelipump_fdc.real_wayne_price.session_helpers import (
    dc1_is,
    next_sequence_nibble,
    poll_status_until,
    sequence_stale_status_hint,
    wait_for_ack_frame,
)
from intelipump_fdc.real_wayne_price.states import ReturnStatusResetWriteState
from intelipump_fdc.real_wayne_price.status_decode import (
    StatusPreconditionError,
    decode_status_frame,
    validate_post_reset_status,
    validate_reset_preconditions,
)


@dataclass
class ReturnStatusResetWriteResult:
    state: ReturnStatusResetWriteState
    summary: dict[str, Any]
    evidence_paths: dict[str, Path]
    transmitted: bool = False
    serial_write_called_for_candidate: bool = False


class ReturnStatusResetWriteSession:
    """Office-capture path: RETURN_STATUS → poll → RESET → verify DC1 RESET.

    Exactly two active DATA writes (each single-shot authorized). Sequences:
    ``--sequence N`` for RETURN_STATUS, ``N+1`` for RESET.
    """

    def __init__(
        self,
        transport: BenchByteTransport,
        params: ReturnStatusResetWriteParams,
        *,
        canonical_port: str | None = None,
    ) -> None:
        self.transport = transport
        self.params = params
        self.canonical_port = canonical_port or params.port
        self.logical_address = params.address
        self.wire_address = encode_wire_address(params.address)
        self.session_id = new_session_id().replace(
            "price-dry-run", "cd1-return-status-reset"
        )
        self.commit = software_commit()

    async def run(self) -> ReturnStatusResetWriteResult:
        refusal_reasons: list[str] = []
        state = ReturnStatusResetWriteState.REFUSED
        decoded_before: dict[str, Any] = {}
        decoded_mid: dict[str, Any] = {}
        decoded_after: dict[str, Any] = {}
        status_tx = build_poll(self.logical_address).hex(" ")
        status_rx_before = ""
        status_rx_mid = ""
        status_rx_after = ""
        rs_payload = ""
        rs_frame_hex = ""
        rs_ack_outcome = "NOT_ATTEMPTED"
        rs_ack_observed: list[str] = []
        reset_payload = ""
        reset_frame_hex = ""
        reset_crc = ""
        reset_expected_ack = ""
        reset_ack_outcome = "NOT_ATTEMPTED"
        reset_ack_observed: list[str] = []
        reset_seq = next_sequence_nibble(self.params.sequence)
        serial_cfg: dict[str, Any] = {}
        transmitted = False
        serial_write_called = False
        poll_writes = 0
        active_writes = 0
        warnings: list[str] = []

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
            decoded_before = snap.to_report_dict()
            validate_reset_preconditions(
                snap, expected_wire_address=self.wire_address
            )
            if snap.nozzle_out is False:
                warnings.append(
                    "initial status reports nozzle IN; office captures used OUT"
                )
            state = ReturnStatusResetWriteState.FILLING_COMPLETE

            authorize = getattr(self.transport, "authorize_single_active_write", None)
            if not callable(authorize):
                raise StatusPreconditionError(
                    "transport cannot authorize active CD1 writes",
                    reasons=["transport_missing_active_authorization"],
                )
            flush = getattr(self.transport, "flush", None)

            # --- Step 1: CD1 RETURN_STATUS ---
            rs_cmd = build_cd1_command(PumpControlCommand.RETURN_STATUS)
            rs_frame, _, rs_ack = build_cd1_candidate_frame(
                logical_address=self.logical_address,
                sequence=self.params.sequence,
                cd1=rs_cmd,
            )
            rs_payload = rs_cmd.payload_hex
            rs_frame_hex = rs_frame.hex(" ")
            authorize(rs_frame, kind=ActiveFrameKind.CD1_RETURN_STATUS)
            await self.transport.write(rs_frame)
            if callable(flush):
                await flush()
            transmitted = True
            serial_write_called = True
            active_writes = int(
                getattr(self.transport, "cd1_return_status_write_count", 1)
            )
            state = ReturnStatusResetWriteState.RETURN_STATUS_TRANSMITTED

            matched, rs_ack_outcome, rs_ack_observed = await wait_for_ack_frame(
                self.transport,
                expected_ack=rs_ack,
                timeout_ms=self.params.ack_timeout_ms,
            )
            if matched:
                state = ReturnStatusResetWriteState.RETURN_STATUS_ACK
            else:
                warnings.append(
                    "RETURN_STATUS ACK not observed; continuing mid-status poll"
                )

            # Mid status poll (mirrors office controller after RETURN_STATUS).
            mid = await send_status_poll_and_read_response(
                self.transport,
                self.logical_address,
                self.params.response_timeout_ms,
            )
            poll_writes += 1
            if mid.outcome is StatusPollOutcome.DATA_RESPONSE and mid.frame is not None:
                status_rx_mid = mid.frame.raw_frame.hex(" ")
                mid_snap = decode_status_frame(
                    mid.frame, expected_wire_address=self.wire_address
                )
                decoded_mid = mid_snap.to_report_dict()
                validate_reset_preconditions(
                    mid_snap, expected_wire_address=self.wire_address
                )
                if mid_snap.nozzle_out is False:
                    warnings.append(
                        "mid status after RETURN_STATUS reports nozzle IN"
                    )
                state = ReturnStatusResetWriteState.MID_STATUS_POLLED
            else:
                warnings.append(
                    f"mid status poll outcome={mid.outcome.value}; "
                    "continuing to RESET"
                )

            # --- Step 2: CD1 RESET (next sequence) ---
            reset_cmd = build_cd1_command(PumpControlCommand.RESET)
            reset_frame, crc, reset_ack = build_cd1_candidate_frame(
                logical_address=self.logical_address,
                sequence=reset_seq,
                cd1=reset_cmd,
            )
            reset_payload = reset_cmd.payload_hex
            reset_frame_hex = reset_frame.hex(" ")
            reset_crc = f"{crc:04X}"
            reset_expected_ack = reset_ack.hex(" ")
            authorize(reset_frame, kind=ActiveFrameKind.CD1_RESET)
            await self.transport.write(reset_frame)
            if callable(flush):
                await flush()
            active_writes = int(getattr(self.transport, "active_write_count", 2))
            state = ReturnStatusResetWriteState.RESET_TRANSMITTED

            matched, reset_ack_outcome, reset_ack_observed = await wait_for_ack_frame(
                self.transport,
                expected_ack=reset_ack,
                timeout_ms=self.params.ack_timeout_ms,
            )
            if matched:
                state = ReturnStatusResetWriteState.RESET_ACK
            else:
                warnings.append(
                    "RESET ACK not observed within timeout; continuing verify"
                )

            if self.params.confirmations.post_write_status_verification_required:
                snap_after, frame_after, extra_polls, notes = await poll_status_until(
                    self.transport,
                    logical_address=self.logical_address,
                    wire_address=self.wire_address,
                    response_timeout_ms=self.params.response_timeout_ms,
                    predicate=dc1_is(WaynePumpStatus.RESET),
                    settle_ms=self.params.post_write_settle_ms,
                    max_attempts=self.params.post_write_max_status_polls,
                    failure_label="post_return_status_reset_RESET",
                )
                poll_writes += extra_polls
                warnings.extend(notes)
                status_rx_after = frame_after.raw_frame.hex(" ")
                decoded_after = snap_after.to_report_dict()
                validate_post_reset_status(
                    snap_after, expected_wire_address=self.wire_address
                )
                state = ReturnStatusResetWriteState.RESET_VERIFIED
            else:
                warnings.append("post-write status verification was not required")

        except (StatusPreconditionError, CD1Error, RealWayneActiveCommandRefusedError) as exc:
            refusal_reasons = list(getattr(exc, "reasons", [str(exc)]))
            state = (
                ReturnStatusResetWriteState.FAULT
                if transmitted
                else ReturnStatusResetWriteState.REFUSED
            )
            last_snap = getattr(exc, "last_snap", None)
            last_frame = getattr(exc, "last_frame", None)
            extra_polls = int(getattr(exc, "poll_count", 0) or 0)
            if extra_polls:
                poll_writes += extra_polls
            if last_snap is not None:
                decoded_after = last_snap.to_report_dict()
            if last_frame is not None:
                status_rx_after = last_frame.raw_frame.hex(" ")
            hint = sequence_stale_status_hint(
                sequence=reset_seq,
                ack_outcome=reset_ack_outcome,
                refusal_reasons=refusal_reasons,
            )
            if hint:
                warnings.append(hint)
            clear = getattr(self.transport, "clear_active_write_authorization", None)
            if callable(clear):
                clear()
        except Exception as exc:  # pragma: no cover
            refusal_reasons = [str(exc)]
            state = ReturnStatusResetWriteState.FAULT
            clear = getattr(self.transport, "clear_active_write_authorization", None)
            if callable(clear):
                clear()

        expected_after = {
            "code": int(WaynePumpStatus.RESET),
            "name": "RESET",
            "basis": "OFFICE_CAPTURE_RETURN_STATUS_THEN_RESET",
            "wayneEnum": WaynePumpStatus.RESET.name,
        }
        bundle = ActiveWriteEvidenceBundle(
            session_id=self.session_id,
            commit=self.commit,
            target_type=self.params.target_type,
            command_name="RETURN_STATUS_AND_RESET",
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            serial_config=serial_cfg,
            status_poll_tx_hex=status_tx,
            status_response_before_hex=status_rx_before,
            status_response_after_hex=status_rx_after,
            decoded_status_before=decoded_before,
            decoded_status_after=decoded_after,
            confirmations=self.params.confirmations.to_dict(),
            candidate_payload_hex=reset_payload,
            candidate_frame_hex=reset_frame_hex,
            crc_hex=reset_crc,
            sequence=self.params.sequence,
            expected_ack_hex=reset_expected_ack,
            ack_outcome=reset_ack_outcome,
            ack_observed_hex=reset_ack_observed,
            expected_status_after=expected_after,
            write_state=state.value,
            transmitted=transmitted,
            serial_write_called_for_candidate=serial_write_called,
            poll_write_count=poll_writes,
            active_write_count=active_writes,
            remaining_uncertainties=default_return_status_reset_uncertainties(),
            refusal_reasons=refusal_reasons,
            warnings=warnings,
            prior_step_name="RETURN_STATUS",
            prior_payload_hex=rs_payload,
            prior_frame_hex=rs_frame_hex,
            prior_sequence=self.params.sequence,
            prior_ack_outcome=rs_ack_outcome,
            mid_status_hex=status_rx_mid,
            decoded_status_mid=decoded_mid or None,
            reset_sequence=reset_seq,
        )
        paths = write_active_write_evidence(
            self.params.evidence_dir,
            bundle,
            stem="cd1-return-status-reset-write",
        )

        try:
            if self.transport.is_open:
                await self.transport.close()
        except Exception:
            pass

        summary = {
            "sessionId": self.session_id,
            "command": "RETURN_STATUS_AND_RESET",
            "state": state.value,
            "transmitted": transmitted,
            "serialWriteCalledForCandidate": serial_write_called,
            "activeWriteCount": active_writes,
            "pollWriteCount": poll_writes,
            "logicalAddress": self.logical_address,
            "wireAddress": f"0x{self.wire_address:02X}",
            "decodedStatusBefore": decoded_before,
            "decodedStatusMid": decoded_mid,
            "decodedStatusAfter": decoded_after,
            "returnStatusPayloadHex": rs_payload,
            "returnStatusFrameHex": rs_frame_hex,
            "returnStatusSequence": self.params.sequence,
            "returnStatusAckOutcome": rs_ack_outcome,
            "returnStatusAckObservedHex": rs_ack_observed,
            "candidatePayloadHex": reset_payload,
            "candidateFrameHex": reset_frame_hex,
            "resetSequence": reset_seq,
            "crc": reset_crc,
            "expectedAckHypothesis": reset_expected_ack,
            "ackOutcome": reset_ack_outcome,
            "ackObservedHex": reset_ack_observed,
            "expectedStatusAfter": expected_after,
            "refusalReasons": refusal_reasons,
            "warnings": warnings,
            "evidence": {k: str(v) for k, v in paths.items()},
            "softwareCommit": self.commit,
            "targetType": self.params.target_type,
        }
        return ReturnStatusResetWriteResult(
            state=state,
            summary=summary,
            evidence_paths=paths,
            transmitted=transmitted,
            serial_write_called_for_candidate=serial_write_called,
        )
