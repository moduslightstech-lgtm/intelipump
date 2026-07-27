"""Technician-supervised real-Wayne CD1 AUTHORIZE (single-shot)."""

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
    default_authorize_uncertainties,
    new_session_id,
    software_commit,
    write_active_write_evidence,
)
from intelipump_fdc.real_wayne_price.guards import AuthorizeWriteParams
from intelipump_fdc.real_wayne_price.session_helpers import (
    dc1_is,
    poll_status_until,
    wait_for_ack_frame,
)
from intelipump_fdc.real_wayne_price.states import AuthorizeWriteState
from intelipump_fdc.real_wayne_price.status_decode import (
    StatusPreconditionError,
    decode_status_frame,
    validate_authorize_preconditions,
    validate_post_authorize_status,
)


@dataclass
class AuthorizeWriteResult:
    state: AuthorizeWriteState
    summary: dict[str, Any]
    evidence_paths: dict[str, Path]
    transmitted: bool = False
    serial_write_called_for_candidate: bool = False


class AuthorizeWriteSession:
    """Poll RESET → build CD1 AUTHORIZE → authorize → TX → ACK → verify AUTHORIZED."""

    def __init__(
        self,
        transport: BenchByteTransport,
        params: AuthorizeWriteParams,
        *,
        canonical_port: str | None = None,
    ) -> None:
        self.transport = transport
        self.params = params
        self.canonical_port = canonical_port or params.port
        self.logical_address = params.address
        self.wire_address = encode_wire_address(params.address)
        self.session_id = new_session_id().replace("price-dry-run", "cd1-authorize")
        self.commit = software_commit()

    async def run(self) -> AuthorizeWriteResult:
        refusal_reasons: list[str] = []
        state = AuthorizeWriteState.REFUSED
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
            validate_authorize_preconditions(
                snap, expected_wire_address=self.wire_address
            )
            state = AuthorizeWriteState.RESET

            cd1 = build_cd1_command(PumpControlCommand.AUTHORIZE)
            frame, crc, ack = build_cd1_candidate_frame(
                logical_address=self.logical_address,
                sequence=self.params.sequence,
                cd1=cd1,
            )
            payload_hex = cd1.payload_hex
            candidate_hex = frame.hex(" ")
            crc_hex = f"{crc:04X}"
            expected_ack = ack.hex(" ")
            state = AuthorizeWriteState.AUTHORIZE_BLOCK_BUILT

            authorize = getattr(self.transport, "authorize_single_active_write", None)
            if not callable(authorize):
                raise StatusPreconditionError(
                    "transport cannot authorize CD1 AUTHORIZE",
                    reasons=["transport_missing_active_authorization"],
                )
            authorize(frame, kind=ActiveFrameKind.CD1_AUTHORIZE)
            state = AuthorizeWriteState.AUTHORIZE_AUTHORIZED_FOR_SINGLE_WRITE
            await self.transport.write(frame)
            flush = getattr(self.transport, "flush", None)
            if callable(flush):
                await flush()
            transmitted = True
            serial_write_called = True
            active_writes = int(
                getattr(self.transport, "cd1_authorize_write_count", 1)
            )
            state = AuthorizeWriteState.AUTHORIZE_TRANSMITTED

            matched, ack_outcome, ack_rx_hex = await wait_for_ack_frame(
                self.transport,
                expected_ack=ack,
                timeout_ms=self.params.ack_timeout_ms,
            )
            if matched:
                state = AuthorizeWriteState.ACK_RECEIVED
            else:
                state = AuthorizeWriteState.ACK_TIMEOUT
                warnings.append(
                    "ACK not observed within timeout; continuing to status verify"
                )

            if self.params.confirmations.post_write_status_verification_required:
                snap_after, frame_after, extra_polls, notes = await poll_status_until(
                    self.transport,
                    logical_address=self.logical_address,
                    wire_address=self.wire_address,
                    response_timeout_ms=self.params.response_timeout_ms,
                    predicate=dc1_is(WaynePumpStatus.AUTHORIZED),
                    settle_ms=self.params.post_write_settle_ms,
                    max_attempts=self.params.post_write_max_status_polls,
                    failure_label="post_authorize_AUTHORIZED",
                )
                poll_writes += extra_polls
                warnings.extend(notes)
                status_rx_after = frame_after.raw_frame.hex(" ")
                decoded_after = snap_after.to_report_dict()
                validate_post_authorize_status(
                    snap_after, expected_wire_address=self.wire_address
                )
                state = AuthorizeWriteState.AUTHORIZED_VERIFIED
            else:
                warnings.append("post-write status verification was not required")

        except (StatusPreconditionError, CD1Error, RealWayneActiveCommandRefusedError) as exc:
            refusal_reasons = list(getattr(exc, "reasons", [str(exc)]))
            state = (
                AuthorizeWriteState.FAULT
                if transmitted
                else AuthorizeWriteState.REFUSED
            )
            clear = getattr(self.transport, "clear_active_write_authorization", None)
            if callable(clear):
                clear()
        except Exception as exc:  # pragma: no cover
            refusal_reasons = [str(exc)]
            state = AuthorizeWriteState.FAULT
            clear = getattr(self.transport, "clear_active_write_authorization", None)
            if callable(clear):
                clear()

        expected_after = {
            "code": int(WaynePumpStatus.AUTHORIZED),
            "name": "AUTHORIZED",
            "basis": "DART_DOCUMENTED_STATE_FLOW",
            "wayneEnum": WaynePumpStatus.AUTHORIZED.name,
        }
        bundle = ActiveWriteEvidenceBundle(
            session_id=self.session_id,
            commit=self.commit,
            target_type=self.params.target_type,
            command_name="AUTHORIZE",
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
            remaining_uncertainties=default_authorize_uncertainties(),
            refusal_reasons=refusal_reasons,
            warnings=warnings,
        )
        paths = write_active_write_evidence(
            self.params.evidence_dir, bundle, stem="cd1-authorize-write"
        )

        try:
            if self.transport.is_open:
                await self.transport.close()
        except Exception:
            pass

        summary = {
            "sessionId": self.session_id,
            "command": "AUTHORIZE",
            "state": state.value,
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
            "expectedAckHypothesis": expected_ack,
            "ackOutcome": ack_outcome,
            "ackObservedHex": ack_rx_hex,
            "expectedStatusAfter": expected_after,
            "refusalReasons": refusal_reasons,
            "warnings": warnings,
            "evidence": {k: str(v) for k, v in paths.items()},
            "softwareCommit": self.commit,
            "targetType": self.params.target_type,
        }
        return AuthorizeWriteResult(
            state=state,
            summary=summary,
            evidence_paths=paths,
            transmitted=transmitted,
            serial_write_called_for_candidate=serial_write_called,
        )
