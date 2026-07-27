"""Technician-supervised real-Wayne CD5 price dry-run (never transmits CD5)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from intelipump_fdc.bench_poll.poll_io import (
    StatusPollOutcome,
    send_status_poll_and_read_response,
)
from intelipump_fdc.bench_poll.transport import (
    BenchByteTransport,
)
from intelipump_fdc.controller.price_safety import (
    RealWayneActiveCommandRefusedError,
    assert_real_wayne_poll_only,
)
from intelipump_fdc.protocol.cd5 import (
    CD5Error,
    build_cd5_candidate_frame,
    build_cd5_price_update,
)
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.real_wayne_price.evidence import (
    DryRunEvidenceBundle,
    default_uncertainties,
    new_session_id,
    software_commit,
    write_evidence,
)
from intelipump_fdc.real_wayne_price.guards import PriceDryRunParams
from intelipump_fdc.real_wayne_price.states import PriceDryRunState
from intelipump_fdc.real_wayne_price.status_decode import (
    StatusPreconditionError,
    decode_status_frame,
    validate_preconditions,
)


@dataclass
class PriceDryRunResult:
    state: PriceDryRunState
    summary: dict[str, Any]
    evidence_paths: dict[str, Path]
    transmitted: bool = False
    serial_write_called_for_candidate: bool = False


class PriceDryRunSession:
    """Poll status, validate, build CD5 candidate in memory — never TX CD5."""

    def __init__(
        self,
        transport: BenchByteTransport,
        params: PriceDryRunParams,
        *,
        canonical_port: str | None = None,
    ) -> None:
        self.transport = transport
        self.params = params
        self.canonical_port = canonical_port or params.port
        self.logical_address = params.address
        self.wire_address = encode_wire_address(params.address)
        self.session_id = new_session_id()
        self.commit = software_commit()
        self._candidate_write_attempts = 0
        self._poll_writes = 0

    def _record_candidate_blocked(self, frame: bytes) -> None:
        """Prove candidate never reaches serial.write (also exercises guard)."""
        self._candidate_write_attempts += 1
        try:
            assert_real_wayne_poll_only(frame)
        except RealWayneActiveCommandRefusedError:
            return
        raise AssertionError("candidate frame incorrectly classified as poll")

    async def run(self) -> PriceDryRunResult:
        refusal_reasons: list[str] = []
        state = PriceDryRunState.REFUSED
        decoded: dict[str, Any] = {}
        status_tx = build_poll(self.logical_address).hex(" ")
        status_rx = ""
        cd5_payload = ""
        candidate_hex = ""
        crc_hex = ""
        expected_ack = ""
        prices_report: list[dict[str, Any]] = []
        cd5_breakdown: dict[str, Any] = {}
        serial_cfg: dict[str, Any] = {}

        try:
            if not self.transport.is_open:
                await self.transport.open()
            snapshot = getattr(self.transport, "serial_config_snapshot", None)
            if callable(snapshot):
                serial_cfg = snapshot()

            # Only status poll may be written.
            response = await send_status_poll_and_read_response(
                self.transport,
                self.logical_address,
                self.params.response_timeout_ms,
            )
            self._poll_writes = getattr(self.transport, "write_count", 1)
            status_tx = response.poll_tx.hex(" ")

            if response.outcome is not StatusPollOutcome.DATA_RESPONSE:
                raise StatusPreconditionError(
                    f"status poll outcome={response.outcome.value}",
                    reasons=[f"status_outcome_{response.outcome.value}"],
                )
            assert response.frame is not None
            status_rx = response.frame.raw_frame.hex(" ")
            snap = decode_status_frame(
                response.frame, expected_wire_address=self.wire_address
            )
            decoded = snap.to_report_dict()
            validate_preconditions(
                snap,
                expected_wire_address=self.wire_address,
                authorization_disabled=self.params.confirmations.authorization_disabled,
            )
            state = PriceDryRunState.PUMP_NOT_PROGRAMMED

            cd5 = build_cd5_price_update(
                self.params.prices_dict(),
                logical_nozzle_count=self.params.logical_nozzle_count,
                logical_nozzle_mapping_confirmed=(
                    self.params.confirmations.logical_nozzle_mapping_confirmed
                ),
                price_scale_confirmed=self.params.confirmations.price_scale_confirmed,
            )
            frame, crc, ack = build_cd5_candidate_frame(
                logical_address=self.logical_address,
                sequence=self.params.sequence,
                cd5=cd5,
            )
            # Mandatory: attempt guard path; never call transport.write(candidate).
            self._record_candidate_blocked(frame)

            cd5_payload = cd5.payload_hex
            candidate_hex = frame.hex(" ")
            crc_hex = f"{crc:04X}"
            expected_ack = ack.hex(" ")
            prices_report = [
                {
                    "logicalNozzle": p.logical_nozzle,
                    "input": p.input_price,
                    "packedBcd": p.packed_bcd_hex,
                }
                for p in cd5.prices
            ]
            cd5_breakdown = {
                "transactionType": f"0x{cd5.transaction_type:02X}",
                "payloadLength": cd5.payload_length,
                "lngHex": f"0x{cd5.payload_length:02X}",
                "pri1": prices_report[0] if prices_report else None,
                "pri2": prices_report[1] if len(prices_report) > 1 else None,
                "applicationPayloadHex": cd5_payload,
                "sourceReference": cd5.source_reference,
                "confidence": cd5.confidence,
            }
            state = PriceDryRunState.PRICE_BLOCK_BUILT
            state = PriceDryRunState.PRICE_BLOCK_REVIEW_PENDING
            state = PriceDryRunState.PRICE_BLOCK_APPROVED_NOT_TRANSMITTED
            state = PriceDryRunState.FILLING_COMPLETE_EXPECTED

        except (StatusPreconditionError, CD5Error, RealWayneActiveCommandRefusedError) as exc:
            refusal_reasons = list(getattr(exc, "reasons", [str(exc)]))
            state = PriceDryRunState.REFUSED
        except Exception as exc:  # pragma: no cover - unexpected
            refusal_reasons = [str(exc)]
            state = PriceDryRunState.FAULT

        expected_after = {
            "code": int(WaynePumpStatus.FILLING_COMPLETED),
            "name": "FILLING_COMPLETE",
            "basis": "DART_DOCUMENTED_STATE_FLOW",
            "wayneEnum": WaynePumpStatus.FILLING_COMPLETED.name,
        }
        bundle = DryRunEvidenceBundle(
            session_id=self.session_id,
            commit=self.commit,
            target_type=self.params.target_type,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            serial_config=serial_cfg,
            status_poll_tx_hex=status_tx,
            status_response_raw_hex=status_rx,
            decoded_status=decoded,
            confirmations=self.params.confirmations.to_dict(),
            logical_nozzle_count=self.params.logical_nozzle_count,
            prices=prices_report,
            cd5_payload_hex=cd5_payload,
            cd5_breakdown=cd5_breakdown,
            candidate_frame_hex=candidate_hex,
            crc_hex=crc_hex,
            sequence=self.params.sequence,
            expected_ack_hex=expected_ack,
            expected_status_after=expected_after,
            dry_run_state=state.value,
            transmitted=False,
            serial_write_called_for_candidate=False,
            write_refused_by_software=True,
            poll_write_count=self._poll_writes,
            remaining_uncertainties=default_uncertainties(),
            refusal_reasons=refusal_reasons,
        )
        paths = write_evidence(self.params.evidence_dir, bundle)

        try:
            if self.transport.is_open:
                await self.transport.close()
        except Exception:
            pass

        summary = {
            "sessionId": self.session_id,
            "state": state.value,
            "transmitted": False,
            "serialWriteCalledForCandidate": False,
            "writeRefusedBySoftware": True,
            "candidateWriteAttemptsBlocked": self._candidate_write_attempts,
            "pollWriteCount": self._poll_writes,
            "logicalAddress": self.logical_address,
            "wireAddress": f"0x{self.wire_address:02X}",
            "decodedStatus": decoded,
            "cd5PayloadHex": cd5_payload,
            "candidateFrameHex": candidate_hex,
            "crc": crc_hex,
            "expectedAckHypothesis": expected_ack,
            "expectedStatusAfterValidPrice": expected_after,
            "prices": prices_report,
            "refusalReasons": refusal_reasons,
            "evidence": {k: str(v) for k, v in paths.items()},
            "softwareCommit": self.commit,
            "targetType": self.params.target_type,
        }
        return PriceDryRunResult(
            state=state,
            summary=summary,
            evidence_paths=paths,
            transmitted=False,
            serial_write_called_for_candidate=False,
        )
