"""Technician-supervised real-Wayne CD5 price write (single-shot)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from intelipump_fdc.bench_poll.poll_io import (
    StatusPollOutcome,
    send_status_poll_and_read_response,
)
from intelipump_fdc.bench_poll.serial_reader import SerialChunk
from intelipump_fdc.bench_poll.transport import BenchByteTransport
from intelipump_fdc.controller.price_safety import RealWayneActiveCommandRefusedError
from intelipump_fdc.protocol.cd5 import (
    CD5Error,
    build_cd5_candidate_frame,
    build_cd5_price_update,
)
from intelipump_fdc.protocol.dart.application.status import WaynePumpStatus
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.captured_classify import CapturedFrameClass
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)
from intelipump_fdc.real_wayne_price.evidence import (
    WriteEvidenceBundle,
    default_write_uncertainties,
    new_session_id,
    software_commit,
    write_write_evidence,
)
from intelipump_fdc.real_wayne_price.guards import PriceWriteParams
from intelipump_fdc.real_wayne_price.states import PriceWriteState
from intelipump_fdc.real_wayne_price.status_decode import (
    StatusPreconditionError,
    decode_status_frame,
    validate_post_write_status,
    validate_preconditions,
)


class _Cd5CapableTransport(Protocol):
    async def open(self) -> None: ...

    async def close(self) -> None: ...

    async def write(self, data: bytes) -> int: ...

    async def flush(self) -> None: ...

    async def get_chunk(self, timeout_s: float) -> SerialChunk | None: ...

    def authorize_single_cd5_write(self, frame: bytes) -> None: ...

    def clear_cd5_write_authorization(self) -> None: ...

    @property
    def is_open(self) -> bool: ...

    write_count: int
    cd5_write_count: int


@dataclass
class PriceWriteResult:
    state: PriceWriteState
    summary: dict[str, Any]
    evidence_paths: dict[str, Path]
    transmitted: bool = False
    serial_write_called_for_candidate: bool = False


async def wait_for_ack_frame(
    transport: BenchByteTransport,
    *,
    expected_ack: bytes,
    timeout_ms: int,
) -> tuple[bool, str, list[str]]:
    """Read until expected ACK-family short frame or timeout.

    Returns ``(matched, outcome_label, observed_hex_frames)``.
    """
    assembler = LegacyIgemStreamAssembler()
    observed: list[str] = []
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    t0 = time.monotonic()
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        chunk = await transport.get_chunk(min(remaining, 0.05))
        if chunk is None or chunk.is_error or not chunk.raw:
            continue
        if chunk.monotonic_s < t0:
            continue
        for event in assembler.feed(chunk.raw):
            if event.kind is not AssemblerEventKind.FRAME:
                continue
            raw = event.raw
            observed.append(raw.hex(" "))
            view = event.captured
            if raw == expected_ack:
                return True, "ACK_MATCH", observed
            if (
                view is not None
                and view.classification is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK
                and len(raw) == 3
                and raw[0] == expected_ack[0]
                and raw[1] == expected_ack[1]
            ):
                return True, "ACK_MATCH", observed
    return False, "ACK_TIMEOUT", observed


class PriceWriteSession:
    """Poll → build CD5 → authorize one write → TX → ACK → verify status."""

    def __init__(
        self,
        transport: BenchByteTransport,
        params: PriceWriteParams,
        *,
        canonical_port: str | None = None,
    ) -> None:
        self.transport = transport
        self.params = params
        self.canonical_port = canonical_port or params.port
        self.logical_address = params.address
        self.wire_address = encode_wire_address(params.address)
        self.session_id = new_session_id().replace("dry-run", "price-write")
        self.commit = software_commit()

    async def run(self) -> PriceWriteResult:
        refusal_reasons: list[str] = []
        state = PriceWriteState.REFUSED
        decoded_before: dict[str, Any] = {}
        decoded_after: dict[str, Any] = {}
        status_tx = build_poll(self.logical_address).hex(" ")
        status_rx_before = ""
        status_rx_after = ""
        cd5_payload = ""
        candidate_hex = ""
        crc_hex = ""
        expected_ack = ""
        ack_rx_hex: list[str] = []
        ack_outcome = "NOT_ATTEMPTED"
        prices_report: list[dict[str, Any]] = []
        cd5_breakdown: dict[str, Any] = {}
        serial_cfg: dict[str, Any] = {}
        transmitted = False
        serial_write_called = False
        poll_writes = 0
        cd5_writes = 0
        warnings: list[str] = []

        try:
            if not self.transport.is_open:
                await self.transport.open()
            snapshot = getattr(self.transport, "serial_config_snapshot", None)
            if callable(snapshot):
                serial_cfg = snapshot()

            # 1) Pre-write status poll only.
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
            validate_preconditions(
                snap,
                expected_wire_address=self.wire_address,
                authorization_disabled=self.params.confirmations.authorization_disabled,
            )
            state = PriceWriteState.PUMP_NOT_PROGRAMMED

            # 2) Build CD5 candidate.
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
            state = PriceWriteState.PRICE_BLOCK_BUILT

            # 3) Single-shot authorize + write (exact candidate only).
            authorize = getattr(self.transport, "authorize_single_cd5_write", None)
            if not callable(authorize):
                raise StatusPreconditionError(
                    "transport cannot authorize CD5 write",
                    reasons=["transport_missing_cd5_authorization"],
                )
            authorize(frame)
            state = PriceWriteState.CD5_AUTHORIZED_FOR_SINGLE_WRITE
            await self.transport.write(frame)
            flush = getattr(self.transport, "flush", None)
            if callable(flush):
                await flush()
            transmitted = True
            serial_write_called = True
            cd5_writes = int(getattr(self.transport, "cd5_write_count", 1))
            state = PriceWriteState.CD5_TRANSMITTED

            # 4) Wait for line-layer ACK hypothesis (non-fatal if timeout).
            matched, ack_outcome, ack_rx_hex = await wait_for_ack_frame(
                self.transport,
                expected_ack=ack,
                timeout_ms=self.params.ack_timeout_ms,
            )
            if matched:
                state = PriceWriteState.ACK_RECEIVED
            else:
                state = PriceWriteState.ACK_TIMEOUT
                warnings.append(
                    "ACK not observed within timeout; continuing to status verify"
                )

            # 5) Post-write status poll; require FILLING_COMPLETE when confirmed.
            post = await send_status_poll_and_read_response(
                self.transport,
                self.logical_address,
                self.params.response_timeout_ms,
            )
            poll_writes += 1
            if post.outcome is not StatusPollOutcome.DATA_RESPONSE:
                raise StatusPreconditionError(
                    f"post-write status poll outcome={post.outcome.value}",
                    reasons=[f"post_status_outcome_{post.outcome.value}"],
                )
            assert post.frame is not None
            status_rx_after = post.frame.raw_frame.hex(" ")
            snap_after = decode_status_frame(
                post.frame, expected_wire_address=self.wire_address
            )
            decoded_after = snap_after.to_report_dict()
            if self.params.confirmations.post_write_status_verification_required:
                validate_post_write_status(
                    snap_after, expected_wire_address=self.wire_address
                )
                state = PriceWriteState.FILLING_COMPLETE_VERIFIED
            else:
                warnings.append("post-write status verification was not required")
                if snap_after.dc1_code == int(WaynePumpStatus.FILLING_COMPLETED):
                    state = PriceWriteState.FILLING_COMPLETE_VERIFIED

        except (StatusPreconditionError, CD5Error, RealWayneActiveCommandRefusedError) as exc:
            refusal_reasons = list(getattr(exc, "reasons", [str(exc)]))
            state = (
                PriceWriteState.FAULT if transmitted else PriceWriteState.REFUSED
            )
            clear = getattr(self.transport, "clear_cd5_write_authorization", None)
            if callable(clear):
                clear()
        except Exception as exc:  # pragma: no cover
            refusal_reasons = [str(exc)]
            state = PriceWriteState.FAULT
            clear = getattr(self.transport, "clear_cd5_write_authorization", None)
            if callable(clear):
                clear()

        expected_after = {
            "code": int(WaynePumpStatus.FILLING_COMPLETED),
            "name": "FILLING_COMPLETE",
            "basis": "DART_DOCUMENTED_STATE_FLOW",
            "wayneEnum": WaynePumpStatus.FILLING_COMPLETED.name,
        }
        bundle = WriteEvidenceBundle(
            session_id=self.session_id,
            commit=self.commit,
            target_type=self.params.target_type,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            serial_config=serial_cfg,
            status_poll_tx_hex=status_tx,
            status_response_before_hex=status_rx_before,
            status_response_after_hex=status_rx_after,
            decoded_status_before=decoded_before,
            decoded_status_after=decoded_after,
            confirmations=self.params.confirmations.to_dict(),
            logical_nozzle_count=self.params.logical_nozzle_count,
            prices=prices_report,
            cd5_payload_hex=cd5_payload,
            cd5_breakdown=cd5_breakdown,
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
            cd5_write_count=cd5_writes,
            remaining_uncertainties=default_write_uncertainties(),
            refusal_reasons=refusal_reasons,
            warnings=warnings,
        )
        paths = write_write_evidence(self.params.evidence_dir, bundle)

        try:
            if self.transport.is_open:
                await self.transport.close()
        except Exception:
            pass

        summary = {
            "sessionId": self.session_id,
            "state": state.value,
            "transmitted": transmitted,
            "serialWriteCalledForCandidate": serial_write_called,
            "cd5WriteCount": cd5_writes,
            "pollWriteCount": poll_writes,
            "logicalAddress": self.logical_address,
            "wireAddress": f"0x{self.wire_address:02X}",
            "decodedStatusBefore": decoded_before,
            "decodedStatusAfter": decoded_after,
            "cd5PayloadHex": cd5_payload,
            "candidateFrameHex": candidate_hex,
            "crc": crc_hex,
            "expectedAckHypothesis": expected_ack,
            "ackOutcome": ack_outcome,
            "ackObservedHex": ack_rx_hex,
            "expectedStatusAfterValidPrice": expected_after,
            "prices": prices_report,
            "refusalReasons": refusal_reasons,
            "warnings": warnings,
            "evidence": {k: str(v) for k, v in paths.items()},
            "softwareCommit": self.commit,
            "targetType": self.params.target_type,
            "authorizationIncluded": False,
            "resetIncluded": False,
        }
        return PriceWriteResult(
            state=state,
            summary=summary,
            evidence_paths=paths,
            transmitted=transmitted,
            serial_write_called_for_candidate=serial_write_called,
        )
