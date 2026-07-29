"""Technician-supervised real-Wayne CD101 request totals (single-shot)."""

from __future__ import annotations

import asyncio
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
from intelipump_fdc.protocol.cd101 import (
    CD101Error,
    build_cd101_candidate_frame,
    build_cd101_request,
)
from intelipump_fdc.protocol.dart.application.constants import TransactionType
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.real_wayne_price.evidence import (
    ActiveWriteEvidenceBundle,
    default_cd101_uncertainties,
    new_session_id,
    software_commit,
    write_active_write_evidence,
)
from intelipump_fdc.real_wayne_price.guards import Cd101WriteParams
from intelipump_fdc.real_wayne_price.session_helpers import (
    sequence_stale_status_hint,
    wait_for_ack_frame,
)
from intelipump_fdc.real_wayne_price.states import Cd101WriteState
from intelipump_fdc.real_wayne_price.status_decode import (
    StatusPreconditionError,
    decode_status_frame,
)


@dataclass
class Cd101WriteResult:
    state: Cd101WriteState
    summary: dict[str, Any]
    evidence_paths: dict[str, Path]
    transmitted: bool = False
    serial_write_called_for_candidate: bool = False


class Cd101WriteSession:
    """Poll → build CD101 → authorize one write → TX → ACK → optional DC101 poll."""

    def __init__(
        self,
        transport: BenchByteTransport,
        params: Cd101WriteParams,
        *,
        canonical_port: str | None = None,
    ) -> None:
        self.transport = transport
        self.params = params
        self.canonical_port = canonical_port or params.port
        self.logical_address = params.address
        self.wire_address = encode_wire_address(params.address)
        self.session_id = new_session_id().replace("price-dry-run", "cd101")
        self.commit = software_commit()

    async def run(self) -> Cd101WriteResult:
        refusal_reasons: list[str] = []
        state = Cd101WriteState.REFUSED
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
        dc101_seen = False

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
            if not snap.crc_valid:
                raise StatusPreconditionError(
                    "pre_cd101_crc_invalid",
                    reasons=["pre_cd101_crc_invalid"],
                    last_snap=snap,
                    last_frame=response.frame,
                )
            state = Cd101WriteState.STATUS_OBSERVED

            cd101 = build_cd101_request(counter_select=self.params.counter_select)
            frame, crc, ack = build_cd101_candidate_frame(
                logical_address=self.logical_address,
                sequence=self.params.sequence,
                cd101=cd101,
            )
            payload_hex = cd101.payload_hex
            candidate_hex = frame.hex(" ")
            crc_hex = f"{crc:04X}"
            expected_ack = ack.hex(" ")
            state = Cd101WriteState.CD101_BLOCK_BUILT

            authorize = getattr(self.transport, "authorize_single_active_write", None)
            if not callable(authorize):
                raise RealWayneActiveCommandRefusedError(
                    "transport cannot authorize CD101"
                )
            authorize(frame, kind=ActiveFrameKind.CD101_REQUEST_TOTALS)
            state = Cd101WriteState.CD101_AUTHORIZED_FOR_SINGLE_WRITE

            await self.transport.write(frame)
            flush = getattr(self.transport, "flush", None)
            if callable(flush):
                await flush()
            transmitted = True
            serial_write_called = True
            active_writes = 1
            state = Cd101WriteState.CD101_TRANSMITTED

            matched, ack_outcome, observed = await wait_for_ack_frame(
                self.transport,
                expected_ack=ack,
                timeout_ms=self.params.ack_timeout_ms,
            )
            ack_rx_hex = list(observed)
            if matched:
                state = Cd101WriteState.ACK_RECEIVED
            else:
                state = Cd101WriteState.ACK_TIMEOUT
                warnings.append(
                    "ACK not observed within timeout; continuing optional status poll"
                )
                hint = sequence_stale_status_hint(
                    sequence=self.params.sequence,
                    ack_outcome=ack_outcome,
                    refusal_reasons=[],
                )
                if hint:
                    warnings.append(hint)

            for _ in range(self.params.post_write_max_status_polls):
                if self.params.post_write_settle_ms > 0:
                    await asyncio.sleep(self.params.post_write_settle_ms / 1000.0)
                post = await send_status_poll_and_read_response(
                    self.transport,
                    self.logical_address,
                    self.params.response_timeout_ms,
                )
                poll_writes += 1
                if post.outcome is not StatusPollOutcome.DATA_RESPONSE:
                    continue
                assert post.frame is not None
                status_rx_after = post.frame.raw_frame.hex(" ")
                after = decode_status_frame(
                    post.frame, expected_wire_address=self.wire_address
                )
                decoded_after = after.to_report_dict()
                for tx in after.transactions:
                    t = str(tx.get("type", ""))
                    if t == TransactionType.DC101_TOTAL_COUNTERS.value or "DC101" in t:
                        dc101_seen = True
                        break
                if dc101_seen:
                    if state is Cd101WriteState.ACK_RECEIVED:
                        state = Cd101WriteState.DC101_OBSERVED
                    break

            if state is Cd101WriteState.ACK_RECEIVED and not dc101_seen:
                warnings.append(
                    "ACK_MATCH but no DC101 in post-poll window "
                    "(may still be OK; ePump often got totals on next DATA)"
                )

        except (
            StatusPreconditionError,
            CD101Error,
            RealWayneActiveCommandRefusedError,
            OSError,
        ) as exc:
            if isinstance(exc, StatusPreconditionError):
                refusal_reasons.extend(exc.reasons)
            else:
                refusal_reasons.append(str(exc))
            state = Cd101WriteState.FAULT if transmitted else Cd101WriteState.REFUSED
        except Exception as exc:  # noqa: BLE001 — evidence always written
            refusal_reasons.append(f"unexpected:{type(exc).__name__}:{exc}")
            state = Cd101WriteState.FAULT if transmitted else Cd101WriteState.REFUSED
            warnings.append(str(exc))

        expected_after = {
            "note": (
                "CD101 does not require a DC1 change; "
                "DC101 reply is optional evidence"
            )
        }
        bundle = ActiveWriteEvidenceBundle(
            session_id=self.session_id,
            commit=self.commit,
            target_type=self.params.target_type,
            command_name="CD101_REQUEST_TOTALS",
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
            remaining_uncertainties=default_cd101_uncertainties(),
            refusal_reasons=refusal_reasons,
            warnings=warnings,
        )
        paths = write_active_write_evidence(
            self.params.evidence_dir,
            bundle,
            stem="cd101-request",
        )
        summary = {
            "state": state.value,
            "transmitted": transmitted,
            "serialWriteCalledForCandidate": serial_write_called,
            "candidatePayloadHex": payload_hex,
            "candidateFrameHex": candidate_hex,
            "ackOutcome": ack_outcome,
            "dc101Observed": dc101_seen,
            "counterSelect": self.params.counter_select,
            "evidence": {k: str(v) for k, v in paths.items()},
            "warnings": warnings,
            "refusalReasons": refusal_reasons,
            "basis": "EPUMP_PRE_OUT_WINDOW_CD101_REQUEST_TOTALS",
        }
        return Cd101WriteResult(
            state=state,
            summary=summary,
            evidence_paths=paths,
            transmitted=transmitted,
            serial_write_called_for_candidate=serial_write_called,
        )
