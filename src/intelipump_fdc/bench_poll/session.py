"""Bounded poll-only bench session (verified DART POLL frames only)."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path

from intelipump_fdc.bench_poll.evidence import (
    BenchEvent,
    BenchResult,
    BenchSessionStats,
    EvidenceWriter,
    new_session_id,
    suggest_result,
    write_markdown_summary,
)
from intelipump_fdc.bench_poll.guards import TARGET_OWNED_LAB_WAYNE
from intelipump_fdc.bench_poll.transport import BenchByteTransport
from intelipump_fdc.protocol.dart.line.addressing import encode_wire_address
from intelipump_fdc.protocol.dart.line.captured_classify import CapturedFrameClass
from intelipump_fdc.protocol.dart.line.frame_builder import build_poll
from intelipump_fdc.protocol.dart.line.legacy_stream import (
    AssemblerEventKind,
    LegacyIgemStreamAssembler,
)

logger = logging.getLogger(__name__)

_VALID_RESPONSE_CLASSES = frozenset(
    {
        CapturedFrameClass.SHORT_CONTROL_70,
        CapturedFrameClass.DATA_FRAME,
    }
)


@dataclass(frozen=True, slots=True)
class PollBenchSessionConfig:
    port: str
    address: int
    baud: int
    max_polls: int
    response_timeout_ms: int
    evidence_jsonl: Path
    evidence_md: Path
    read_size: int = 256
    target_type: str = TARGET_OWNED_LAB_WAYNE
    simulator_validation: bool = False


class PollBenchSession:
    """Sends only ``build_poll``; never creates a command queue or authorize path."""

    def __init__(
        self,
        transport: BenchByteTransport,
        config: PollBenchSessionConfig,
    ) -> None:
        if not (1 <= config.max_polls <= 10):
            raise ValueError("max_polls must be 1-10")
        self.transport = transport
        self.config = config
        self.logical_address = config.address
        self.wire_address = encode_wire_address(config.address)
        self.session_id = new_session_id()
        self.stats = BenchSessionStats()
        self._stop = asyncio.Event()
        self._assembler = LegacyIgemStreamAssembler()
        self.command_queue_created = False
        self.authorization_objects_created = 0
        self.result: BenchResult | None = None

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> dict[str, object]:
        jsonl_path = self.config.evidence_jsonl
        md_path = self.config.evidence_md
        writer = EvidenceWriter(
            jsonl_path,
            self.session_id,
            target_type=self.config.target_type,
            simulator_validation=self.config.simulator_validation,
        )
        try:
            if not self.transport.is_open:
                await self.transport.open()
            writer.emit_event(
                BenchEvent.BENCH_STARTED,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                notes=(
                    f"POLL_ONLY_BENCH port={self.config.port} "
                    f"baud={self.config.baud} max_polls={self.config.max_polls} "
                    f"logicalAddress={self.logical_address} "
                    f"wireAddress=0x{self.wire_address:02X} "
                    f"targetType={self.config.target_type} "
                    f"simulatorValidation={self.config.simulator_validation}"
                ),
            )
            for seq in range(1, self.config.max_polls + 1):
                if self._stop.is_set():
                    break
                stop_early = await self._one_poll(writer, seq)
                if stop_early or self._stop.is_set():
                    break
        finally:
            self.result = suggest_result(
                self.stats, max_polls=self.config.max_polls
            )
            writer.emit_event(
                BenchEvent.BENCH_STOPPED,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                notes=(
                    f"result={self.result.value} "
                    f"polls={self.stats.polls_sent} "
                    f"valid={self.stats.valid_responses} "
                    f"timeouts={self.stats.timeouts}"
                ),
            )
            writer.close()
            try:
                if self.transport.is_open:
                    await self.transport.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("serial close failed: %s", exc)
            write_markdown_summary(
                md_path,
                session_id=self.session_id,
                port=self.config.port,
                baud=self.config.baud,
                address=self.logical_address,
                max_polls=self.config.max_polls,
                stats=self.stats,
                result=self.result,
                target_type=self.config.target_type,
                simulator_validation=self.config.simulator_validation,
            )

        return {
            "sessionId": self.session_id,
            "result": self.result.value if self.result else None,
            "pollsSent": self.stats.polls_sent,
            "validResponses": self.stats.valid_responses,
            "timeouts": self.stats.timeouts,
            "crcErrors": self.stats.crc_errors,
            "commandQueueCreated": self.command_queue_created,
            "authorizationObjectsCreated": self.authorization_objects_created,
            "logicalAddress": self.logical_address,
            "wireAddress": self.wire_address,
            "targetType": self.config.target_type,
            "simulatorValidation": self.config.simulator_validation,
            "evidenceJsonl": str(jsonl_path),
            "evidenceMd": str(md_path),
            "writeCount": getattr(self.transport, "write_count", self.stats.polls_sent),
        }

    async def _one_poll(self, writer: EvidenceWriter, seq: int) -> bool:
        """Return True to stop the bench early (CRC/protocol error)."""
        poll = build_poll(self.logical_address)
        t0 = time.monotonic()
        mono_tx = time.monotonic_ns()
        await self.transport.write(poll)
        self.stats.polls_sent += 1
        self.stats.last_tx_hex = poll.hex(" ")
        writer.emit_frame(
            direction="TX",
            raw=poll,
            monotonic_ns=mono_tx,
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            timeout_ms=self.config.response_timeout_ms,
            classification=CapturedFrameClass.POLL.value,
            crc_valid=None,
            event=BenchEvent.POLL_SENT,
            notes="verified_build_poll_only",
        )
        writer.emit_event(
            BenchEvent.POLL_SENT,
            monotonic_ns=mono_tx,
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            timeout_ms=self.config.response_timeout_ms,
        )

        deadline = t0 + (self.config.response_timeout_ms / 1000.0)
        # Persistent buffer across reads within this poll; do not hunt mid-frame.
        while time.monotonic() < deadline and not self._stop.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                chunk = await asyncio.wait_for(
                    self.transport.read(self.config.read_size),
                    timeout=min(remaining, 0.05),
                )
            except TimeoutError:
                chunk = b""
            if not chunk:
                await asyncio.sleep(0.001)
                continue
            for event in self._assembler.feed(chunk):
                if event.kind is AssemblerEventKind.NOISE:
                    continue
                if event.kind is AssemblerEventKind.OVERFLOW:
                    self.stats.protocol_errors += 1
                    writer.emit_event(
                        BenchEvent.PROTOCOL_ERROR,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=seq,
                        notes=event.message or "overflow",
                        classification="OVERFLOW",
                    )
                    return True
                if event.kind is AssemblerEventKind.PARTIAL:
                    continue
                if event.kind is AssemblerEventKind.REJECTED:
                    self.stats.protocol_errors += 1
                    writer.emit_frame(
                        direction="RX",
                        raw=event.raw,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=seq,
                        timeout_ms=self.config.response_timeout_ms,
                        classification="REJECTED",
                        crc_valid=None,
                        event=BenchEvent.PROTOCOL_ERROR,
                        notes=event.message,
                    )
                    return True
                frame = event.frame
                assert frame is not None
                captured = event.captured
                latency_ms = (time.monotonic() - t0) * 1000.0
                self.stats.latencies_ms.append(latency_ms)
                self.stats.last_rx_hex = frame.raw_frame.hex(" ")
                classification = (
                    captured.classification.value
                    if captured is not None
                    else frame.control_type.value
                )
                crc_valid = frame.crc_valid
                if (
                    captured is not None
                    and captured.classification is CapturedFrameClass.DATA_FRAME
                    and crc_valid is False
                ):
                    self.stats.crc_errors += 1
                    writer.emit_frame(
                        direction="RX",
                        raw=frame.raw_frame,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=seq,
                        timeout_ms=self.config.response_timeout_ms,
                        classification=classification,
                        crc_valid=False,
                        event=BenchEvent.PROTOCOL_ERROR,
                        notes="crc_invalid_stop",
                    )
                    return True

                if frame.address != self.wire_address:
                    self.stats.protocol_errors += 1
                    writer.emit_event(
                        BenchEvent.PROTOCOL_ERROR,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=seq,
                        notes=(
                            f"address_mismatch got_wire=0x{frame.address:02X} "
                            f"expected_wire=0x{self.wire_address:02X}"
                        ),
                        classification="ADDRESS_MISMATCH",
                    )
                    return True

                if (
                    captured is not None
                    and captured.classification not in _VALID_RESPONSE_CLASSES
                ):
                    # SEQUENCE_CONTROL_OR_ACK is not a status-poll response itself.
                    if (
                        captured.classification
                        is CapturedFrameClass.SEQUENCE_CONTROL_OR_ACK
                    ):
                        self.stats.protocol_errors += 1
                        writer.emit_event(
                            BenchEvent.PROTOCOL_ERROR,
                            monotonic_ns=time.monotonic_ns(),
                            pump_address=self.logical_address,
                            logical_address=self.logical_address,
                            wire_address=self.wire_address,
                            poll_sequence=seq,
                            notes=(
                                f"unexpected_frame {classification}; "
                                f"sequenceNibble={captured.sequence_nibble}; "
                                "possibleAcknowledgement=true; not nozzle_lift"
                            ),
                            classification=classification,
                        )
                        return True
                    self.stats.protocol_errors += 1
                    writer.emit_event(
                        BenchEvent.PROTOCOL_ERROR,
                        monotonic_ns=time.monotonic_ns(),
                        pump_address=self.logical_address,
                        logical_address=self.logical_address,
                        wire_address=self.wire_address,
                        poll_sequence=seq,
                        notes=f"unexpected_frame {classification}",
                        classification=classification,
                    )
                    return True

                self.stats.valid_responses += 1
                writer.emit_frame(
                    direction="RX",
                    raw=frame.raw_frame,
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.logical_address,
                    logical_address=self.logical_address,
                    wire_address=self.wire_address,
                    poll_sequence=seq,
                    timeout_ms=self.config.response_timeout_ms,
                    classification=classification,
                    crc_valid=crc_valid,
                    event=BenchEvent.RESPONSE_RECEIVED,
                    notes=f"latency_ms={latency_ms:.2f}",
                )
                writer.emit_event(
                    BenchEvent.RESPONSE_RECEIVED,
                    monotonic_ns=time.monotonic_ns(),
                    pump_address=self.logical_address,
                    logical_address=self.logical_address,
                    wire_address=self.wire_address,
                    poll_sequence=seq,
                    classification=classification,
                    crc_valid=crc_valid,
                    notes=f"latency_ms={latency_ms:.2f}",
                )
                return False

        # Preserve partial buffer diagnostics on timeout.
        for event in self._assembler.expire_partial():
            writer.emit_frame(
                direction="RX",
                raw=event.raw,
                monotonic_ns=time.monotonic_ns(),
                pump_address=self.logical_address,
                logical_address=self.logical_address,
                wire_address=self.wire_address,
                poll_sequence=seq,
                timeout_ms=self.config.response_timeout_ms,
                classification=CapturedFrameClass.PARTIAL_FRAME.value,
                crc_valid=None,
                event=BenchEvent.RESPONSE_TIMEOUT,
                notes=event.message,
            )

        self.stats.timeouts += 1
        writer.emit_event(
            BenchEvent.RESPONSE_TIMEOUT,
            monotonic_ns=time.monotonic_ns(),
            pump_address=self.logical_address,
            logical_address=self.logical_address,
            wire_address=self.wire_address,
            poll_sequence=seq,
            timeout_ms=self.config.response_timeout_ms,
            notes="no_frame_before_deadline",
        )
        return False
