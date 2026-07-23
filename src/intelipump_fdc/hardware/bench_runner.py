"""RS-485 office bench harness (controller ↔ simulator over two adapters)."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path

from intelipump_fdc.controller.controller_loop import (
    ControllerLoop,
    ControllerRuntime,
    default_lab_safety,
)
from intelipump_fdc.controller.poll_scheduler import PollSchedulerConfig
from intelipump_fdc.controller.session_events import ControllerEvent, ControllerEventType
from intelipump_fdc.hardware.adapter_validation import (
    expected_serial_config,
    validate_adapter_open,
)
from intelipump_fdc.hardware.bench_faults import BenchFaultPlan, SimulatorFaultInjector
from intelipump_fdc.hardware.capture import JsonlCaptureWriter
from intelipump_fdc.hardware.errors import (
    BenchConfigError,
    BenchRuntimeError,
    PortNotFoundError,
)
from intelipump_fdc.hardware.evidence import write_evidence_json, write_markdown_report
from intelipump_fdc.hardware.latency import LatencyTracker
from intelipump_fdc.hardware.models import BenchConfig, BenchEvidence
from intelipump_fdc.hardware.port_identity import (
    classify_open_error,
    ensure_port_present,
    resolve_bench_ports,
)
from intelipump_fdc.hardware.reconnect import ReconnectPolicy
from intelipump_fdc.protocol.dart.transport.serial import SerialTransport
from intelipump_fdc.simulator.serial_bridge import (
    SerialBridgeConfig,
    SimulatorSerialBridge,
)


class _BenchEventSubscriber:
    def __init__(
        self,
        tracker: LatencyTracker,
        *,
        capture: JsonlCaptureWriter | None,
        port: str,
        stable_id: str | None,
        role: str = "controller",
    ) -> None:
        self._tracker = tracker
        self._capture = capture
        self._port = port
        self._stable_id = stable_id
        self._role = role

    def __call__(self, event: ControllerEvent) -> None:
        now = time.monotonic()
        if event.address is None:
            return
        raw_hex = str(event.payload.get("raw_hex") or "")
        raw = bytes.fromhex(raw_hex.replace(" ", "")) if raw_hex else b""
        write_start = event.payload.get("write_start_monotonic_s")
        write_complete = event.payload.get("write_complete_monotonic_s")
        first_byte = event.payload.get("first_byte_monotonic_s")

        if event.type is ControllerEventType.FRAME_SENT and event.detail in {
            "POLL",
            "POLL_RETRY",
        }:
            # Prefer transport-adjacent monotonic stamps from the controller write path.
            start_s = float(write_start) if write_start is not None else now
            complete_s = float(write_complete) if write_complete is not None else now
            self._tracker.mark_write_start(event.address, monotonic_s=start_s)
            self._tracker.mark_write_complete(event.address, monotonic_s=complete_s)
            if self._capture is not None and raw:
                self._capture.record_bytes(
                    direction="TX",
                    role=self._role,
                    port=self._port,
                    adapter_stable_id=self._stable_id,
                    raw=raw,
                    parsed_frame_type=event.detail,
                    dart_address=event.address,
                    sequence=None,
                    crc_valid=None,
                )
        elif event.type is ControllerEventType.FRAME_SENT and event.detail == "ACK":
            start_s = float(write_start) if write_start is not None else now
            complete_s = float(write_complete) if write_complete is not None else now
            self._tracker.mark_ack_write_start(event.address, monotonic_s=start_s)
            self._tracker.mark_ack_write(event.address, monotonic_s=complete_s)
            if self._capture is not None and raw:
                self._capture.record_bytes(
                    direction="TX",
                    role=self._role,
                    port=self._port,
                    adapter_stable_id=self._stable_id,
                    raw=raw,
                    parsed_frame_type="ACK",
                    dart_address=event.address,
                )
        elif event.type is ControllerEventType.FIRST_RESPONSE_BYTE:
            byte_s = float(first_byte) if first_byte is not None else now
            self._tracker.mark_first_response_byte(event.address, monotonic_s=byte_s)
        elif event.type in {
            ControllerEventType.DATA_RECEIVED,
            ControllerEventType.EOT_RECEIVED,
            ControllerEventType.NAK_RECEIVED,
        }:
            # Interval 2 uses complete-frame events only (not FRAME_RECEIVED, which
            # fires before EOT/DATA specialization and would double-count).
            kind = event.type.value
            sample = self._tracker.mark_response(
                event.address, monotonic_s=now, response_kind=kind
            )
            if self._capture is not None and raw:
                self._capture.record_bytes(
                    direction="RX",
                    role=self._role,
                    port=self._port,
                    adapter_stable_id=self._stable_id,
                    raw=raw,
                    parsed_frame_type=kind,
                    dart_address=event.address,
                    latency_ms=(
                        sample.write_complete_to_complete_response_ms
                        if sample
                        else None
                    ),
                )
        elif event.type is ControllerEventType.RESPONSE_TIMEOUT:
            self._tracker.mark_response(
                event.address, monotonic_s=now, response_kind="RESPONSE_TIMEOUT"
            )


async def run_rs485_bench(
    config: BenchConfig,
    *,
    evidence_path: Path | str | None = None,
    report_path: Path | str | None = None,
    capture_path: Path | str | None = None,
    skip_open_validation: bool = False,
    log_frames: bool = False,
    fault_plan: BenchFaultPlan | None = None,
    physical_run: bool = False,
) -> BenchEvidence:
    """
    Run LISTEN_ONLY controller + simulator serial bridge on two ports.

    Does not enable active field commands or connect to a real Wayne dispenser.
    physical_hil_status remains NOT_RUN unless physical_run=True and success.
    """
    config.validate()
    if config.environment.upper() != "LAB":
        raise BenchConfigError("bench harness is LAB-only")

    try:
        controller_dev, simulator_dev = resolve_bench_ports(
            controller_port=config.controller_port,
            simulator_port=config.simulator_port,
            controller_stable_id=config.controller_adapter_stable_id,
            simulator_stable_id=config.simulator_adapter_stable_id,
        )
    except PortNotFoundError:
        # Virtual/memory paths used in unit tests may not exist as by-id.
        if config.controller_port.startswith("/tmp/") or skip_open_validation:
            from intelipump_fdc.hardware.models import SerialDeviceInfo

            controller_dev = SerialDeviceInfo(
                device_path=config.controller_port,
                stable_id="virtual-controller",
            )
            simulator_dev = SerialDeviceInfo(
                device_path=config.simulator_port,
                stable_id="virtual-simulator",
            )
        else:
            raise

    for path in (controller_dev.device_path, simulator_dev.device_path):
        try:
            ensure_port_present(path)
        except PortNotFoundError:
            if not skip_open_validation and not path.startswith("/tmp/"):
                raise

    # pyserial read timeout must stay short (inter-chunk). The configured bench
    # response timeout remains the software deadline in ControllerLoop only.
    # Setting read_timeout_s == response_timeout_ms makes every successful
    # read(n>available) wait ~timeout and falsely clusters latency near 100 ms.
    controller_cfg = expected_serial_config(
        controller_dev.device_path,
        baud_rate=config.baud_rate,
        exclusive_open=config.exclusive_open,
        read_timeout_s=0.02,
    )
    simulator_cfg = expected_serial_config(
        simulator_dev.device_path,
        baud_rate=config.baud_rate,
        exclusive_open=config.exclusive_open,
        read_timeout_s=0.02,
    )

    evidence = BenchEvidence(
        bench=config,
        started_at=datetime.now(UTC),
        controller_device=controller_dev,
        simulator_device=simulator_dev,
        listen_only=True,
        active_commands_enabled=False,
        physical_hil_status="NOT_RUN",
        notes=[
            "Phase 10 office bench: adapters A/B only; no Wayne dispenser.",
            f"automatic_direction_control={config.automatic_direction_control}",
            f"termination_enabled={config.termination_enabled}",
            f"bias_enabled={config.bias_enabled}",
            f"ground_reference_connected={config.ground_reference_connected}",
            "no_command_replay",
            "no_duplicate_transaction_completion_policy",
        ],
    )

    if not skip_open_validation:
        try:
            evidence.controller_validation = await validate_adapter_open(controller_cfg)
            evidence.simulator_validation = await validate_adapter_open(simulator_cfg)
        except Exception as exc:
            mapped = classify_open_error(exc, port=controller_dev.device_path)
            evidence.notes.append(f"open_error:{mapped}")
            evidence.finished_at = datetime.now(UTC)
            _write_outputs(evidence, evidence_path, report_path)
            raise BenchRuntimeError(str(mapped)) from exc
        if evidence.controller_validation and not evidence.controller_validation.ok:
            raise BenchRuntimeError(
                "controller adapter validation failed: "
                + ", ".join(evidence.controller_validation.errors)
            )
        if evidence.simulator_validation and not evidence.simulator_validation.ok:
            raise BenchRuntimeError(
                "simulator adapter validation failed: "
                + ", ".join(evidence.simulator_validation.errors)
            )

    tracker = LatencyTracker(
        protocol_target_ms=float(config.protocol_target_ms),
        configured_bench_timeout_ms=float(config.response_timeout_ms),
    )
    evidence.latency = tracker
    evidence.notes.append(
        "latency intervals: write_complete→first_byte, "
        "write_complete→complete_response, data→ack_start, ack_complete→next_poll; "
        "protocol_target_ms and configured_bench_timeout_ms are not part of samples"
    )

    capture: JsonlCaptureWriter | None = None
    if capture_path is not None:
        capture = JsonlCaptureWriter(capture_path, write_sanitized_copy=True)

    safety = default_lab_safety()
    reconnect = ReconnectPolicy(min_delay_s=0.5, max_delay_s=8.0, max_attempts=10)
    runtime = ControllerRuntime(
        transport=SerialTransport(controller_cfg),
        safety=safety,
        config=PollSchedulerConfig(
            addresses=config.addresses,
            response_timeout_ms=config.response_timeout_ms,
            inter_poll_delay_ms=max(1, int(config.inter_frame_delay_ms)),
            idle_sleep_ms=max(5, int(config.turnaround_delay_ms * 2)),
            max_retries=1,
            max_consecutive_timeouts=5,
            reconnect_delay_s=reconnect.min_delay_s,
        ),
        log_frames=log_frames,
    )
    runtime.events.add_subscriber(
        _BenchEventSubscriber(
            tracker,
            capture=capture,
            port=controller_dev.device_path,
            stable_id=controller_dev.stable_id,
        )
    )
    loop = ControllerLoop(runtime)

    injector = (
        SimulatorFaultInjector(plan=fault_plan) if fault_plan is not None else None
    )
    if fault_plan is not None:
        evidence.fault_scenarios.extend(k.value for k in fault_plan.kinds)

    # Offline address faults (software): disable pumps before run.
    bridge = SimulatorSerialBridge(
        SerialTransport(simulator_cfg),
        config=SerialBridgeConfig(
            idle_sleep_ms=2,
            sim_time_step_ms=20,
            log_frames=log_frames,
        ),
        fault_injector=injector,
    )
    if fault_plan is not None:
        from intelipump_fdc.hardware.bench_faults import BenchFaultKind

        if fault_plan.take(BenchFaultKind.ONE_ADDRESS_OFFLINE):
            addr = fault_plan.offline_addresses[0] if fault_plan.offline_addresses else 1
            if addr in bridge.simulator.pumps:
                bridge.simulator.pumps[addr].communication_enabled = False
            evidence.fault_scenarios.append(f"ONE_ADDRESS_OFFLINE:{addr}")
        if fault_plan.take(BenchFaultKind.ALL_ADDRESSES_OFFLINE):
            for pump in bridge.simulator.pumps.values():
                pump.communication_enabled = False
            evidence.fault_scenarios.append("ALL_ADDRESSES_OFFLINE")
        if fault_plan.take(BenchFaultKind.SIMULATOR_RESTART):
            bridge.soft_restart()
            evidence.fault_scenarios.append("SIMULATOR_RESTART")

    async def run_ctrl() -> None:
        try:
            await loop.run(duration_s=config.duration_s)
        finally:
            bridge.request_stop()

    try:
        await asyncio.gather(bridge.run(), run_ctrl())
    except Exception as exc:
        evidence.notes.append(f"runtime_error:{type(exc).__name__}:{exc}")
        evidence.reconnect_events = len(reconnect.events)
        evidence.finished_at = datetime.now(UTC)
        if capture is not None:
            cap = capture.finalize()
            evidence.capture_path = str(cap)
            if capture._sanitized_path is not None:
                evidence.sanitized_capture_path = str(capture._sanitized_path)
        _write_outputs(evidence, evidence_path, report_path)
        raise BenchRuntimeError(str(exc)) from exc

    summary = loop.summary()
    totals = summary.get("totals", {})
    if isinstance(totals, dict):
        evidence.poll_count = int(totals.get("poll_count", 0))
        evidence.data_count = int(totals.get("data_count", 0))
        evidence.eot_count = int(totals.get("eot_count", 0))
        evidence.timeout_count = int(totals.get("timeouts", 0))
        evidence.crc_error_count = int(totals.get("crc_errors", 0))
        evidence.nak_count = int(totals.get("nak_count", 0))
        evidence.retry_count = int(totals.get("retry_count", 0) or 0)
    # ACK count from FRAME_SENT detail is not aggregated; approximate from data.
    evidence.ack_count = evidence.data_count
    evidence.reconnect_events = len(reconnect.events)
    if injector is not None:
        evidence.fault_scenarios.extend(injector.applied)

    evidence.finished_at = datetime.now(UTC)
    software_ok = evidence.poll_count > 0 and (
        evidence.data_count + evidence.eot_count > 0
        or evidence.timeout_count > 0  # offline fault runs still "succeed" software-wise
    )
    evidence.success = software_ok
    if physical_run and software_ok and evidence.crc_error_count == 0:
        evidence.physical_hil_status = "PASS"
    else:
        evidence.physical_hil_status = "NOT_RUN"
        if physical_run and not software_ok:
            evidence.physical_hil_status = "FAIL"
        evidence.notes.append(
            "physical_hil_status=NOT_RUN unless physical_run=True with healthy adapters"
        )

    if capture is not None:
        cap = capture.finalize()
        evidence.capture_path = str(cap)
        if capture._sanitized_path is not None:
            evidence.sanitized_capture_path = str(capture._sanitized_path)

    _write_outputs(evidence, evidence_path, report_path)
    return evidence


def _write_outputs(
    evidence: BenchEvidence,
    evidence_path: Path | str | None,
    report_path: Path | str | None,
) -> None:
    if evidence_path:
        # If evidence path ends with .jsonl, also write sibling .json summary.
        path = Path(evidence_path)
        json_path = path if path.suffix == ".json" else path.with_suffix(".json")
        write_evidence_json(evidence, json_path)
        evidence.evidence_json_path = str(json_path)
    if report_path:
        write_markdown_report(evidence, report_path)
        evidence.report_markdown_path = str(report_path)
    elif evidence_path:
        md = Path(evidence_path).with_suffix(".md")
        write_markdown_report(evidence, md)
        evidence.report_markdown_path = str(md)
