"""Hardware and RS-485 office-bench models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class SerialDeviceInfo:
    """Discovered USB/serial adapter metadata."""

    device_path: str
    stable_id: str
    vid: int | None = None
    pid: int | None = None
    serial_number: str | None = None
    manufacturer: str | None = None
    product: str | None = None
    interface: str | None = None
    usb_location: str | None = None
    hardware_id: str | None = None
    detected_driver: str | None = None
    description: str | None = None
    by_id_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AdapterValidationResult:
    device_path: str
    ok: bool
    baud_rate: int
    data_bits: int
    parity: str
    stop_bits: int
    flow_control_disabled: bool
    exclusive_open: bool | None
    read_timeout_s: float
    write_timeout_s: float
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BenchConfig:
    """Office RS-485 two-adapter bench configuration (LAB only)."""

    name: str = "us-office-rs485-01"
    environment: str = "LAB"
    controller_port: str = ""
    simulator_port: str = ""
    controller_adapter_stable_id: str | None = None
    simulator_adapter_stable_id: str | None = None
    baud_rate: int = 9600
    parity: str = "ODD"
    data_bits: int = 8
    stop_bits: int = 1
    response_timeout_ms: int = 100
    protocol_target_ms: int = 25
    turnaround_delay_ms: float = 2.0
    inter_frame_delay_ms: float = 5.0
    adapter_type: str = "usb-rs485-auto"
    automatic_direction_control: bool = True
    termination_enabled: bool = False
    bias_enabled: bool = False
    ground_reference_connected: bool = True
    addresses: tuple[int, ...] = (1, 2)
    duration_s: float = 30.0
    exclusive_open: bool = True

    def validate(self) -> None:
        from intelipump_fdc.hardware.errors import BenchConfigError

        if self.environment.upper() != "LAB":
            raise BenchConfigError("Phase 10 bench requires environment=LAB")
        if not self.controller_port or not self.simulator_port:
            raise BenchConfigError(
                "controller_port and simulator_port are required"
            )
        if self.controller_port == self.simulator_port:
            raise BenchConfigError(
                "controller_port and simulator_port must be distinct adapters"
            )
        if self.baud_rate != 9600:
            raise BenchConfigError("bench baud_rate must be 9600 for Phase 10")
        if self.parity.upper() != "ODD":
            raise BenchConfigError("bench parity must be ODD (no 8N1 fallback)")
        if self.data_bits != 8 or self.stop_bits != 1:
            raise BenchConfigError("bench requires 8 data bits and 1 stop bit")
        if not self.addresses:
            raise BenchConfigError("at least one DART address required")

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["addresses"] = list(self.addresses)
        return data


@dataclass
class LatencySample:
    address: int
    poll_to_response_ms: float
    response_kind: str
    timestamp: datetime


@dataclass
class LatencySummary:
    """Deprecated thin wrapper — prefer TimingStats from hardware.latency."""

    samples: list[LatencySample] = field(default_factory=list)
    protocol_target_ms: float = 25.0
    configured_bench_timeout_ms: float = 100.0

    def to_dict(self) -> dict[str, Any]:
        values = [s.poll_to_response_ms for s in self.samples]
        return {
            "count": len(values),
            "mean_ms": (sum(values) / len(values)) if values else None,
            "protocol_target_ms": self.protocol_target_ms,
            "configured_bench_timeout_ms": self.configured_bench_timeout_ms,
        }


@dataclass
class BenchEvidence:
    """Hardware metadata and test evidence for an office bench run."""

    bench: BenchConfig
    started_at: datetime
    finished_at: datetime | None = None
    controller_device: SerialDeviceInfo | None = None
    simulator_device: SerialDeviceInfo | None = None
    controller_validation: AdapterValidationResult | None = None
    simulator_validation: AdapterValidationResult | None = None
    latency: Any = None
    poll_count: int = 0
    data_count: int = 0
    eot_count: int = 0
    ack_count: int = 0
    nak_count: int = 0
    timeout_count: int = 0
    crc_error_count: int = 0
    retry_count: int = 0
    reconnect_events: int = 0
    fault_scenarios: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    listen_only: bool = True
    active_commands_enabled: bool = False
    success: bool = False
    # NOT_RUN until real two-adapter HIL executes successfully.
    physical_hil_status: str = "NOT_RUN"
    capture_path: str | None = None
    sanitized_capture_path: str | None = None
    evidence_json_path: str | None = None
    report_markdown_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        latency_dict: dict[str, Any]
        if self.latency is None:
            latency_dict = {}
        elif hasattr(self.latency, "to_dict"):
            latency_dict = self.latency.to_dict()
        else:
            latency_dict = {}
        return {
            "bench": self.bench.to_dict(),
            "started_at": self.started_at.isoformat(),
            "finished_at": (
                self.finished_at.isoformat() if self.finished_at else None
            ),
            "controller_device": (
                self.controller_device.to_dict() if self.controller_device else None
            ),
            "simulator_device": (
                self.simulator_device.to_dict() if self.simulator_device else None
            ),
            "controller_validation": (
                self.controller_validation.to_dict()
                if self.controller_validation
                else None
            ),
            "simulator_validation": (
                self.simulator_validation.to_dict()
                if self.simulator_validation
                else None
            ),
            "latency": latency_dict,
            "poll_count": self.poll_count,
            "data_count": self.data_count,
            "eot_count": self.eot_count,
            "ack_count": self.ack_count,
            "nak_count": self.nak_count,
            "timeout_count": self.timeout_count,
            "crc_error_count": self.crc_error_count,
            "retry_count": self.retry_count,
            "reconnect_events": self.reconnect_events,
            "fault_scenarios": list(self.fault_scenarios),
            "notes": list(self.notes),
            "listen_only": self.listen_only,
            "active_commands_enabled": self.active_commands_enabled,
            "success": self.success,
            "physical_hil_status": self.physical_hil_status,
            "capture_path": self.capture_path,
            "sanitized_capture_path": self.sanitized_capture_path,
            "evidence_json_path": self.evidence_json_path,
            "report_markdown_path": self.report_markdown_path,
        }
