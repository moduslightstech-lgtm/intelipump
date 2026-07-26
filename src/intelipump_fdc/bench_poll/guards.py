"""Fail-closed startup guards for intelipump-poll-bench."""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from intelipump_fdc.capture.port_guards import (
    DEFAULT_CONTROLLER_SERVICE,
    DEFAULT_LOCK_DIR,
    AppDeviceLock,
    PassiveCaptureRefusedError,
    SystemctlRunner,
    assert_controller_service_inactive,
    assert_no_foreign_holders,
    find_device_holder_pids,
    is_virtual_or_test_port,
    resolve_canonical_device,
)
from intelipump_fdc.core.config import ControllerMode, Settings


class PollBenchRefusedError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "safety_refused") -> None:
        super().__init__(message)
        self.reason = reason

    def exit_message(self) -> str:
        return f"POLL_BENCH_REFUSED: {self}"


@dataclass(frozen=True, slots=True)
class PollBenchConfirmations:
    owned_lab_pump: bool = False
    technician_present: bool = False
    emergency_isolation_ready: bool = False
    no_fuel_test: bool = False
    authorization_disabled: bool = False

    def missing_flags(self) -> list[str]:
        missing: list[str] = []
        if not self.owned_lab_pump:
            missing.append("--confirm-owned-lab-pump")
        if not self.technician_present:
            missing.append("--confirm-technician-present")
        if not self.emergency_isolation_ready:
            missing.append("--confirm-emergency-isolation-ready")
        if not self.no_fuel_test:
            missing.append("--confirm-no-fuel-test")
        if not self.authorization_disabled:
            missing.append("--confirm-authorization-disabled")
        return missing


@dataclass(frozen=True, slots=True)
class PollBenchParams:
    port: str
    address: int
    baud: int
    max_polls: int
    response_timeout_ms: int
    evidence_dir: Path
    confirmations: PollBenchConfirmations
    controller_service: str = DEFAULT_CONTROLLER_SERVICE
    lock_dir: Path = DEFAULT_LOCK_DIR
    simulator_ports: tuple[str, ...] = ()
    skip_service_check: bool = False
    skip_port_check: bool = False


def _cmdline_of(pid: int) -> str:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return ""
    return raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").lower()


def _process_name(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/comm").read_text(encoding="utf-8").strip().lower()
    except OSError:
        return ""


def is_simulator_process(pid: int) -> bool:
    name = _process_name(pid)
    cmd = _cmdline_of(pid)
    markers = (
        "intelipump-simulator",
        "intelipump_fdc.simulator",
        "simulator.serial_cli",
        "simulator.cli",
    )
    if "simulator" in name and ("intelipump" in name or "intelipump" in cmd):
        return True
    return any(m in cmd for m in markers)


def assert_simulator_not_owning_adapters(
    ports: Sequence[str],
    *,
    holder_finder: Callable[[str], list[int]] | None = None,
    simulator_checker: Callable[[int], bool] | None = None,
) -> None:
    finder = holder_finder or find_device_holder_pids
    is_sim = simulator_checker or is_simulator_process
    for port in ports:
        if not port or is_virtual_or_test_port(port):
            continue
        try:
            canonical = resolve_canonical_device(port)
        except PassiveCaptureRefusedError:
            continue
        for pid in finder(canonical):
            if is_sim(pid):
                raise PollBenchRefusedError(
                    f"simulator process pid={pid} owns serial adapter "
                    f"{canonical} (requested={port})",
                    reason="simulator_owns_adapter",
                )


def validate_poll_bench_settings(settings: Settings) -> None:
    if settings.environment.upper() != "LAB":
        raise PollBenchRefusedError(
            f"environment must be LAB, got {settings.environment!r}",
            reason="non_lab",
        )
    if settings.controller.mode is not ControllerMode.POLL_ONLY_BENCH:
        raise PollBenchRefusedError(
            "INTELIPUMP_CONTROLLER__MODE must be POLL_ONLY_BENCH "
            f"(got {settings.controller.mode.value})",
            reason="wrong_mode",
        )
    if settings.safety.active_commands_enabled:
        raise PollBenchRefusedError(
            "active commands must be disabled", reason="active_commands"
        )
    if settings.safety.remote_authorization_enabled:
        raise PollBenchRefusedError(
            "remote authorization must be disabled", reason="remote_authorization"
        )
    if settings.safety.automatic_authorization_enabled:
        raise PollBenchRefusedError(
            "automatic authorization must be disabled",
            reason="automatic_authorization",
        )
    if settings.safety.command_replay_enabled:
        raise PollBenchRefusedError(
            "command replay must be disabled", reason="command_replay"
        )
    if settings.safety.allow_lab_simulator_commands:
        raise PollBenchRefusedError(
            "lab simulator commands must be disabled for real-pump bench",
            reason="lab_simulator_commands",
        )
    if settings.mqtt.enabled:
        raise PollBenchRefusedError("MQTT must be disabled", reason="mqtt_enabled")


def validate_poll_bench_params(params: PollBenchParams) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing confirmation flags: " + ", ".join(missing),
            reason="missing_confirmations",
        )
    if not (1 <= params.address <= 0xFF):
        raise PollBenchRefusedError(
            f"address must be 1-255, got {params.address}", reason="bad_address"
        )
    if not (1 <= params.max_polls <= 10):
        raise PollBenchRefusedError(
            f"max-polls must be 1-10, got {params.max_polls}", reason="bad_max_polls"
        )
    if params.baud not in {9600, 19200}:
        raise PollBenchRefusedError(
            f"baud must be 9600 or 19200, got {params.baud}", reason="bad_baud"
        )
    if params.response_timeout_ms < 1:
        raise PollBenchRefusedError(
            "response-timeout-ms must be >= 1", reason="bad_timeout"
        )


def ensure_evidence_dir_writable(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except OSError as exc:
        raise PollBenchRefusedError(
            f"evidence directory not writable: {path} ({exc})",
            reason="evidence_not_writable",
        ) from exc


def run_poll_bench_preflight(
    params: PollBenchParams,
    settings: Settings,
    *,
    systemctl_runner: SystemctlRunner | None = None,
    holder_finder: Callable[[str], list[int]] | None = None,
    simulator_checker: Callable[[int], bool] | None = None,
) -> tuple[str, AppDeviceLock | None]:
    """Validate all guards. Returns (canonical_port, app_lock)."""
    validate_poll_bench_settings(settings)
    validate_poll_bench_params(params)
    ensure_evidence_dir_writable(params.evidence_dir)

    if not params.skip_service_check and not is_virtual_or_test_port(params.port):
        try:
            assert_controller_service_inactive(
                params.controller_service, runner=systemctl_runner
            )
        except PassiveCaptureRefusedError as exc:
            raise PollBenchRefusedError(
                str(exc), reason="controller_service_active"
            ) from exc

    sim_ports = list(params.simulator_ports)
    if settings.bench.simulator_port:
        sim_ports.append(settings.bench.simulator_port)
    # Common udev alias used on the Pi lab.
    sim_ports.append("/dev/intelipump-simulator")
    assert_simulator_not_owning_adapters(
        sim_ports,
        holder_finder=holder_finder,
        simulator_checker=simulator_checker,
    )

    if is_virtual_or_test_port(params.port):
        return params.port, None

    try:
        canonical = resolve_canonical_device(params.port)
    except PassiveCaptureRefusedError as exc:
        raise PollBenchRefusedError(str(exc), reason=exc.reason.value) from exc

    app_lock: AppDeviceLock | None = None
    if not params.skip_port_check:
        try:
            app_lock = AppDeviceLock.acquire(
                requested_path=params.port,
                canonical_path=canonical,
                lock_dir=params.lock_dir,
                process_name="intelipump-poll-bench",
            )
            assert_no_foreign_holders(
                canonical,
                requested_path=params.port,
                holder_finder=holder_finder,
            )
            # Also ensure simulator is not on the controller canonical device.
            assert_simulator_not_owning_adapters(
                [canonical],
                holder_finder=holder_finder,
                simulator_checker=simulator_checker,
            )
        except PassiveCaptureRefusedError as exc:
            if app_lock is not None:
                app_lock.release()
            raise PollBenchRefusedError(str(exc), reason=exc.reason.value) from exc
        except PollBenchRefusedError:
            if app_lock is not None:
                app_lock.release()
            raise

    return canonical, app_lock


def software_commit() -> str:
    import subprocess

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
            cwd=os.getcwd(),
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass
    return "unknown"
