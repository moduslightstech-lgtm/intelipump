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
    find_device_holder_pids,
    is_virtual_or_test_port,
    resolve_canonical_device,
)
from intelipump_fdc.core.config import ControllerMode, Settings

DEFAULT_SIMULATOR_ALIAS = "/dev/intelipump-simulator"
DEFAULT_CONTROLLER_ALIAS = "/dev/intelipump-controller"

TARGET_SIMULATOR = "SIMULATOR"
TARGET_OWNED_LAB_WAYNE = "OWNED_LAB_WAYNE"


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
    simulator_validation: bool = False
    skip_service_check: bool = False
    skip_port_check: bool = False

    @property
    def target_type(self) -> str:
        if self.simulator_validation:
            return TARGET_SIMULATOR
        return TARGET_OWNED_LAB_WAYNE


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


def find_running_simulator_pids(
    *,
    simulator_checker: Callable[[int], bool] | None = None,
    proc_root: Path | None = None,
) -> list[int]:
    """Best-effort scan for running intelipump simulator processes."""
    is_sim = simulator_checker or is_simulator_process
    root = proc_root or Path("/proc")
    if not root.is_dir():
        return []
    found: list[int] = []
    self_pid = os.getpid()
    for entry in root.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == self_pid:
            continue
        try:
            if is_sim(pid):
                found.append(pid)
        except OSError:
            continue
    return sorted(found)


def _resolve_existing(path: str) -> str | None:
    if not path or is_virtual_or_test_port(path):
        return path or None
    try:
        return resolve_canonical_device(path)
    except PassiveCaptureRefusedError:
        return None


def known_simulator_canonicals(
    settings: Settings,
    extra_ports: Sequence[str] = (),
) -> set[str]:
    ports = list(extra_ports)
    if settings.bench.simulator_port:
        ports.append(settings.bench.simulator_port)
    ports.append(DEFAULT_SIMULATOR_ALIAS)
    out: set[str] = set()
    for port in ports:
        canonical = _resolve_existing(port)
        if canonical:
            out.add(canonical)
    return out


def assert_simulator_not_owning_adapters(
    ports: Sequence[str],
    *,
    holder_finder: Callable[[str], list[int]] | None = None,
    simulator_checker: Callable[[int], bool] | None = None,
) -> None:
    """Refuse when a simulator PID owns any of the given adapter paths."""
    finder = holder_finder or find_device_holder_pids
    is_sim = simulator_checker or is_simulator_process
    for port in ports:
        canonical = _resolve_existing(port)
        if not canonical:
            continue
        for pid in finder(canonical):
            if is_sim(pid):
                raise PollBenchRefusedError(
                    f"simulator process pid={pid} owns serial adapter "
                    f"{canonical} (requested={port})",
                    reason="simulator_owns_adapter",
                )


def assert_adapter_holders_allowed(
    *,
    port: str,
    canonical: str,
    allow_simulator: bool,
    holder_finder: Callable[[str], list[int]] | None = None,
    simulator_checker: Callable[[int], bool] | None = None,
) -> None:
    """Validate holders on one adapter.

    - ``allow_simulator=True``: only known simulator PIDs may hold the device.
    - ``allow_simulator=False``: any holder is refused (device must be free).
    """
    finder = holder_finder or find_device_holder_pids
    is_sim = simulator_checker or is_simulator_process
    holders = finder(canonical)
    if not holders:
        return
    if not allow_simulator:
        raise PollBenchRefusedError(
            f"serial device busy: canonical={canonical} requested={port} "
            f"holder_pids={holders}",
            reason="device_busy",
        )
    for pid in holders:
        if not is_sim(pid):
            raise PollBenchRefusedError(
                f"unrelated process pid={pid} owns serial adapter "
                f"{canonical} (requested={port})",
                reason="unrelated_holder",
            )


def validate_lab_bench_safety_settings(settings: Settings) -> None:
    """Shared LAB safety envelope (mode-agnostic)."""
    if settings.environment.upper() != "LAB":
        raise PollBenchRefusedError(
            f"environment must be LAB, got {settings.environment!r}",
            reason="non_lab",
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


def validate_poll_bench_settings(settings: Settings) -> None:
    validate_lab_bench_safety_settings(settings)
    if settings.controller.mode is not ControllerMode.POLL_ONLY_BENCH:
        raise PollBenchRefusedError(
            "INTELIPUMP_CONTROLLER__MODE must be POLL_ONLY_BENCH "
            f"(got {settings.controller.mode.value})",
            reason="wrong_mode",
        )


def validate_poll_bench_params(params: PollBenchParams) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing confirmation flags: " + ", ".join(missing),
            reason="missing_confirmations",
        )
    if params.address not in {1, 2}:
        raise PollBenchRefusedError(
            f"logical address must be 1 or 2 for captured legacy iGEM profile; "
            f"got {params.address}",
            reason="bad_address",
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
    simulator_pid_finder: Callable[..., list[int]] | None = None,
    enforce_poll_only_bench_mode: bool = True,
) -> tuple[str, AppDeviceLock | None]:
    """Validate all guards. Returns (canonical_port, app_lock).

    ``enforce_poll_only_bench_mode`` is required for ``intelipump-poll-bench``.
    Price dry-run/write tools set it False after applying their own mode allowlist
    (e.g. LISTEN_ONLY is valid for those CLIs).
    """
    if enforce_poll_only_bench_mode:
        validate_poll_bench_settings(settings)
    else:
        validate_lab_bench_safety_settings(settings)
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

    sim_canonicals = known_simulator_canonicals(settings, params.simulator_ports)
    find_sims = simulator_pid_finder or find_running_simulator_pids

    if not params.simulator_validation:
        # Real Wayne mode: any running simulator is refuse-closed.
        running = find_sims(simulator_checker=simulator_checker)
        if running:
            raise PollBenchRefusedError(
                f"simulator process running (pids={running}); "
                "refuse for real Wayne poll. Use --simulator-validation "
                "only for LAB simulator one-poll checks.",
                reason="simulator_running",
            )
        # Also refuse if a simulator still owns either adapter.
        check_ports = list(sim_canonicals)
        if params.port:
            check_ports.append(params.port)
        check_ports.append(DEFAULT_CONTROLLER_ALIAS)
        assert_simulator_not_owning_adapters(
            check_ports,
            holder_finder=holder_finder,
            simulator_checker=simulator_checker,
        )

    if is_virtual_or_test_port(params.port):
        return params.port, None

    try:
        controller_canonical = resolve_canonical_device(params.port)
    except PassiveCaptureRefusedError as exc:
        raise PollBenchRefusedError(str(exc), reason=exc.reason.value) from exc

    app_lock: AppDeviceLock | None = None
    if not params.skip_port_check:
        try:
            # Controller adapter must never be held by a simulator.
            assert_simulator_not_owning_adapters(
                [controller_canonical],
                holder_finder=holder_finder,
                simulator_checker=simulator_checker,
            )
            # Controller adapter must be free of all holders before exclusive open.
            assert_adapter_holders_allowed(
                port=params.port,
                canonical=controller_canonical,
                allow_simulator=False,
                holder_finder=holder_finder,
                simulator_checker=simulator_checker,
            )

            if params.simulator_validation:
                # Simulator may own only known simulator adapters; unrelated
                # holders on those adapters are refused.
                for sim_canonical in sim_canonicals:
                    if sim_canonical == controller_canonical:
                        raise PollBenchRefusedError(
                            "simulator adapter path resolves to the controller "
                            f"device ({controller_canonical})",
                            reason="simulator_controller_path_collision",
                        )
                    assert_adapter_holders_allowed(
                        port=sim_canonical,
                        canonical=sim_canonical,
                        allow_simulator=True,
                        holder_finder=holder_finder,
                        simulator_checker=simulator_checker,
                    )

            app_lock = AppDeviceLock.acquire(
                requested_path=params.port,
                canonical_path=controller_canonical,
                lock_dir=params.lock_dir,
                process_name="intelipump-poll-bench",
            )
        except PassiveCaptureRefusedError as exc:
            if app_lock is not None:
                app_lock.release()
            raise PollBenchRefusedError(str(exc), reason=exc.reason.value) from exc
        except PollBenchRefusedError:
            if app_lock is not None:
                app_lock.release()
            raise

    return controller_canonical, app_lock


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
