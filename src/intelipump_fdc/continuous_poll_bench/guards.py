"""Fail-closed startup guards for intelipump-continuous-poll-bench."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from intelipump_fdc.bench_poll.guards import (
    DEFAULT_CONTROLLER_ALIAS,
    TARGET_OWNED_LAB_WAYNE,
    TARGET_SIMULATOR,
    PollBenchRefusedError,
    assert_adapter_holders_allowed,
    assert_simulator_not_owning_adapters,
    ensure_evidence_dir_writable,
    find_running_simulator_pids,
    known_simulator_canonicals,
    software_commit,
)
from intelipump_fdc.capture.port_guards import (
    DEFAULT_CONTROLLER_SERVICE,
    DEFAULT_LOCK_DIR,
    AppDeviceLock,
    PassiveCaptureRefusedError,
    SystemctlRunner,
    assert_controller_service_inactive,
    is_virtual_or_test_port,
    resolve_canonical_device,
)
from intelipump_fdc.core.config import ControllerMode, Settings

# Re-export for callers/tests.
__all__ = [
    "REAL_WAYNE_DEFAULT_RESPONSE_TIMEOUT_MS",
    "REAL_WAYNE_EXTENDED_MAX_DURATION_S",
    "REAL_WAYNE_EXTENDED_MAX_WRITES",
    "REAL_WAYNE_MAX_DURATION_S",
    "REAL_WAYNE_MAX_WRITES",
    "REAL_WAYNE_MIN_POLL_INTERVAL_MS",
    "SIMULATOR_EXTENDED_MAX_DURATION_S",
    "SIMULATOR_MAX_DURATION_S",
    "ContinuousPollBenchParams",
    "ContinuousPollConfirmations",
    "ContinuousPollRefusedError",
    "run_continuous_poll_preflight",
    "software_commit",
]

# Short default path (no --confirm-extended-watch).
REAL_WAYNE_MAX_DURATION_S = 5
SIMULATOR_MAX_DURATION_S = 30
REAL_WAYNE_MAX_WRITES = 50
# Extended POLL-only watch (requires --confirm-extended-watch). Still bounded;
# Ctrl+C / SIGTERM stops early. Not a daemon / indefinite mode.
REAL_WAYNE_EXTENDED_MAX_DURATION_S = 300
SIMULATOR_EXTENDED_MAX_DURATION_S = 300
REAL_WAYNE_EXTENDED_MAX_WRITES = 1000
REAL_WAYNE_MIN_POLL_INTERVAL_MS = 300
REAL_WAYNE_DEFAULT_RESPONSE_TIMEOUT_MS = 250
MALFORMED_THRESHOLD = 3


class ContinuousPollRefusedError(PollBenchRefusedError):
    def exit_message(self) -> str:
        return f"CONTINUOUS_POLL_BENCH_REFUSED: {self}"


@dataclass(frozen=True, slots=True)
class ContinuousPollConfirmations:
    owned_lab_pump: bool = False
    technician_present: bool = False
    emergency_isolation_ready: bool = False
    no_fuel_test: bool = False
    authorization_disabled: bool = False
    status_poll_only: bool = False
    bounded_duration: bool = False
    # Optional: required only when duration exceeds the short-path max.
    extended_watch: bool = False
    # Option 2: interleave gated CD1 RETURN_STATUS (no RESET/AUTHORIZE).
    return_status_cadence: bool = False
    no_reset_no_authorize: bool = False

    def missing_flags(self) -> list[str]:
        missing: list[str] = []
        mapping = (
            (self.owned_lab_pump, "--confirm-owned-lab-pump"),
            (self.technician_present, "--confirm-technician-present"),
            (self.emergency_isolation_ready, "--confirm-emergency-isolation-ready"),
            (self.no_fuel_test, "--confirm-no-fuel-test"),
            (self.authorization_disabled, "--confirm-authorization-disabled"),
            (self.bounded_duration, "--confirm-bounded-duration"),
        )
        for ok, flag in mapping:
            if not ok:
                missing.append(flag)
        if self.return_status_cadence:
            if not self.no_reset_no_authorize:
                missing.append("--confirm-no-reset-no-authorize")
        elif not self.status_poll_only:
            missing.append("--confirm-status-poll-only")
        return missing


@dataclass(frozen=True, slots=True)
class ContinuousPollBenchParams:
    port: str
    address: int
    duration_seconds: float
    poll_interval_ms: int
    response_timeout_ms: int
    evidence_dir: Path
    confirmations: ContinuousPollConfirmations
    baud: int = 9600
    controller_service: str = DEFAULT_CONTROLLER_SERVICE
    lock_dir: Path = DEFAULT_LOCK_DIR
    simulator_ports: tuple[str, ...] = ()
    simulator_validation: bool = False
    skip_service_check: bool = False
    skip_port_check: bool = False
    return_status_every_n_polls: int = 2
    return_status_sequence: int = 0
    ack_timeout_ms: int = 200

    @property
    def target_type(self) -> str:
        return TARGET_SIMULATOR if self.simulator_validation else TARGET_OWNED_LAB_WAYNE

    @property
    def extended_watch(self) -> bool:
        return self.confirmations.extended_watch

    @property
    def return_status_cadence(self) -> bool:
        return self.confirmations.return_status_cadence

    @property
    def max_duration_s(self) -> int:
        if self.extended_watch:
            return (
                SIMULATOR_EXTENDED_MAX_DURATION_S
                if self.simulator_validation
                else REAL_WAYNE_EXTENDED_MAX_DURATION_S
            )
        return (
            SIMULATOR_MAX_DURATION_S
            if self.simulator_validation
            else REAL_WAYNE_MAX_DURATION_S
        )

    @property
    def max_writes(self) -> int:
        # Bound by duration/interval (+2 for edge) but never unbounded.
        # RETURN_STATUS cadence can roughly double TX count → use 2x when enabled.
        factor = 2 if self.return_status_cadence else 1
        approx = (
            int(self.duration_seconds * 1000 / self.poll_interval_ms) + 2
        ) * factor
        if self.simulator_validation:
            cap = (
                REAL_WAYNE_EXTENDED_MAX_WRITES
                if self.extended_watch
                else 600
            )
            return min(approx, cap)
        if self.extended_watch:
            return min(approx, REAL_WAYNE_EXTENDED_MAX_WRITES)
        return REAL_WAYNE_MAX_WRITES


def validate_continuous_settings(settings: Settings) -> None:
    if settings.environment.upper() != "LAB":
        raise ContinuousPollRefusedError(
            f"environment must be LAB, got {settings.environment!r}",
            reason="non_lab",
        )
    if settings.controller.mode is not ControllerMode.CONTINUOUS_POLL_BENCH:
        raise ContinuousPollRefusedError(
            "INTELIPUMP_CONTROLLER__MODE must be CONTINUOUS_POLL_BENCH "
            f"(got {settings.controller.mode.value})",
            reason="wrong_mode",
        )
    if settings.safety.active_commands_enabled:
        raise ContinuousPollRefusedError(
            "active commands must be disabled", reason="active_commands"
        )
    if settings.safety.remote_authorization_enabled:
        raise ContinuousPollRefusedError(
            "remote authorization must be disabled", reason="remote_authorization"
        )
    if settings.safety.automatic_authorization_enabled:
        raise ContinuousPollRefusedError(
            "automatic authorization must be disabled",
            reason="automatic_authorization",
        )
    if settings.safety.command_replay_enabled:
        raise ContinuousPollRefusedError(
            "command replay must be disabled", reason="command_replay"
        )
    if settings.safety.allow_lab_simulator_commands:
        raise ContinuousPollRefusedError(
            "lab simulator commands must be disabled",
            reason="lab_simulator_commands",
        )
    if settings.mqtt.enabled:
        raise ContinuousPollRefusedError("MQTT must be disabled", reason="mqtt_enabled")


def validate_continuous_params(params: ContinuousPollBenchParams) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise ContinuousPollRefusedError(
            "missing confirmation flags: " + ", ".join(missing),
            reason="missing_confirmations",
        )
    if params.address not in {1, 2}:
        raise ContinuousPollRefusedError(
            f"logical address must be 1 or 2 for captured legacy iGEM profile; "
            f"got {params.address}",
            reason="bad_address",
        )
    if params.duration_seconds < 1:
        raise ContinuousPollRefusedError(
            "duration-seconds must be >= 1", reason="bad_duration"
        )
    short_max = (
        SIMULATOR_MAX_DURATION_S
        if params.simulator_validation
        else REAL_WAYNE_MAX_DURATION_S
    )
    if (
        params.duration_seconds > short_max
        and not params.confirmations.extended_watch
    ):
        raise ContinuousPollRefusedError(
            f"duration-seconds max is {short_max} without "
            f"--confirm-extended-watch "
            f"({'simulator' if params.simulator_validation else 'real-Wayne'} "
            f"short path); got {params.duration_seconds}. "
            f"For a longer POLL-only watch (max "
            f"{REAL_WAYNE_EXTENDED_MAX_DURATION_S if not params.simulator_validation else SIMULATOR_EXTENDED_MAX_DURATION_S}s, "
            f"Ctrl+C to stop early), pass --confirm-extended-watch.",
            reason="duration_exceeded",
        )
    if params.duration_seconds > params.max_duration_s:
        raise ContinuousPollRefusedError(
            f"duration-seconds max is {params.max_duration_s} "
            f"({'simulator' if params.simulator_validation else 'real-Wayne'}"
            f"{' extended-watch' if params.extended_watch else ''} mode); "
            f"got {params.duration_seconds}",
            reason="duration_exceeded",
        )
    if not (50 <= params.poll_interval_ms <= 1000):
        raise ContinuousPollRefusedError(
            "poll-interval-ms must be 50-1000", reason="bad_poll_interval"
        )
    if params.response_timeout_ms < 1:
        raise ContinuousPollRefusedError(
            "response-timeout-ms must be >= 1", reason="bad_timeout"
        )
    if params.response_timeout_ms >= params.poll_interval_ms:
        raise ContinuousPollRefusedError(
            "response-timeout-ms must be less than poll-interval-ms "
            "(no overlapping catch-up polls)",
            reason="timeout_vs_interval",
        )
    if params.baud not in {9600, 19200}:
        raise ContinuousPollRefusedError(
            f"baud must be 9600 or 19200, got {params.baud}", reason="bad_baud"
        )
    if (
        params.confirmations.return_status_cadence
        and params.confirmations.status_poll_only
    ):
        raise ContinuousPollRefusedError(
            "cannot combine --confirm-status-poll-only with "
            "--confirm-return-status-cadence; omit status-poll-only for "
            "POLL+RETURN_STATUS watch",
            reason="conflicting_confirmations",
        )
    if params.confirmations.return_status_cadence:
        if not params.confirmations.extended_watch:
            raise ContinuousPollRefusedError(
                "return-status-cadence requires --confirm-extended-watch "
                "(bounded longer watch; Ctrl+C to stop early)",
                reason="return_status_requires_extended",
            )
        if not (1 <= params.return_status_every_n_polls <= 20):
            raise ContinuousPollRefusedError(
                "return-status-every-n-polls must be 1-20",
                reason="bad_return_status_every_n",
            )
        if not (0 <= params.return_status_sequence <= 15):
            raise ContinuousPollRefusedError(
                "sequence must be 0-15 (DATA sequence nibble)",
                reason="bad_sequence",
            )
        if not (50 <= params.ack_timeout_ms < params.poll_interval_ms):
            raise ContinuousPollRefusedError(
                "ack-timeout-ms must be >= 50 and < poll-interval-ms",
                reason="bad_ack_timeout",
            )
    if not params.simulator_validation:
        if params.poll_interval_ms < REAL_WAYNE_MIN_POLL_INTERVAL_MS:
            raise ContinuousPollRefusedError(
                f"real-Wayne poll-interval-ms must be >= "
                f"{REAL_WAYNE_MIN_POLL_INTERVAL_MS} (got {params.poll_interval_ms})",
                reason="real_wayne_poll_interval",
            )
        # Real-Wayne: hard-cap computed writes (polls + optional RETURN_STATUS).
        duration_ms = params.duration_seconds * 1000.0
        approx_polls = int((duration_ms - 1e-9) // params.poll_interval_ms) + 1
        factor = 2 if params.return_status_cadence else 1
        approx = approx_polls * factor
        write_cap = (
            REAL_WAYNE_EXTENDED_MAX_WRITES
            if params.extended_watch
            else REAL_WAYNE_MAX_WRITES
        )
        if approx > write_cap:
            raise ContinuousPollRefusedError(
                f"computed write count {approx} exceeds real-Wayne max "
                f"{write_cap}; reduce duration, increase interval, or "
                f"raise return-status-every-n-polls",
                reason="max_writes_exceeded",
            )


def run_continuous_poll_preflight(
    params: ContinuousPollBenchParams,
    settings: Settings,
    *,
    systemctl_runner: SystemctlRunner | None = None,
    holder_finder: Callable[[str], list[int]] | None = None,
    simulator_checker: Callable[[int], bool] | None = None,
    simulator_pid_finder: Callable[..., list[int]] | None = None,
) -> tuple[str, AppDeviceLock | None]:
    validate_continuous_settings(settings)
    validate_continuous_params(params)
    try:
        ensure_evidence_dir_writable(params.evidence_dir)
    except PollBenchRefusedError as exc:
        raise ContinuousPollRefusedError(str(exc), reason=exc.reason) from exc


    if not params.skip_service_check and not is_virtual_or_test_port(params.port):
        try:
            assert_controller_service_inactive(
                params.controller_service, runner=systemctl_runner
            )
        except PassiveCaptureRefusedError as exc:
            raise ContinuousPollRefusedError(
                str(exc), reason="controller_service_active"
            ) from exc

    sim_canonicals = known_simulator_canonicals(settings, params.simulator_ports)
    find_sims = simulator_pid_finder or find_running_simulator_pids

    if not params.simulator_validation:
        running = find_sims(simulator_checker=simulator_checker)
        if running:
            raise ContinuousPollRefusedError(
                f"simulator process running (pids={running}); "
                "refuse for real Wayne continuous poll. "
                "Use --simulator-validation for LAB simulator checks.",
                reason="simulator_running",
            )
        check_ports = list(sim_canonicals)
        check_ports.append(params.port)
        check_ports.append(DEFAULT_CONTROLLER_ALIAS)
        try:
            assert_simulator_not_owning_adapters(
                check_ports,
                holder_finder=holder_finder,
                simulator_checker=simulator_checker,
            )
        except PollBenchRefusedError as exc:
            raise ContinuousPollRefusedError(str(exc), reason=exc.reason) from exc

    if is_virtual_or_test_port(params.port):
        return params.port, None

    try:
        controller_canonical = resolve_canonical_device(params.port)
    except PassiveCaptureRefusedError as exc:
        raise ContinuousPollRefusedError(str(exc), reason=exc.reason.value) from exc

    app_lock: AppDeviceLock | None = None
    if not params.skip_port_check:
        try:
            assert_simulator_not_owning_adapters(
                [controller_canonical],
                holder_finder=holder_finder,
                simulator_checker=simulator_checker,
            )
            assert_adapter_holders_allowed(
                port=params.port,
                canonical=controller_canonical,
                allow_simulator=False,
                holder_finder=holder_finder,
                simulator_checker=simulator_checker,
            )
            if params.simulator_validation:
                for sim_canonical in sim_canonicals:
                    if sim_canonical == controller_canonical:
                        raise ContinuousPollRefusedError(
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
                process_name="intelipump-continuous-poll-bench",
            )
        except PassiveCaptureRefusedError as exc:
            if app_lock is not None:
                app_lock.release()
            raise ContinuousPollRefusedError(
                str(exc), reason=exc.reason.value
            ) from exc
        except PollBenchRefusedError as exc:
            if app_lock is not None:
                app_lock.release()
            if isinstance(exc, ContinuousPollRefusedError):
                raise
            raise ContinuousPollRefusedError(str(exc), reason=exc.reason) from exc

    return controller_canonical, app_lock
