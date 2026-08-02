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
    "REAL_WAYNE_OWNED_LAB_FAST_POLL_MIN_INTERVAL_MS",
    "SIMULATOR_EXTENDED_MAX_DURATION_S",
    "SIMULATOR_MAX_DURATION_S",
    "ContinuousPollBenchParams",
    "ContinuousPollConfirmations",
    "ContinuousPollRefusedError",
    "estimate_continuous_write_count",
    "run_continuous_poll_preflight",
    "software_commit",
]

# Short default path (no --confirm-extended-watch).
REAL_WAYNE_MAX_DURATION_S = 5
SIMULATOR_MAX_DURATION_S = 30
REAL_WAYNE_MAX_WRITES = 50
# Extended POLL-only / POLL+RS / POLL+CD101 watch (requires --confirm-extended-watch).
# Still bounded; Ctrl+C / SIGTERM stops early. Not a daemon / indefinite mode.
REAL_WAYNE_EXTENDED_MAX_DURATION_S = 300
SIMULATOR_EXTENDED_MAX_DURATION_S = 300
# Allows ~5 min @ 300 ms with RETURN_STATUS every 2 polls (~1500 TX).
REAL_WAYNE_EXTENDED_MAX_WRITES = 2000
# Default real-Wayne floor (production-safe lab default).
REAL_WAYNE_MIN_POLL_INTERVAL_MS = 300
# Owned-lab only: unlock with --confirm-owned-lab-fast-poll-100ms.
REAL_WAYNE_OWNED_LAB_FAST_POLL_MIN_INTERVAL_MS = 100
REAL_WAYNE_DEFAULT_RESPONSE_TIMEOUT_MS = 250
MALFORMED_THRESHOLD = 3


def estimate_continuous_write_count(
    *,
    duration_seconds: float,
    poll_interval_ms: int,
    return_status_cadence: bool,
    return_status_every_n_polls: int = 2,
    cd101_cadence: bool = False,
    cd101_every_n_polls: int = 4,
) -> int:
    """Approximate TX count: polls + optional RS / CD101 cadences.

    Conflict slots (both due) count both (slight overestimate); runtime
    defers CD101 when RETURN_STATUS wins the same slot.
    """
    duration_ms = duration_seconds * 1000.0
    polls = int((duration_ms - 1e-9) // poll_interval_ms) + 1
    extras = 0
    if return_status_cadence:
        every_n = max(1, int(return_status_every_n_polls))
        extras += (polls + every_n - 1) // every_n
    if cd101_cadence:
        every_m = max(1, int(cd101_every_n_polls))
        extras += (polls + every_m - 1) // every_m
    return polls + extras


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
    # Run until SIGINT/SIGTERM (no wall-clock duration stop).
    until_ctrl_c: bool = False
    # Option 2: interleave gated CD1 RETURN_STATUS (no RESET/AUTHORIZE/CD5).
    return_status_cadence: bool = False
    # Option 3: interleave gated CD101 request-totals (no RESET/AUTHORIZE/CD5).
    cd101_cadence: bool = False
    no_reset_no_authorize: bool = False
    # Owned-lab only: allow real-Wayne poll-interval-ms down to 100.
    owned_lab_fast_poll_100ms: bool = False

    def missing_flags(self) -> list[str]:
        missing: list[str] = []
        mapping = (
            (self.owned_lab_pump, "--confirm-owned-lab-pump"),
            (self.technician_present, "--confirm-technician-present"),
            (self.emergency_isolation_ready, "--confirm-emergency-isolation-ready"),
            (self.no_fuel_test, "--confirm-no-fuel-test"),
            (self.authorization_disabled, "--confirm-authorization-disabled"),
        )
        for ok, flag in mapping:
            if not ok:
                missing.append(flag)
        if self.until_ctrl_c:
            pass  # --confirm-until-ctrl-c already set on this object
        elif not self.bounded_duration:
            missing.append("--confirm-bounded-duration")
        active_cadence = self.return_status_cadence or self.cd101_cadence
        if active_cadence:
            if not self.no_reset_no_authorize:
                missing.append("--confirm-no-reset-no-authorize")
        elif not self.status_poll_only:
            missing.append("--confirm-status-poll-only")
        return missing


@dataclass(frozen=True, slots=True)
class ContinuousPollBenchParams:
    port: str
    addresses: tuple[int, ...]
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
    cd101_every_n_polls: int = 4
    cd101_counter_select: int = 1

    @property
    def address(self) -> int:
        """Primary / first logical address (compat)."""
        return self.addresses[0]

    @property
    def target_type(self) -> str:
        return TARGET_SIMULATOR if self.simulator_validation else TARGET_OWNED_LAB_WAYNE

    @property
    def extended_watch(self) -> bool:
        return self.confirmations.extended_watch

    @property
    def until_ctrl_c(self) -> bool:
        return self.confirmations.until_ctrl_c

    @property
    def return_status_cadence(self) -> bool:
        return self.confirmations.return_status_cadence

    @property
    def cd101_cadence(self) -> bool:
        return self.confirmations.cd101_cadence

    @property
    def max_duration_s(self) -> int:
        if self.until_ctrl_c:
            # Wall-clock duration is unused; keep a large sentinel for messages.
            return REAL_WAYNE_EXTENDED_MAX_DURATION_S
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
        """0 means unlimited (until-ctrl-c mode only)."""
        if self.until_ctrl_c:
            return 0
        approx = estimate_continuous_write_count(
            duration_seconds=self.duration_seconds,
            poll_interval_ms=self.poll_interval_ms,
            return_status_cadence=self.return_status_cadence,
            return_status_every_n_polls=self.return_status_every_n_polls,
            cd101_cadence=self.cd101_cadence,
            cd101_every_n_polls=self.cd101_every_n_polls,
        ) + 2
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
    if not params.addresses:
        raise ContinuousPollRefusedError(
            "at least one --address is required", reason="bad_address"
        )
    if len(params.addresses) > 2:
        raise ContinuousPollRefusedError(
            "at most two addresses (1 and/or 2) on one RS-485 adapter",
            reason="bad_address",
        )
    if len(set(params.addresses)) != len(params.addresses):
        raise ContinuousPollRefusedError(
            f"duplicate addresses not allowed: {params.addresses}",
            reason="bad_address",
        )
    for addr in params.addresses:
        if addr not in {1, 2}:
            raise ContinuousPollRefusedError(
                f"logical address must be 1 or 2 for captured legacy iGEM "
                f"profile; got {addr}",
                reason="bad_address",
            )
    if (
        params.confirmations.until_ctrl_c
        and params.confirmations.bounded_duration
    ):
        raise ContinuousPollRefusedError(
            "cannot combine --confirm-bounded-duration with "
            "--confirm-until-ctrl-c; omit bounded-duration for continuous "
            "until-Ctrl+C mode",
            reason="conflicting_confirmations",
        )
    if not params.confirmations.until_ctrl_c:
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
                f"{(
                    SIMULATOR_EXTENDED_MAX_DURATION_S
                    if params.simulator_validation
                    else REAL_WAYNE_EXTENDED_MAX_DURATION_S
                )}s, "
                f"Ctrl+C to stop early), pass --confirm-extended-watch. "
                f"For continuous until Ctrl+C, pass --confirm-until-ctrl-c.",
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
    active_cadence = (
        params.confirmations.return_status_cadence
        or params.confirmations.cd101_cadence
    )
    if active_cadence and params.confirmations.status_poll_only:
        raise ContinuousPollRefusedError(
            "cannot combine --confirm-status-poll-only with "
            "--confirm-return-status-cadence or --confirm-cd101-cadence; "
            "omit status-poll-only for POLL+cadence watch",
            reason="conflicting_confirmations",
        )
    if active_cadence:
        if not (
            params.confirmations.extended_watch
            or params.confirmations.until_ctrl_c
        ):
            raise ContinuousPollRefusedError(
                "return-status-cadence / cd101-cadence requires "
                "--confirm-extended-watch or --confirm-until-ctrl-c",
                reason="cadence_requires_extended",
            )
        if not (0 <= params.return_status_sequence <= 15):
            raise ContinuousPollRefusedError(
                "sequence must be 0-15 (shared DATA sequence nibble for "
                "RETURN_STATUS and/or CD101)",
                reason="bad_sequence",
            )
        if not (50 <= params.ack_timeout_ms < params.poll_interval_ms):
            raise ContinuousPollRefusedError(
                "ack-timeout-ms must be >= 50 and < poll-interval-ms "
                "(example at 100 ms poll: ack 70, response 80)",
                reason="bad_ack_timeout",
            )
    if params.confirmations.return_status_cadence and not (
        1 <= params.return_status_every_n_polls <= 20
    ):
        raise ContinuousPollRefusedError(
            "return-status-every-n-polls must be 1-20",
            reason="bad_return_status_every_n",
        )
    if params.confirmations.cd101_cadence and not (
        1 <= params.cd101_every_n_polls <= 20
    ):
        raise ContinuousPollRefusedError(
            "cd101-every-n-polls must be 1-20",
            reason="bad_cd101_every_n",
        )
    if params.confirmations.cd101_cadence and not (
        0 <= params.cd101_counter_select <= 0xFF
    ):
        raise ContinuousPollRefusedError(
            "cd101-counter-select must be 0-255 (ePump-like default 1)",
            reason="bad_cd101_counter_select",
        )
    if not params.simulator_validation:
        if params.confirmations.owned_lab_fast_poll_100ms:
            min_interval = REAL_WAYNE_OWNED_LAB_FAST_POLL_MIN_INTERVAL_MS
            interval_hint = (
                f">= {min_interval} with "
                f"--confirm-owned-lab-fast-poll-100ms"
            )
        else:
            min_interval = REAL_WAYNE_MIN_POLL_INTERVAL_MS
            interval_hint = (
                f">= {min_interval} (or pass "
                f"--confirm-owned-lab-fast-poll-100ms for owned-lab "
                f"fast path down to "
                f"{REAL_WAYNE_OWNED_LAB_FAST_POLL_MIN_INTERVAL_MS} ms)"
            )
        if params.poll_interval_ms < min_interval:
            raise ContinuousPollRefusedError(
                f"real-Wayne poll-interval-ms must be {interval_hint} "
                f"(got {params.poll_interval_ms})",
                reason="real_wayne_poll_interval",
            )
        if not params.until_ctrl_c:
            # Real-Wayne: hard-cap computed writes (polls + optional cadences).
            approx = estimate_continuous_write_count(
                duration_seconds=params.duration_seconds,
                poll_interval_ms=params.poll_interval_ms,
                return_status_cadence=params.return_status_cadence,
                return_status_every_n_polls=params.return_status_every_n_polls,
                cd101_cadence=params.cd101_cadence,
                cd101_every_n_polls=params.cd101_every_n_polls,
            )
            write_cap = (
                REAL_WAYNE_EXTENDED_MAX_WRITES
                if params.extended_watch
                else REAL_WAYNE_MAX_WRITES
            )
            if approx > write_cap:
                raise ContinuousPollRefusedError(
                    f"computed write count {approx} exceeds real-Wayne max "
                    f"{write_cap}; reduce duration, increase interval, "
                    f"raise cadence every-n, or use "
                    f"--confirm-until-ctrl-c",
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
