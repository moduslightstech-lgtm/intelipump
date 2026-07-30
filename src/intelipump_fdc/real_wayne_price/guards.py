"""Fail-closed confirmations for real-Wayne CD5 price dry-run."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from intelipump_fdc.bench_poll.guards import (
    TARGET_OWNED_LAB_WAYNE,
    PollBenchRefusedError,
    software_commit,
)
from intelipump_fdc.core.config import ControllerMode, Settings


@dataclass(frozen=True, slots=True)
class PriceDryRunConfirmations:
    owned_lab_pump: bool = False
    technician_present: bool = False
    no_product_connected: bool = False
    motor_isolated: bool = False
    valves_isolated: bool = False
    emergency_isolation_ready: bool = False
    authorization_disabled: bool = False
    single_write_plan_reviewed: bool = False
    price_scale_confirmed: bool = False
    logical_nozzle_mapping_confirmed: bool = False

    def missing_flags(self) -> list[str]:
        mapping = {
            "owned_lab_pump": "--confirm-owned-lab-pump",
            "technician_present": "--confirm-technician-present",
            "no_product_connected": "--confirm-no-product-connected",
            "motor_isolated": "--confirm-motor-isolated",
            "valves_isolated": "--confirm-valves-isolated",
            "emergency_isolation_ready": "--confirm-emergency-isolation-ready",
            "authorization_disabled": "--confirm-authorization-disabled",
            "single_write_plan_reviewed": "--confirm-single-write-plan-reviewed",
            "price_scale_confirmed": "--price-scale-confirmed-by-technician",
            "logical_nozzle_mapping_confirmed": (
                "--logical-nozzle-mapping-confirmed-by-technician"
            ),
        }
        missing: list[str] = []
        for attr, flag in mapping.items():
            if not getattr(self, attr):
                missing.append(flag)
        return missing

    def to_dict(self) -> dict[str, bool]:
        return {
            "ownedLabPump": self.owned_lab_pump,
            "technicianPresent": self.technician_present,
            "noProductConnected": self.no_product_connected,
            "motorIsolated": self.motor_isolated,
            "valvesIsolated": self.valves_isolated,
            "emergencyIsolationReady": self.emergency_isolation_ready,
            "authorizationDisabled": self.authorization_disabled,
            "singleWritePlanReviewed": self.single_write_plan_reviewed,
            "priceScaleConfirmed": self.price_scale_confirmed,
            "logicalNozzleMappingConfirmed": self.logical_nozzle_mapping_confirmed,
        }


@dataclass(frozen=True, slots=True)
class PriceDryRunParams:
    port: str
    address: int
    logical_nozzle_count: int
    price_nozzle_1: int
    price_nozzle_2: int | None
    evidence_dir: Path
    confirmations: PriceDryRunConfirmations
    baud: int = 9600
    response_timeout_ms: int = 250
    sequence: int = 0
    skip_service_check: bool = False
    skip_port_check: bool = False

    @property
    def target_type(self) -> str:
        return TARGET_OWNED_LAB_WAYNE

    def prices_dict(self) -> dict[int, int]:
        if self.logical_nozzle_count == 1:
            return {1: self.price_nozzle_1}
        if self.logical_nozzle_count == 2:
            if self.price_nozzle_2 is None:
                raise PollBenchRefusedError(
                    "price-nozzle-2 required when logical-nozzle-count=2"
                )
            return {1: self.price_nozzle_1, 2: self.price_nozzle_2}
        raise PollBenchRefusedError(
            f"unsupported logical-nozzle-count={self.logical_nozzle_count}"
        )


def validate_price_dry_run_params(params: PriceDryRunParams) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing required confirmations: " + ", ".join(missing)
        )
    if params.address not in {1, 2}:
        raise PollBenchRefusedError("address must be 1 or 2")
    if params.logical_nozzle_count not in {1, 2}:
        raise PollBenchRefusedError("logical-nozzle-count must be 1 or 2")
    if params.logical_nozzle_count == 2 and params.price_nozzle_2 is None:
        raise PollBenchRefusedError("--price-nozzle-2 required for two nozzles")
    if params.logical_nozzle_count == 1 and params.price_nozzle_2 is not None:
        raise PollBenchRefusedError(
            "--price-nozzle-2 not allowed when logical-nozzle-count=1"
        )


def validate_price_dry_run_settings(settings: Settings) -> None:
    if settings.environment.upper() != "LAB":
        raise PollBenchRefusedError("price dry-run requires LAB environment")
    if settings.controller.mode not in {
        ControllerMode.LISTEN_ONLY,
        ControllerMode.POLL_ONLY_BENCH,
        ControllerMode.CONTINUOUS_POLL_BENCH,
        ControllerMode.BENCH_CONTROL,
    }:
        raise PollBenchRefusedError(
            f"controller mode {settings.controller.mode.value} refuses price dry-run"
        )
    if settings.safety.active_commands_enabled:
        raise PollBenchRefusedError("active_commands_enabled must be false")
    if settings.safety.remote_authorization_enabled:
        raise PollBenchRefusedError("remote_authorization_enabled must be false")
    if settings.safety.automatic_authorization_enabled:
        raise PollBenchRefusedError("automatic_authorization_enabled must be false")
    if settings.safety.command_replay_enabled:
        raise PollBenchRefusedError("command_replay_enabled must be false")


@dataclass(frozen=True, slots=True)
class PriceWriteConfirmations:
    """All dry-run confirms plus explicit execute-write authorization."""

    owned_lab_pump: bool = False
    technician_present: bool = False
    no_product_connected: bool = False
    motor_isolated: bool = False
    valves_isolated: bool = False
    emergency_isolation_ready: bool = False
    authorization_disabled: bool = False
    single_write_plan_reviewed: bool = False
    price_scale_confirmed: bool = False
    logical_nozzle_mapping_confirmed: bool = False
    execute_cd5_write: bool = False
    post_write_status_verification_required: bool = False
    understand_transmits_to_owned_lab_pump: bool = False

    def missing_flags(self) -> list[str]:
        mapping = {
            "owned_lab_pump": "--confirm-owned-lab-pump",
            "technician_present": "--confirm-technician-present",
            "no_product_connected": "--confirm-no-product-connected",
            "motor_isolated": "--confirm-motor-isolated",
            "valves_isolated": "--confirm-valves-isolated",
            "emergency_isolation_ready": "--confirm-emergency-isolation-ready",
            "authorization_disabled": "--confirm-authorization-disabled",
            "single_write_plan_reviewed": "--confirm-single-write-plan-reviewed",
            "price_scale_confirmed": "--price-scale-confirmed-by-technician",
            "logical_nozzle_mapping_confirmed": (
                "--logical-nozzle-mapping-confirmed-by-technician"
            ),
            "execute_cd5_write": "--confirm-execute-cd5-write",
            "post_write_status_verification_required": (
                "--confirm-post-write-status-verification-required"
            ),
            "understand_transmits_to_owned_lab_pump": (
                "--i-understand-this-transmits-to-owned-lab-pump"
            ),
        }
        return [flag for attr, flag in mapping.items() if not getattr(self, attr)]

    def to_dict(self) -> dict[str, bool]:
        return {
            "ownedLabPump": self.owned_lab_pump,
            "technicianPresent": self.technician_present,
            "noProductConnected": self.no_product_connected,
            "motorIsolated": self.motor_isolated,
            "valvesIsolated": self.valves_isolated,
            "emergencyIsolationReady": self.emergency_isolation_ready,
            "authorizationDisabled": self.authorization_disabled,
            "singleWritePlanReviewed": self.single_write_plan_reviewed,
            "priceScaleConfirmed": self.price_scale_confirmed,
            "logicalNozzleMappingConfirmed": self.logical_nozzle_mapping_confirmed,
            "executeCd5Write": self.execute_cd5_write,
            "postWriteStatusVerificationRequired": (
                self.post_write_status_verification_required
            ),
            "understandTransmitsToOwnedLabPump": (
                self.understand_transmits_to_owned_lab_pump
            ),
        }


@dataclass(frozen=True, slots=True)
class PriceWriteParams:
    port: str
    address: int
    logical_nozzle_count: int
    price_nozzle_1: int
    price_nozzle_2: int | None
    evidence_dir: Path
    confirmations: PriceWriteConfirmations
    baud: int = 9600
    response_timeout_ms: int = 500
    ack_timeout_ms: int = 250
    post_write_settle_ms: int = 1000
    post_write_max_status_polls: int = 16
    sequence: int = 0
    skip_service_check: bool = False
    skip_port_check: bool = False

    @property
    def target_type(self) -> str:
        return TARGET_OWNED_LAB_WAYNE

    def prices_dict(self) -> dict[int, int]:
        if self.logical_nozzle_count == 1:
            return {1: self.price_nozzle_1}
        if self.logical_nozzle_count == 2:
            if self.price_nozzle_2 is None:
                raise PollBenchRefusedError(
                    "price-nozzle-2 required when logical-nozzle-count=2"
                )
            return {1: self.price_nozzle_1, 2: self.price_nozzle_2}
        raise PollBenchRefusedError(
            f"unsupported logical-nozzle-count={self.logical_nozzle_count}"
        )


def validate_price_write_params(params: PriceWriteParams) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing required confirmations: " + ", ".join(missing)
        )
    if params.address not in {1, 2}:
        raise PollBenchRefusedError("address must be 1 or 2")
    if params.logical_nozzle_count not in {1, 2}:
        raise PollBenchRefusedError("logical-nozzle-count must be 1 or 2")
    if params.logical_nozzle_count == 2 and params.price_nozzle_2 is None:
        raise PollBenchRefusedError("--price-nozzle-2 required for two nozzles")
    if params.logical_nozzle_count == 1 and params.price_nozzle_2 is not None:
        raise PollBenchRefusedError(
            "--price-nozzle-2 not allowed when logical-nozzle-count=1"
        )
    if not 0 <= params.sequence <= 0x0F:
        raise PollBenchRefusedError("sequence must be 0..15")


def validate_price_write_settings(settings: Settings) -> None:
    # Same LAB safety envelope as dry-run; write still forbids auth flags.
    validate_price_dry_run_settings(settings)


@dataclass(frozen=True, slots=True)
class ResetWriteConfirmations:
    owned_lab_pump: bool = False
    technician_present: bool = False
    no_product_connected: bool = False
    motor_isolated: bool = False
    valves_isolated: bool = False
    emergency_isolation_ready: bool = False
    authorization_disabled: bool = False
    single_write_plan_reviewed: bool = False
    execute_cd1_reset: bool = False
    post_write_status_verification_required: bool = False
    understand_transmits_to_owned_lab_pump: bool = False
    # Owned-lab NOZIO-unknown override (optional; required only when allow flag set).
    confirm_physical_nozzle_out: bool = False
    confirm_price_visible: bool = False
    allow_nozio_unknown_for_reset: bool = False
    confirm_reset_only: bool = False
    confirm_no_authorize: bool = False

    def missing_flags(self) -> list[str]:
        mapping = {
            "owned_lab_pump": "--confirm-owned-lab-pump",
            "technician_present": "--confirm-technician-present",
            "no_product_connected": "--confirm-no-product-connected",
            "motor_isolated": "--confirm-motor-isolated",
            "valves_isolated": "--confirm-valves-isolated",
            "emergency_isolation_ready": "--confirm-emergency-isolation-ready",
            "authorization_disabled": "--confirm-authorization-disabled",
            "single_write_plan_reviewed": "--confirm-single-write-plan-reviewed",
            "execute_cd1_reset": "--confirm-execute-cd1-reset",
            "post_write_status_verification_required": (
                "--confirm-post-write-status-verification-required"
            ),
            "understand_transmits_to_owned_lab_pump": (
                "--i-understand-this-transmits-to-owned-lab-pump"
            ),
        }
        return [flag for attr, flag in mapping.items() if not getattr(self, attr)]

    def to_dict(self) -> dict[str, bool]:
        return {
            "ownedLabPump": self.owned_lab_pump,
            "technicianPresent": self.technician_present,
            "noProductConnected": self.no_product_connected,
            "motorIsolated": self.motor_isolated,
            "valvesIsolated": self.valves_isolated,
            "emergencyIsolationReady": self.emergency_isolation_ready,
            "authorizationDisabled": self.authorization_disabled,
            "singleWritePlanReviewed": self.single_write_plan_reviewed,
            "executeCd1Reset": self.execute_cd1_reset,
            "postWriteStatusVerificationRequired": (
                self.post_write_status_verification_required
            ),
            "understandTransmitsToOwnedLabPump": (
                self.understand_transmits_to_owned_lab_pump
            ),
            "confirmPhysicalNozzleOut": self.confirm_physical_nozzle_out,
            "confirmPriceVisible": self.confirm_price_visible,
            "allowNozioUnknownForReset": self.allow_nozio_unknown_for_reset,
            "confirmResetOnly": self.confirm_reset_only,
            "confirmNoAuthorize": self.confirm_no_authorize,
        }


@dataclass(frozen=True, slots=True)
class ResetWriteParams:
    port: str
    address: int
    evidence_dir: Path
    confirmations: ResetWriteConfirmations
    baud: int = 9600
    response_timeout_ms: int = 500
    ack_timeout_ms: int = 500
    post_write_settle_ms: int = 1000
    post_write_max_status_polls: int = 16
    post_reset_observation_seconds: float = 3.0
    sequence: int = 0
    skip_service_check: bool = False
    skip_port_check: bool = False
    target_type: str = TARGET_OWNED_LAB_WAYNE


@dataclass(frozen=True, slots=True)
class AuthorizeWriteConfirmations:
    """AUTHORIZE is high-risk; isolation flags are mandatory."""

    owned_lab_pump: bool = False
    technician_present: bool = False
    no_product_connected: bool = False
    motor_isolated: bool = False
    valves_isolated: bool = False
    emergency_isolation_ready: bool = False
    single_write_plan_reviewed: bool = False
    execute_cd1_authorize: bool = False
    post_write_status_verification_required: bool = False
    understand_transmits_to_owned_lab_pump: bool = False
    understand_authorize_enables_live_delivery_ui: bool = False

    def missing_flags(self) -> list[str]:
        mapping = {
            "owned_lab_pump": "--confirm-owned-lab-pump",
            "technician_present": "--confirm-technician-present",
            "no_product_connected": "--confirm-no-product-connected",
            "motor_isolated": "--confirm-motor-isolated",
            "valves_isolated": "--confirm-valves-isolated",
            "emergency_isolation_ready": "--confirm-emergency-isolation-ready",
            "single_write_plan_reviewed": "--confirm-single-write-plan-reviewed",
            "execute_cd1_authorize": "--confirm-execute-cd1-authorize",
            "post_write_status_verification_required": (
                "--confirm-post-write-status-verification-required"
            ),
            "understand_transmits_to_owned_lab_pump": (
                "--i-understand-this-transmits-to-owned-lab-pump"
            ),
            "understand_authorize_enables_live_delivery_ui": (
                "--i-understand-authorize-enables-live-delivery-ui"
            ),
        }
        return [flag for attr, flag in mapping.items() if not getattr(self, attr)]

    def to_dict(self) -> dict[str, bool]:
        return {
            "ownedLabPump": self.owned_lab_pump,
            "technicianPresent": self.technician_present,
            "noProductConnected": self.no_product_connected,
            "motorIsolated": self.motor_isolated,
            "valvesIsolated": self.valves_isolated,
            "emergencyIsolationReady": self.emergency_isolation_ready,
            "singleWritePlanReviewed": self.single_write_plan_reviewed,
            "executeCd1Authorize": self.execute_cd1_authorize,
            "postWriteStatusVerificationRequired": (
                self.post_write_status_verification_required
            ),
            "understandTransmitsToOwnedLabPump": (
                self.understand_transmits_to_owned_lab_pump
            ),
            "understandAuthorizeEnablesLiveDeliveryUi": (
                self.understand_authorize_enables_live_delivery_ui
            ),
        }


@dataclass(frozen=True, slots=True)
class AuthorizeWriteParams:
    port: str
    address: int
    evidence_dir: Path
    confirmations: AuthorizeWriteConfirmations
    baud: int = 9600
    response_timeout_ms: int = 500
    ack_timeout_ms: int = 500
    post_write_settle_ms: int = 1000
    post_write_max_status_polls: int = 16
    sequence: int = 0
    skip_service_check: bool = False
    skip_port_check: bool = False

    @property
    def target_type(self) -> str:
        return TARGET_OWNED_LAB_WAYNE


def validate_reset_write_params(params: ResetWriteParams) -> None:
    # Local import avoids circular dependency with reset_decision.
    from intelipump_fdc.real_wayne_price.reset_decision import (
        override_confirmation_missing_flags,
    )

    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing required confirmations: " + ", ".join(missing)
        )
    if params.address not in {1, 2}:
        raise PollBenchRefusedError("address must be 1 or 2")
    if not 0 <= params.sequence <= 0x0F:
        raise PollBenchRefusedError("sequence must be 0..15")
    if params.post_reset_observation_seconds < 0:
        raise PollBenchRefusedError("post-reset-observation-seconds must be >= 0")
    if params.confirmations.allow_nozio_unknown_for_reset:
        if params.target_type != TARGET_OWNED_LAB_WAYNE:
            raise PollBenchRefusedError(
                "nozio-unknown RESET override requires targetType=OWNED_LAB_WAYNE"
            )
        if not params.confirmations.authorization_disabled:
            raise PollBenchRefusedError(
                "nozio-unknown RESET override refuses when authorization is enabled"
            )
        override_missing = override_confirmation_missing_flags(params.confirmations)
        if override_missing:
            raise PollBenchRefusedError(
                "missing required nozio-unknown override confirmations: "
                + ", ".join(override_missing)
            )


def validate_authorize_write_params(params: AuthorizeWriteParams) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing required confirmations: " + ", ".join(missing)
        )
    if params.address not in {1, 2}:
        raise PollBenchRefusedError("address must be 1 or 2")
    if not 0 <= params.sequence <= 0x0F:
        raise PollBenchRefusedError("sequence must be 0..15")


def validate_reset_write_settings(settings: Settings) -> None:
    validate_price_dry_run_settings(settings)


def validate_authorize_write_settings(settings: Settings) -> None:
    # Same LAB envelope; automatic/remote authorization flags must stay off.
    validate_price_dry_run_settings(settings)


@dataclass(frozen=True, slots=True)
class Cd2ResetWriteConfirmations:
    owned_lab_pump: bool = False
    technician_present: bool = False
    no_product_connected: bool = False
    motor_isolated: bool = False
    valves_isolated: bool = False
    emergency_isolation_ready: bool = False
    authorization_disabled: bool = False
    single_write_plan_reviewed: bool = False
    logical_nozzle_mapping_confirmed: bool = False
    nozzle_out_observed: bool = False
    execute_cd2_and_cd1_reset: bool = False
    post_write_status_verification_required: bool = False
    understand_transmits_to_owned_lab_pump: bool = False

    def missing_flags(self) -> list[str]:
        mapping = {
            "owned_lab_pump": "--confirm-owned-lab-pump",
            "technician_present": "--confirm-technician-present",
            "no_product_connected": "--confirm-no-product-connected",
            "motor_isolated": "--confirm-motor-isolated",
            "valves_isolated": "--confirm-valves-isolated",
            "emergency_isolation_ready": "--confirm-emergency-isolation-ready",
            "authorization_disabled": "--confirm-authorization-disabled",
            "single_write_plan_reviewed": "--confirm-single-write-plan-reviewed",
            "logical_nozzle_mapping_confirmed": (
                "--logical-nozzle-mapping-confirmed-by-technician"
            ),
            "nozzle_out_observed": "--confirm-nozzle-out-observed",
            "execute_cd2_and_cd1_reset": "--confirm-execute-cd2-and-cd1-reset",
            "post_write_status_verification_required": (
                "--confirm-post-write-status-verification-required"
            ),
            "understand_transmits_to_owned_lab_pump": (
                "--i-understand-this-transmits-to-owned-lab-pump"
            ),
        }
        return [flag for attr, flag in mapping.items() if not getattr(self, attr)]

    def to_dict(self) -> dict[str, bool]:
        return {
            "ownedLabPump": self.owned_lab_pump,
            "technicianPresent": self.technician_present,
            "noProductConnected": self.no_product_connected,
            "motorIsolated": self.motor_isolated,
            "valvesIsolated": self.valves_isolated,
            "emergencyIsolationReady": self.emergency_isolation_ready,
            "authorizationDisabled": self.authorization_disabled,
            "singleWritePlanReviewed": self.single_write_plan_reviewed,
            "logicalNozzleMappingConfirmed": self.logical_nozzle_mapping_confirmed,
            "nozzleOutObserved": self.nozzle_out_observed,
            "executeCd2AndCd1Reset": self.execute_cd2_and_cd1_reset,
            "postWriteStatusVerificationRequired": (
                self.post_write_status_verification_required
            ),
            "understandTransmitsToOwnedLabPump": (
                self.understand_transmits_to_owned_lab_pump
            ),
        }


@dataclass(frozen=True, slots=True)
class Cd2ResetWriteParams:
    port: str
    address: int
    evidence_dir: Path
    confirmations: Cd2ResetWriteConfirmations
    allowed_nozzles: tuple[int, ...]
    baud: int = 9600
    response_timeout_ms: int = 500
    ack_timeout_ms: int = 500
    post_write_settle_ms: int = 1000
    post_write_max_status_polls: int = 16
    sequence: int = 0
    skip_service_check: bool = False
    skip_port_check: bool = False

    @property
    def target_type(self) -> str:
        return TARGET_OWNED_LAB_WAYNE


def validate_cd2_reset_write_params(params: Cd2ResetWriteParams) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing required confirmations: " + ", ".join(missing)
        )
    if params.address not in {1, 2}:
        raise PollBenchRefusedError("address must be 1 or 2")
    if not 0 <= params.sequence <= 0x0F:
        raise PollBenchRefusedError("sequence must be 0..15")
    if not params.allowed_nozzles:
        raise PollBenchRefusedError("at least one --allowed-nozzle required")
    if len(params.allowed_nozzles) != len(set(params.allowed_nozzles)):
        raise PollBenchRefusedError("duplicate --allowed-nozzle values")
    for n in params.allowed_nozzles:
        if n not in {1, 2}:
            raise PollBenchRefusedError(
                f"allowed nozzle must be 1 or 2 for lab path, got {n}"
            )


def validate_cd2_reset_write_settings(settings: Settings) -> None:
    validate_price_dry_run_settings(settings)


@dataclass(frozen=True, slots=True)
class ReturnStatusResetWriteConfirmations:
    """RETURN_STATUS then RESET — capture-matched path; no AUTHORIZE."""

    owned_lab_pump: bool = False
    technician_present: bool = False
    no_product_connected: bool = False
    motor_isolated: bool = False
    valves_isolated: bool = False
    emergency_isolation_ready: bool = False
    authorization_disabled: bool = False
    single_write_plan_reviewed: bool = False
    nozzle_out_observed: bool = False
    execute_cd1_return_status_and_reset: bool = False
    post_write_status_verification_required: bool = False
    understand_transmits_to_owned_lab_pump: bool = False

    def missing_flags(self) -> list[str]:
        mapping = {
            "owned_lab_pump": "--confirm-owned-lab-pump",
            "technician_present": "--confirm-technician-present",
            "no_product_connected": "--confirm-no-product-connected",
            "motor_isolated": "--confirm-motor-isolated",
            "valves_isolated": "--confirm-valves-isolated",
            "emergency_isolation_ready": "--confirm-emergency-isolation-ready",
            "authorization_disabled": "--confirm-authorization-disabled",
            "single_write_plan_reviewed": "--confirm-single-write-plan-reviewed",
            "nozzle_out_observed": "--confirm-nozzle-out-observed",
            "execute_cd1_return_status_and_reset": (
                "--confirm-execute-cd1-return-status-and-reset"
            ),
            "post_write_status_verification_required": (
                "--confirm-post-write-status-verification-required"
            ),
            "understand_transmits_to_owned_lab_pump": (
                "--i-understand-this-transmits-to-owned-lab-pump"
            ),
        }
        return [flag for attr, flag in mapping.items() if not getattr(self, attr)]

    def to_dict(self) -> dict[str, bool]:
        return {
            "ownedLabPump": self.owned_lab_pump,
            "technicianPresent": self.technician_present,
            "noProductConnected": self.no_product_connected,
            "motorIsolated": self.motor_isolated,
            "valvesIsolated": self.valves_isolated,
            "emergencyIsolationReady": self.emergency_isolation_ready,
            "authorizationDisabled": self.authorization_disabled,
            "singleWritePlanReviewed": self.single_write_plan_reviewed,
            "nozzleOutObserved": self.nozzle_out_observed,
            "executeCd1ReturnStatusAndReset": (
                self.execute_cd1_return_status_and_reset
            ),
            "postWriteStatusVerificationRequired": (
                self.post_write_status_verification_required
            ),
            "understandTransmitsToOwnedLabPump": (
                self.understand_transmits_to_owned_lab_pump
            ),
        }


@dataclass(frozen=True, slots=True)
class ReturnStatusResetWriteParams:
    port: str
    address: int
    evidence_dir: Path
    confirmations: ReturnStatusResetWriteConfirmations
    baud: int = 9600
    response_timeout_ms: int = 500
    ack_timeout_ms: int = 500
    post_write_settle_ms: int = 1000
    post_write_max_status_polls: int = 16
    sequence: int = 0
    skip_service_check: bool = False
    skip_port_check: bool = False

    @property
    def target_type(self) -> str:
        return TARGET_OWNED_LAB_WAYNE


def validate_return_status_reset_write_params(
    params: ReturnStatusResetWriteParams,
) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing required confirmations: " + ", ".join(missing)
        )
    if params.address not in {1, 2}:
        raise PollBenchRefusedError("address must be 1 or 2")
    if not 0 <= params.sequence <= 0x0F:
        raise PollBenchRefusedError("sequence must be 0..15")


def validate_return_status_reset_write_settings(settings: Settings) -> None:
    validate_price_dry_run_settings(settings)


@dataclass(frozen=True, slots=True)
class Cd101WriteConfirmations:
    owned_lab_pump: bool = False
    technician_present: bool = False
    no_product_connected: bool = False
    motor_isolated: bool = False
    valves_isolated: bool = False
    emergency_isolation_ready: bool = False
    authorization_disabled: bool = False
    single_write_plan_reviewed: bool = False
    execute_cd101_request: bool = False
    understand_transmits_to_owned_lab_pump: bool = False

    def missing_flags(self) -> list[str]:
        mapping = {
            "owned_lab_pump": "--confirm-owned-lab-pump",
            "technician_present": "--confirm-technician-present",
            "no_product_connected": "--confirm-no-product-connected",
            "motor_isolated": "--confirm-motor-isolated",
            "valves_isolated": "--confirm-valves-isolated",
            "emergency_isolation_ready": "--confirm-emergency-isolation-ready",
            "authorization_disabled": "--confirm-authorization-disabled",
            "single_write_plan_reviewed": "--confirm-single-write-plan-reviewed",
            "execute_cd101_request": "--confirm-execute-cd101-request",
            "understand_transmits_to_owned_lab_pump": (
                "--i-understand-this-transmits-to-owned-lab-pump"
            ),
        }
        return [flag for attr, flag in mapping.items() if not getattr(self, attr)]

    def to_dict(self) -> dict[str, bool]:
        return {
            "ownedLabPump": self.owned_lab_pump,
            "technicianPresent": self.technician_present,
            "noProductConnected": self.no_product_connected,
            "motorIsolated": self.motor_isolated,
            "valvesIsolated": self.valves_isolated,
            "emergencyIsolationReady": self.emergency_isolation_ready,
            "authorizationDisabled": self.authorization_disabled,
            "singleWritePlanReviewed": self.single_write_plan_reviewed,
            "executeCd101Request": self.execute_cd101_request,
            "understandTransmitsToOwnedLabPump": (
                self.understand_transmits_to_owned_lab_pump
            ),
        }


@dataclass(frozen=True, slots=True)
class Cd101WriteParams:
    port: str
    address: int
    evidence_dir: Path
    confirmations: Cd101WriteConfirmations
    baud: int = 9600
    response_timeout_ms: int = 500
    ack_timeout_ms: int = 1000
    post_write_settle_ms: int = 200
    post_write_max_status_polls: int = 8
    sequence: int = 0
    counter_select: int = 1
    skip_service_check: bool = False
    skip_port_check: bool = False

    @property
    def target_type(self) -> str:
        return TARGET_OWNED_LAB_WAYNE


def validate_cd101_write_params(params: Cd101WriteParams) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing required confirmations: " + ", ".join(missing)
        )
    if params.address not in {1, 2}:
        raise PollBenchRefusedError("address must be 1 or 2")
    if not 0 <= params.sequence <= 0x0F:
        raise PollBenchRefusedError("sequence must be 0..15")
    if not 0 <= params.counter_select <= 0xFF:
        raise PollBenchRefusedError("counter-select must be 0..255")


def validate_cd101_write_settings(settings: Settings) -> None:
    validate_price_dry_run_settings(settings)


__all__ = [
    "AuthorizeWriteConfirmations",
    "AuthorizeWriteParams",
    "Cd2ResetWriteConfirmations",
    "Cd2ResetWriteParams",
    "Cd101WriteConfirmations",
    "Cd101WriteParams",
    "PollBenchRefusedError",
    "PriceDryRunConfirmations",
    "PriceDryRunParams",
    "PriceWriteConfirmations",
    "PriceWriteParams",
    "ResetWriteConfirmations",
    "ResetWriteParams",
    "ReturnStatusResetWriteConfirmations",
    "ReturnStatusResetWriteParams",
    "software_commit",
    "validate_authorize_write_params",
    "validate_authorize_write_settings",
    "validate_cd2_reset_write_params",
    "validate_cd2_reset_write_settings",
    "validate_cd101_write_params",
    "validate_cd101_write_settings",
    "validate_price_dry_run_params",
    "validate_price_dry_run_settings",
    "validate_price_write_params",
    "validate_price_write_settings",
    "validate_reset_write_params",
    "validate_reset_write_settings",
    "validate_return_status_reset_write_params",
    "validate_return_status_reset_write_settings",
]
