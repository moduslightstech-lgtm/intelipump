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
    response_timeout_ms: int = 250
    ack_timeout_ms: int = 250
    post_write_settle_ms: int = 400
    post_write_max_status_polls: int = 8
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
        }


@dataclass(frozen=True, slots=True)
class ResetWriteParams:
    port: str
    address: int
    evidence_dir: Path
    confirmations: ResetWriteConfirmations
    baud: int = 9600
    response_timeout_ms: int = 250
    ack_timeout_ms: int = 250
    post_write_settle_ms: int = 400
    post_write_max_status_polls: int = 8
    sequence: int = 0
    skip_service_check: bool = False
    skip_port_check: bool = False

    @property
    def target_type(self) -> str:
        return TARGET_OWNED_LAB_WAYNE


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
    response_timeout_ms: int = 250
    ack_timeout_ms: int = 250
    post_write_settle_ms: int = 400
    post_write_max_status_polls: int = 8
    sequence: int = 0
    skip_service_check: bool = False
    skip_port_check: bool = False

    @property
    def target_type(self) -> str:
        return TARGET_OWNED_LAB_WAYNE


def validate_reset_write_params(params: ResetWriteParams) -> None:
    missing = params.confirmations.missing_flags()
    if missing:
        raise PollBenchRefusedError(
            "missing required confirmations: " + ", ".join(missing)
        )
    if params.address not in {1, 2}:
        raise PollBenchRefusedError("address must be 1 or 2")
    if not 0 <= params.sequence <= 0x0F:
        raise PollBenchRefusedError("sequence must be 0..15")


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
    response_timeout_ms: int = 250
    ack_timeout_ms: int = 250
    post_write_settle_ms: int = 400
    post_write_max_status_polls: int = 8
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


__all__ = [
    "AuthorizeWriteConfirmations",
    "AuthorizeWriteParams",
    "Cd2ResetWriteConfirmations",
    "Cd2ResetWriteParams",
    "PollBenchRefusedError",
    "PriceDryRunConfirmations",
    "PriceDryRunParams",
    "PriceWriteConfirmations",
    "PriceWriteParams",
    "ResetWriteConfirmations",
    "ResetWriteParams",
    "software_commit",
    "validate_authorize_write_params",
    "validate_authorize_write_settings",
    "validate_cd2_reset_write_params",
    "validate_cd2_reset_write_settings",
    "validate_price_dry_run_params",
    "validate_price_dry_run_settings",
    "validate_price_write_params",
    "validate_price_write_settings",
    "validate_reset_write_params",
    "validate_reset_write_settings",
]
