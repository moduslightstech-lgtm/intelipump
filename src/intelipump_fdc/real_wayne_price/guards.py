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


__all__ = [
    "PollBenchRefusedError",
    "PriceDryRunConfirmations",
    "PriceDryRunParams",
    "software_commit",
    "validate_price_dry_run_params",
    "validate_price_dry_run_settings",
]
