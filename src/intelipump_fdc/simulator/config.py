"""Simulator configuration (virtual dispenser only)."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum


class SequencePolicy(StrEnum):
    """TX# wrap policy after 0xF.

    SPEC_F_TO_1: documented line-level rule (Phase 1 helper).
    OBSERVED_F_TO_0: seen on merged-bus captures; not proven for
    direction-separated traffic.
    """

    SPEC_F_TO_1 = "SPEC_F_TO_1"
    OBSERVED_F_TO_0 = "OBSERVED_F_TO_0"


@dataclass(frozen=True, slots=True)
class FillingConfig:
    """Deterministic filling engine parameters (scaled integers / Decimal)."""

    # Liters per simulated second (Decimal; no binary float in math path).
    flow_rate_liters_per_second: Decimal = Decimal("0.5")
    update_interval_ms: int = 200
    volume_decimals: int = 3
    amount_decimals: int = 2
    price_decimals: int = 2
    # Optional hard stop when scaled volume/amount reaches preset.
    preset_volume_raw: int | None = None
    preset_amount_raw: int | None = None
    # Natural completion volume when no preset (scaled by volume_decimals).
    natural_complete_volume_raw: int = 40_000  # 40.000 L at 3 decimals


@dataclass(frozen=True, slots=True)
class PumpConfig:
    pump_id: str
    dart_address: int
    nozzle_count: int = 2
    default_price_raw: int = 1175  # e.g. 11.75 at 2 decimals
    filling: FillingConfig = field(default_factory=FillingConfig)


@dataclass(frozen=True, slots=True)
class SimulatorConfig:
    """One virtual dispenser with two fueling positions by default."""

    sequence_policy: SequencePolicy = SequencePolicy.SPEC_F_TO_1
    response_timeout_ms: int = 25
    # Simulator-only: allow Phase-4 active-command eligibility gates.
    simulator_physical_enable: bool = True
    simulator_active_commands_enabled: bool = True
    pumps: tuple[PumpConfig, ...] = (
        PumpConfig(pump_id="fp-1", dart_address=1),
        PumpConfig(pump_id="fp-2", dart_address=2),
    )


def next_sequence(sequence: int, policy: SequencePolicy) -> int:
    """Return next TX# after a successful DATA block under ``policy``."""
    if not 0 <= sequence <= 0x0F:
        raise ValueError(f"sequence out of range 0x0-0xF: {sequence}")
    if sequence != 0x0F:
        return sequence + 1
    if policy is SequencePolicy.SPEC_F_TO_1:
        return 0x01
    return 0x00
