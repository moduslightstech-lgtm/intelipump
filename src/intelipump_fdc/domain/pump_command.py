"""Normalized pump command types (eligibility evaluation only)."""

from __future__ import annotations

from enum import StrEnum


class PumpCommand(StrEnum):
    READ_STATUS = "READ_STATUS"
    READ_TOTALS = "READ_TOTALS"
    SET_PRICE = "SET_PRICE"
    RESET = "RESET"
    AUTHORIZE = "AUTHORIZE"
    STOP = "STOP"
    SUSPEND = "SUSPEND"
    RESUME = "RESUME"
    PRESET_AMOUNT = "PRESET_AMOUNT"
    PRESET_VOLUME = "PRESET_VOLUME"


# Non-idempotent commands must never be blindly retried after restart.
NON_IDEMPOTENT_COMMANDS: frozenset[PumpCommand] = frozenset(
    {
        PumpCommand.AUTHORIZE,
        PumpCommand.STOP,
        PumpCommand.SUSPEND,
        PumpCommand.RESUME,
        PumpCommand.RESET,
        PumpCommand.SET_PRICE,
        PumpCommand.PRESET_AMOUNT,
        PumpCommand.PRESET_VOLUME,
    }
)
