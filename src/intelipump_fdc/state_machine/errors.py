"""Typed state-machine result severities (no exception-driven control flow)."""

from __future__ import annotations

from enum import StrEnum


class TransitionSeverity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
