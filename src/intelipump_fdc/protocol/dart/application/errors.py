"""Typed application-layer decode / split errors."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class AppErrorCode(StrEnum):
    TRUNCATED_HEADER = "TRUNCATED_HEADER"
    TRUNCATED_DATA = "TRUNCATED_DATA"
    INVALID_BCD = "INVALID_BCD"
    LENGTH_MISMATCH = "LENGTH_MISMATCH"
    MALFORMED_BODY = "MALFORMED_BODY"


@dataclass(frozen=True, slots=True)
class AppDecodeError:
    code: AppErrorCode
    message: str
    transaction_id: int | None = None
    expected_length: int | None = None
    actual_length: int | None = None
    raw: bytes = b""
