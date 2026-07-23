"""Application-layer models for DART transactions."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from intelipump_fdc.protocol.dart.application.constants import (
    DecodeStatus,
    MessageDirection,
    TransactionType,
)


@dataclass(frozen=True, slots=True)
class ScaledBcdValue:
    """Packed-BCD numeric field with optional decimal placement.

    When ``decimals`` is None, ``value`` is None and only the raw scaled
    integer (digits as an integer) is available. Decimal placement must come
    from documented pump parameters (DC7 DPVOL/DPAMO/DPUNP) — never assumed.
    """

    raw_bcd: bytes
    raw_scaled: int
    decimals: int | None = None
    value: Decimal | None = None

    def as_report_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "raw_bcd_hex": self.raw_bcd.hex(" ").upper(),
            "raw_scaled": self.raw_scaled,
            "decimals": self.decimals,
        }
        if self.value is not None:
            out["value"] = format(self.value, "f")
        else:
            out["value"] = None
        return out


@dataclass(frozen=True, slots=True)
class RawTransaction:
    """One TRANS+LNG+DATA unit carved from a DATA payload."""

    transaction_id: int
    length: int
    data: bytes
    raw: bytes  # TRANS + LNG + DATA
    offset: int  # byte offset within the DATA payload


@dataclass(frozen=True, slots=True)
class SplitResult:
    transactions: tuple[RawTransaction, ...]
    trailing_bytes: bytes
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ApplicationTransaction:
    """Decoded (or partially decoded) application transaction envelope."""

    transaction_id: int
    transaction_type: TransactionType
    length: int
    raw_payload: bytes  # DATA bytes only (excluding TRANS/LNG)
    raw_transaction: bytes  # TRANS + LNG + DATA
    decoded_body: dict[str, Any] | None
    pump_address: int | None
    line_sequence: int | None
    direction: MessageDirection
    decode_status: DecodeStatus
    warnings: tuple[str, ...] = ()
    source_frame_raw_hex: str | None = None
    offset: int = 0
    reserved_or_unknown_fields: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DecodeBundle:
    """Result of decoding all transactions in one DATA payload."""

    transactions: tuple[ApplicationTransaction, ...]
    trailing_bytes: bytes
    warnings: tuple[str, ...] = ()
