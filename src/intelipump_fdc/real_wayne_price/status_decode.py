"""Decode and validate status-poll responses for CD5 dry-run preconditions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from intelipump_fdc.protocol.dart.application.constants import TransactionType
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.application.status import (
    WaynePumpStatus,
    describe_wayne_status,
)
from intelipump_fdc.protocol.dart.line.models import DartLineFrame


class StatusPreconditionError(RuntimeError):
    """Status preconditions for price dry-run failed."""

    def __init__(self, message: str, *, reasons: list[str] | None = None) -> None:
        super().__init__(message)
        self.reasons = reasons or [message]


@dataclass
class DecodedStatusSnapshot:
    wire_address: int
    crc_valid: bool
    transactions: list[dict[str, Any]] = field(default_factory=list)
    dc1_code: int | None = None
    dc1_name: str | None = None
    volume_raw_scaled: int | None = None
    amount_raw_scaled: int | None = None
    volume_raw_bcd_hex: str | None = None
    amount_raw_bcd_hex: str | None = None
    filling_price_raw_scaled: int | None = None
    filling_price_raw_bcd_hex: str | None = None
    selected_logical_nozzle: int | None = None
    nozzle_out: bool | None = None
    has_dc5_alarm: bool = False
    alarm_code: int | None = None
    raw_frame_hex: str = ""

    def to_report_dict(self) -> dict[str, Any]:
        return {
            "wireAddress": f"0x{self.wire_address:02X}",
            "crcValid": self.crc_valid,
            "transactions": list(self.transactions),
            "dc1": {"code": self.dc1_code, "name": self.dc1_name},
            "dc2": {
                "filledVolumeRawScaled": self.volume_raw_scaled,
                "filledAmountRawScaled": self.amount_raw_scaled,
                "filledVolumeRawBcd": self.volume_raw_bcd_hex,
                "filledAmountRawBcd": self.amount_raw_bcd_hex,
            },
            "dc3": {
                "fillingPriceRawScaled": self.filling_price_raw_scaled,
                "fillingPriceRawBcd": self.filling_price_raw_bcd_hex,
                "selectedLogicalNozzle": self.selected_logical_nozzle,
                "nozzlePosition": (
                    "OUT"
                    if self.nozzle_out is True
                    else ("IN" if self.nozzle_out is False else None)
                ),
                "note": (
                    "selectedLogicalNozzle is evidence only; "
                    "does not prove configured nozzle count"
                ),
            },
            "dc5Alarm": {
                "present": self.has_dc5_alarm,
                "alarmCode": self.alarm_code,
            },
            "rawFrameHex": self.raw_frame_hex,
        }


def decode_status_frame(
    frame: DartLineFrame,
    *,
    expected_wire_address: int,
) -> DecodedStatusSnapshot:
    snap = DecodedStatusSnapshot(
        wire_address=frame.address,
        crc_valid=bool(frame.crc_valid),
        raw_frame_hex=frame.raw_frame.hex(" "),
    )
    bundle = decode_data_payload(
        frame.payload,
        line_sequence=frame.sequence,
        source_frame_raw_hex=frame.raw_frame.hex(" "),
    )
    for tx in bundle.transactions:
        entry: dict[str, Any] = {
            "type": tx.transaction_type.value,
            "transactionId": tx.transaction_id,
            "length": tx.length,
            "rawHex": tx.raw_transaction.hex(" "),
            "decodeStatus": tx.decode_status.value,
        }
        body = tx.decoded_body or {}
        if tx.transaction_type is TransactionType.AMBIGUOUS_CD1_OR_DC1:
            raw_code = body.get("raw_code")
            info = describe_wayne_status(int(raw_code)) if raw_code is not None else None
            snap.dc1_code = int(raw_code) if raw_code is not None else None
            snap.dc1_name = info.name if info is not None else None
            entry.update(
                {
                    "type": "DC1",
                    "statusCode": snap.dc1_code,
                    "status": snap.dc1_name,
                }
            )
        elif tx.transaction_type is TransactionType.DC2_FILLED_VOLUME_AMOUNT:
            vol = body.get("volume") or {}
            amt = body.get("amount") or {}
            snap.volume_raw_scaled = vol.get("raw_scaled")
            snap.amount_raw_scaled = amt.get("raw_scaled")
            snap.volume_raw_bcd_hex = vol.get("raw_bcd_hex")
            snap.amount_raw_bcd_hex = amt.get("raw_bcd_hex")
            entry.update(
                {
                    "type": "DC2",
                    "filledVolumeRaw": _pad_bcd_digits(vol.get("raw_scaled"), 8),
                    "filledAmountRaw": _pad_bcd_digits(amt.get("raw_scaled"), 8),
                }
            )
        elif tx.transaction_type is TransactionType.DC3_NOZZLE_STATUS_PRICE:
            price = body.get("price") or {}
            snap.filling_price_raw_scaled = price.get("raw_scaled")
            snap.filling_price_raw_bcd_hex = price.get("raw_bcd_hex")
            snap.selected_logical_nozzle = body.get("selected_logical_nozzle")
            snap.nozzle_out = body.get("nozzle_out")
            entry.update(
                {
                    "type": "DC3",
                    "fillingPriceRaw": _pad_bcd_digits(price.get("raw_scaled"), 6),
                    "selectedLogicalNozzle": snap.selected_logical_nozzle,
                    "nozzlePosition": "OUT" if snap.nozzle_out else "IN",
                }
            )
        elif tx.transaction_type is TransactionType.DC5_ALARM:
            snap.has_dc5_alarm = True
            snap.alarm_code = body.get("alarm_code")
            entry.update({"type": "DC5", "alarmCode": snap.alarm_code})
        snap.transactions.append(entry)

    _ = expected_wire_address  # validated in validate_preconditions
    return snap


def validate_preconditions(
    snap: DecodedStatusSnapshot,
    *,
    expected_wire_address: int,
    authorization_disabled: bool,
) -> None:
    reasons: list[str] = []
    if not snap.crc_valid:
        reasons.append("crc_invalid")
    if snap.wire_address != expected_wire_address:
        reasons.append(
            f"address_mismatch got=0x{snap.wire_address:02X} "
            f"expected=0x{expected_wire_address:02X}"
        )
    if snap.dc1_code is None:
        reasons.append("dc1_missing")
    elif snap.dc1_code != int(WaynePumpStatus.PUMP_NOT_PROGRAMMED):
        reasons.append(
            f"dc1_not_PUMP_NOT_PROGRAMMED got={snap.dc1_name}/{snap.dc1_code}"
        )
    if snap.volume_raw_scaled is None or snap.amount_raw_scaled is None:
        reasons.append("dc2_missing_or_incomplete")
    else:
        if snap.volume_raw_scaled != 0:
            reasons.append("nonzero_filled_volume")
        if snap.amount_raw_scaled != 0:
            reasons.append("nonzero_filled_amount")
    if snap.dc1_code == int(WaynePumpStatus.FILLING):
        reasons.append("active_filling")
    if snap.has_dc5_alarm:
        reasons.append(f"blocking_alarm code={snap.alarm_code}")
    if snap.selected_logical_nozzle is None and snap.nozzle_out is None:
        reasons.append("nozzle_state_not_recorded")
    if not authorization_disabled:
        reasons.append("authorization_not_disabled")
    if reasons:
        raise StatusPreconditionError(
            "price dry-run status preconditions failed: " + "; ".join(reasons),
            reasons=reasons,
        )


def _pad_bcd_digits(raw_scaled: object, width: int) -> str | None:
    if raw_scaled is None:
        return None
    if not isinstance(raw_scaled, int):
        raise TypeError(f"raw_scaled must be int|None, got {type(raw_scaled)!r}")
    return f"{raw_scaled:0{width}d}"
