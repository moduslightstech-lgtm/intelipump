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

    def __init__(
        self,
        message: str,
        *,
        reasons: list[str] | None = None,
        last_snap: DecodedStatusSnapshot | None = None,
        last_frame: DartLineFrame | None = None,
        poll_count: int = 0,
    ) -> None:
        super().__init__(message)
        self.reasons = reasons or [message]
        self.last_snap = last_snap
        self.last_frame = last_frame
        self.poll_count = poll_count


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
    nozio_evidence: dict[str, Any] | None = None
    has_dc5_alarm: bool = False
    alarm_code: int | None = None
    raw_frame_hex: str = ""

    def to_report_dict(self) -> dict[str, Any]:
        dc3: dict[str, Any] = {
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
        }
        if self.nozio_evidence is not None:
            dc3.update(self.nozio_evidence)
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
            "dc3": dc3,
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
        elif tx.transaction_type in {
            TransactionType.DC3_NOZZLE_STATUS_PRICE,
            TransactionType.AMBIGUOUS_CD3_OR_DC3,
        }:
            # Lab status frames are pump→controller; treat ambiguous CD3/DC3 as DC3
            # evidence for the snapshot (wire collision noted on decode_status).
            price = body.get("price") or {}
            if isinstance(price, dict) and "error" in price:
                price = {}
            snap.filling_price_raw_scaled = price.get("raw_scaled")
            snap.filling_price_raw_bcd_hex = price.get("raw_bcd_hex")
            snap.selected_logical_nozzle = body.get("selected_logical_nozzle")
            snap.nozzle_out = body.get("nozzle_out")
            nozio_ev = body.get("nozio")
            if isinstance(nozio_ev, dict):
                snap.nozio_evidence = dict(nozio_ev)
            entry.update(
                {
                    "type": "DC3",
                    "fillingPriceRaw": _pad_bcd_digits(price.get("raw_scaled"), 6),
                    "selectedLogicalNozzle": snap.selected_logical_nozzle,
                    "nozzlePosition": "OUT" if snap.nozzle_out else "IN",
                    "wireAmbiguousCd3Dc3": (
                        tx.transaction_type is TransactionType.AMBIGUOUS_CD3_OR_DC3
                    ),
                }
            )
            if snap.nozio_evidence is not None:
                entry.update(snap.nozio_evidence)
        elif tx.transaction_type is TransactionType.DC5_ALARM:
            snap.has_dc5_alarm = True
            snap.alarm_code = body.get("alarm_code")
            entry.update({"type": "DC5", "alarmCode": snap.alarm_code})
        snap.transactions.append(entry)

    _ = expected_wire_address  # validated in validate_preconditions
    return snap


_CD5_ELIGIBLE_DC1 = frozenset(
    {
        int(WaynePumpStatus.PUMP_NOT_PROGRAMMED),
        int(WaynePumpStatus.FILLING_COMPLETED),
    }
)


def validate_preconditions(
    snap: DecodedStatusSnapshot,
    *,
    expected_wire_address: int,
    authorization_disabled: bool,
) -> None:
    """CD5 eligibility: idle not-programmed, or re-price from FILLING_COMPLETE.

    Pump Interface: initial price from PUMP NOT PROGRAMMED; example 4.3 also
    sends CD5 while FILLING COMPLETE. Refuse active FILLING / AUTHORIZED / etc.
    """
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
    elif snap.dc1_code not in _CD5_ELIGIBLE_DC1:
        reasons.append(
            "dc1_not_CD5_eligible "
            f"got={snap.dc1_name}/{snap.dc1_code} "
            "allowed=PUMP_NOT_PROGRAMMED|FILLING_COMPLETED"
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


def validate_post_write_status(
    snap: DecodedStatusSnapshot,
    *,
    expected_wire_address: int,
) -> None:
    """Require DC1 FILLING_COMPLETE after an accepted CD5 price update."""
    _validate_dc1_status(
        snap,
        expected_wire_address=expected_wire_address,
        expected=WaynePumpStatus.FILLING_COMPLETED,
        label="post-write status verification",
        expected_name="FILLING_COMPLETE",
    )


def validate_reset_preconditions(
    snap: DecodedStatusSnapshot,
    *,
    expected_wire_address: int,
) -> None:
    """Require FILLING_COMPLETE before CD1 RESET (clear CLOSED display)."""
    _validate_dc1_status(
        snap,
        expected_wire_address=expected_wire_address,
        expected=WaynePumpStatus.FILLING_COMPLETED,
        label="reset status preconditions",
        expected_name="FILLING_COMPLETE",
    )


def validate_cd2_reset_preconditions(
    snap: DecodedStatusSnapshot,
    *,
    expected_wire_address: int,
) -> None:
    """Require FILLING_COMPLETE and nozzle OUT before CD2+RESET."""
    validate_reset_preconditions(
        snap, expected_wire_address=expected_wire_address
    )
    reasons: list[str] = []
    if snap.nozzle_out is not True:
        if snap.nozzle_out is False:
            pos = "IN"
        elif snap.nozzle_out is True:
            pos = "OUT"
        else:
            pos = "unknown"
        reasons.append(f"nozzle_not_OUT got={pos}")
    if reasons:
        raise StatusPreconditionError(
            "cd2+reset status preconditions failed: " + "; ".join(reasons),
            reasons=reasons,
        )


def validate_post_reset_status(
    snap: DecodedStatusSnapshot,
    *,
    expected_wire_address: int,
) -> None:
    """Require DC1 RESET after accepted CD1 RESET."""
    _validate_dc1_status(
        snap,
        expected_wire_address=expected_wire_address,
        expected=WaynePumpStatus.RESET,
        label="post-reset status verification",
        expected_name="RESET",
    )


def validate_authorize_preconditions(
    snap: DecodedStatusSnapshot,
    *,
    expected_wire_address: int,
) -> None:
    """Require RESET before CD1 AUTHORIZE (live volume/amount path)."""
    _validate_dc1_status(
        snap,
        expected_wire_address=expected_wire_address,
        expected=WaynePumpStatus.RESET,
        label="authorize status preconditions",
        expected_name="RESET",
    )


def validate_post_authorize_status(
    snap: DecodedStatusSnapshot,
    *,
    expected_wire_address: int,
) -> None:
    """Require DC1 AUTHORIZED after accepted CD1 AUTHORIZE."""
    _validate_dc1_status(
        snap,
        expected_wire_address=expected_wire_address,
        expected=WaynePumpStatus.AUTHORIZED,
        label="post-authorize status verification",
        expected_name="AUTHORIZED",
    )


def _validate_dc1_status(
    snap: DecodedStatusSnapshot,
    *,
    expected_wire_address: int,
    expected: WaynePumpStatus,
    label: str,
    expected_name: str,
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
    elif snap.dc1_code != int(expected):
        reasons.append(
            f"dc1_not_{expected_name} got={snap.dc1_name}/{snap.dc1_code}"
        )
    if snap.has_dc5_alarm:
        reasons.append(f"blocking_alarm code={snap.alarm_code}")
    if reasons:
        raise StatusPreconditionError(
            f"{label} failed: " + "; ".join(reasons),
            reasons=reasons,
        )


def _pad_bcd_digits(raw_scaled: object, width: int) -> str | None:
    if raw_scaled is None:
        return None
    if not isinstance(raw_scaled, int):
        raise TypeError(f"raw_scaled must be int|None, got {type(raw_scaled)!r}")
    return f"{raw_scaled:0{width}d}"
