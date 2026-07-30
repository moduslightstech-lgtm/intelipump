"""Read-only DART application transaction decoders.

Source of truth: WAYNE EUROPE - Protocol Specification Dart Pump Interface
Revision 2.11 (WM041550 Rev 02).

Direction defaults to UNKNOWN on merged-bus captures. Structural length cues
are used only where the specification makes CD vs DC layouts distinguishable
(documented in warnings when a collision remains).
"""

from __future__ import annotations

from typing import Any

from intelipump_fdc.protocol.dart.application.bcd import BcdError
from intelipump_fdc.protocol.dart.application.constants import (
    DecodeStatus,
    MessageDirection,
    PumpControlCommand,
    TransactionType,
)
from intelipump_fdc.protocol.dart.application.fields import decode_scaled_bcd
from intelipump_fdc.protocol.dart.application.models import (
    ApplicationTransaction,
    DecodeBundle,
    RawTransaction,
)
from intelipump_fdc.protocol.dart.application.nozio import decode_nozio
from intelipump_fdc.protocol.dart.application.splitter import split_transactions
from intelipump_fdc.protocol.dart.application.status import describe_wayne_status


def decode_data_payload(
    payload: bytes,
    *,
    pump_address: int | None = None,
    line_sequence: int | None = None,
    source_frame_raw_hex: str | None = None,
    volume_decimals: int | None = None,
    amount_decimals: int | None = None,
    price_decimals: int | None = None,
) -> DecodeBundle:
    """Split and decode all application transactions in a DATA payload."""
    split = split_transactions(payload)
    decoded: list[ApplicationTransaction] = []
    for raw_tx in split.transactions:
        decoded.append(
            decode_raw_transaction(
                raw_tx,
                pump_address=pump_address,
                line_sequence=line_sequence,
                source_frame_raw_hex=source_frame_raw_hex,
                volume_decimals=volume_decimals,
                amount_decimals=amount_decimals,
                price_decimals=price_decimals,
            )
        )
    return DecodeBundle(
        transactions=tuple(decoded),
        trailing_bytes=split.trailing_bytes,
        warnings=split.warnings,
    )


def decode_raw_transaction(
    raw_tx: RawTransaction,
    *,
    pump_address: int | None = None,
    line_sequence: int | None = None,
    source_frame_raw_hex: str | None = None,
    volume_decimals: int | None = None,
    amount_decimals: int | None = None,
    price_decimals: int | None = None,
) -> ApplicationTransaction:
    """Decode one raw TRANS+LNG+DATA unit."""
    tid = raw_tx.transaction_id
    lng = raw_tx.length

    if tid == 0x01 and lng == 1:
        return _decode_ambiguous_01(
            raw_tx,
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
        )
    if tid == 0x02 and lng == 8:
        return _decode_dc2(
            raw_tx,
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
            volume_decimals=volume_decimals,
            amount_decimals=amount_decimals,
        )
    if tid == 0x03 and lng == 4:
        return _decode_ambiguous_03(
            raw_tx,
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
            price_decimals=price_decimals,
        )
    if tid == 0x05 and lng == 1:
        return _decode_dc5(
            raw_tx,
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
        )
    if tid == 0x05 and lng >= 3 and lng % 3 == 0:
        return _decode_cd5(
            raw_tx,
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
            price_decimals=price_decimals,
        )
    if tid == 0x65 and lng == 1:
        return _decode_cd101_request(
            raw_tx,
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
        )
    if tid == 0x65 and lng == 16:
        return _decode_dc101(
            raw_tx,
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
            volume_decimals=volume_decimals,
            amount_decimals=amount_decimals,
        )

    return _unknown(raw_tx, pump_address, line_sequence, source_frame_raw_hex)


def _envelope(
    raw_tx: RawTransaction,
    *,
    transaction_type: TransactionType,
    direction: MessageDirection,
    decode_status: DecodeStatus,
    decoded_body: dict[str, Any] | None,
    warnings: list[str],
    pump_address: int | None,
    line_sequence: int | None,
    source_frame_raw_hex: str | None,
    reserved_or_unknown_fields: dict[str, str] | None = None,
) -> ApplicationTransaction:
    return ApplicationTransaction(
        transaction_id=raw_tx.transaction_id,
        transaction_type=transaction_type,
        length=raw_tx.length,
        raw_payload=raw_tx.data,
        raw_transaction=raw_tx.raw,
        decoded_body=decoded_body,
        pump_address=pump_address,
        line_sequence=line_sequence,
        direction=direction,
        decode_status=decode_status,
        warnings=tuple(warnings),
        source_frame_raw_hex=source_frame_raw_hex,
        offset=raw_tx.offset,
        reserved_or_unknown_fields=reserved_or_unknown_fields or {},
    )


def _unknown(
    raw_tx: RawTransaction,
    pump_address: int | None,
    line_sequence: int | None,
    source_frame_raw_hex: str | None,
) -> ApplicationTransaction:
    return _envelope(
        raw_tx,
        transaction_type=TransactionType.UNKNOWN,
        direction=MessageDirection.UNKNOWN,
        decode_status=DecodeStatus.UNKNOWN,
        decoded_body=None,
        warnings=[
            f"No read-only decoder for TRANS=0x{raw_tx.transaction_id:02X} "
            f"LNG={raw_tx.length}"
        ],
        pump_address=pump_address,
        line_sequence=line_sequence,
        source_frame_raw_hex=source_frame_raw_hex,
    )


def _decode_ambiguous_01(
    raw_tx: RawTransaction,
    *,
    pump_address: int | None,
    line_sequence: int | None,
    source_frame_raw_hex: str | None,
) -> ApplicationTransaction:
    """CD1 and DC1 share TRANS=0x01 / LNG=1 (Pump Interface pp. 13 and 20)."""
    code = raw_tx.data[0]
    status = describe_wayne_status(code)
    try:
        command = PumpControlCommand(code)
        command_name = command.name
    except ValueError:
        command_name = None

    warnings = [
        "TRANS 0x01 LNG=1 is ambiguous: CD1 (command) and DC1 (pump status) "
        "share this wire encoding (Pump Interface Rev 2.11, pages 13 and 20). "
        "Merged-bus captures cannot distinguish direction; both interpretations "
        "are preserved.",
    ]
    body: dict[str, Any] = {
        "raw_code": code,
        "dc1_pump_status": {
            "known": status.known,
            "name": status.name,
            "description": status.description,
        },
        "cd1_command": {
            "known": command_name is not None,
            "name": command_name,
            "dcc": code,
        },
    }
    return _envelope(
        raw_tx,
        transaction_type=TransactionType.AMBIGUOUS_CD1_OR_DC1,
        direction=MessageDirection.UNKNOWN,
        decode_status=DecodeStatus.PARTIAL,
        decoded_body=body,
        warnings=warnings,
        pump_address=pump_address,
        line_sequence=line_sequence,
        source_frame_raw_hex=source_frame_raw_hex,
    )


def _decode_dc2(
    raw_tx: RawTransaction,
    *,
    pump_address: int | None,
    line_sequence: int | None,
    source_frame_raw_hex: str | None,
    volume_decimals: int | None,
    amount_decimals: int | None,
) -> ApplicationTransaction:
    """DC2 filled volume/amount — Pump Interface Rev 2.11, page 20.

    Structural note: CD2 (allowed nozzles) also uses TRANS=0x02, but DC2 is
    documented as VOL(4)+AMO(4)=8 packed-BCD bytes. LNG=8 is treated as DC2.
    """
    warnings = [
        "Interpreted TRANS 0x02 LNG=8 as DC2 (VOL+AMO). CD2 shares TRANS=0x02 "
        "but has a variable nozzle-list layout (Pump Interface pp. 14 and 20).",
    ]
    try:
        volume = decode_scaled_bcd(raw_tx.data[0:4], decimals=volume_decimals)
        amount = decode_scaled_bcd(raw_tx.data[4:8], decimals=amount_decimals)
    except BcdError as exc:
        return _envelope(
            raw_tx,
            transaction_type=TransactionType.DC2_FILLED_VOLUME_AMOUNT,
            direction=MessageDirection.SLAVE_TO_MASTER,
            decode_status=DecodeStatus.MALFORMED,
            decoded_body=None,
            warnings=[*warnings, f"invalid BCD in DC2: {exc}"],
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
        )

    if volume_decimals is None or amount_decimals is None:
        warnings.append(
            "Volume/amount Decimal values omitted: pump decimal parameters "
            "(DC7 DPVOL/DPAMO) were not provided."
        )

    body = {
        "volume": volume.as_report_dict(),
        "amount": amount.as_report_dict(),
        "spec_ref": "Pump Interface Rev 2.11, page 20, DC2",
    }
    return _envelope(
        raw_tx,
        transaction_type=TransactionType.DC2_FILLED_VOLUME_AMOUNT,
        direction=MessageDirection.SLAVE_TO_MASTER,
        decode_status=DecodeStatus.DECODED,
        decoded_body=body,
        warnings=warnings,
        pump_address=pump_address,
        line_sequence=line_sequence,
        source_frame_raw_hex=source_frame_raw_hex,
    )


def _decode_ambiguous_03(
    raw_tx: RawTransaction,
    *,
    pump_address: int | None,
    line_sequence: int | None,
    source_frame_raw_hex: str | None,
    price_decimals: int | None,
) -> ApplicationTransaction:
    """CD3 and DC3 share TRANS=0x03 / LNG=4 (Pump Interface pp. 14 and 21).

    Passive / merged-bus decode must not claim DC3 (or CD3) without proven
    direction or controller-session context. Both structural views are
    preserved; NOZIO bit masks stay documented and unchanged.
    """
    warnings = [
        "TRANS 0x03 LNG=4 is ambiguous: CD3 (preset volume) and DC3 "
        "(filling price + NOZIO) share this wire encoding (Pump Interface "
        "Rev 2.11, pages 14 and 21). Direction/context required before "
        "treating as DC3; both interpretations are preserved.",
    ]

    # DC3 view: PRI(3) + NOZIO(1). Keep documented NOZIO masks.
    nozio = raw_tx.data[3]
    nozio_dec = decode_nozio(nozio)
    warnings.extend(nozio_dec.warnings)
    try:
        price = decode_scaled_bcd(raw_tx.data[0:3], decimals=price_decimals)
        price_view: dict[str, Any] = price.as_report_dict()
        price_ok = True
    except BcdError as exc:
        price_view = {"error": f"invalid BCD in DC3 price view: {exc}"}
        price_ok = False
        warnings.append(f"invalid BCD in DC3 price view: {exc}")

    if price_decimals is None and price_ok:
        warnings.append(
            "Price Decimal omitted: pump unit-price decimals (DC7 DPUNP) "
            "were not provided."
        )

    # CD3 view: 4-byte packed-BCD preset volume.
    try:
        cd3_volume = decode_scaled_bcd(raw_tx.data, decimals=None).as_report_dict()
    except BcdError as exc:
        cd3_volume = {"error": f"invalid BCD if interpreted as CD3 volume: {exc}"}

    body: dict[str, Any] = {
        # Convenience fields for callers that resolve as DC3.
        "price": price_view,
        "nozio_raw": nozio_dec.nozio_raw,
        "logical_nozzle_raw": nozio_dec.logical_nozzle_raw,
        "selected_logical_nozzle": nozio_dec.selected_logical_nozzle,
        "nozzle_out": nozio_dec.nozzle_out,
        "nozio": nozio_dec.to_evidence_dict(),
        "dc3_nozzle_status_price": {
            "price": price_view,
            "nozio_raw": nozio_dec.nozio_raw,
            "logical_nozzle_raw": nozio_dec.logical_nozzle_raw,
            "selected_logical_nozzle": nozio_dec.selected_logical_nozzle,
            "nozzle_out": nozio_dec.nozzle_out,
            "nozio": nozio_dec.to_evidence_dict(),
            "spec_ref": nozio_dec.documentation_source,
        },
        "cd3_preset_volume": {
            "volume": cd3_volume,
            "spec_ref": "Pump Interface Rev 2.11, page 14, CD3 Preset volume",
        },
        "spec_ref": (
            "Pump Interface Rev 2.11, pages 14 and 21 "
            "(CD3/DC3 TRANS 0x03 LNG=4 collision)"
        ),
    }
    return _envelope(
        raw_tx,
        transaction_type=TransactionType.AMBIGUOUS_CD3_OR_DC3,
        direction=MessageDirection.UNKNOWN,
        decode_status=DecodeStatus.PARTIAL if price_ok else DecodeStatus.MALFORMED,
        decoded_body=body,
        warnings=warnings,
        pump_address=pump_address,
        line_sequence=line_sequence,
        source_frame_raw_hex=source_frame_raw_hex,
    )


def _decode_cd5(
    raw_tx: RawTransaction,
    *,
    pump_address: int | None,
    line_sequence: int | None,
    source_frame_raw_hex: str | None,
    price_decimals: int | None,
) -> ApplicationTransaction:
    """CD5 price update — Pump Interface Rev 2.11, page 15 (LNG = 3 * n)."""
    warnings: list[str] = []
    prices: list[dict[str, Any]] = []
    try:
        for index in range(0, raw_tx.length, 3):
            chunk = raw_tx.data[index : index + 3]
            prices.append(
                {
                    "logical_nozzle": (index // 3) + 1,
                    "price": decode_scaled_bcd(chunk, decimals=price_decimals).as_report_dict(),
                }
            )
    except BcdError as exc:
        return _envelope(
            raw_tx,
            transaction_type=TransactionType.CD5_PRICE_UPDATE,
            direction=MessageDirection.MASTER_TO_SLAVE,
            decode_status=DecodeStatus.MALFORMED,
            decoded_body=None,
            warnings=[f"invalid BCD in CD5: {exc}"],
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
        )

    if price_decimals is None:
        warnings.append(
            "Price Decimal omitted: pump unit-price decimals (DC7 DPUNP) "
            "were not provided."
        )
    body = {"prices": prices, "spec_ref": "Pump Interface Rev 2.11, page 15, CD5"}
    return _envelope(
        raw_tx,
        transaction_type=TransactionType.CD5_PRICE_UPDATE,
        direction=MessageDirection.MASTER_TO_SLAVE,
        decode_status=DecodeStatus.DECODED,
        decoded_body=body,
        warnings=warnings,
        pump_address=pump_address,
        line_sequence=line_sequence,
        source_frame_raw_hex=source_frame_raw_hex,
    )


def _decode_dc5(
    raw_tx: RawTransaction,
    *,
    pump_address: int | None,
    line_sequence: int | None,
    source_frame_raw_hex: str | None,
) -> ApplicationTransaction:
    """DC5 alarm — Pump Interface Rev 2.11, page 22."""
    alarm = raw_tx.data[0]
    body = {
        "alarm_code": alarm,
        "spec_ref": "Pump Interface Rev 2.11, page 22, DC5",
    }
    return _envelope(
        raw_tx,
        transaction_type=TransactionType.DC5_ALARM,
        direction=MessageDirection.SLAVE_TO_MASTER,
        decode_status=DecodeStatus.DECODED,
        decoded_body=body,
        warnings=[],
        pump_address=pump_address,
        line_sequence=line_sequence,
        source_frame_raw_hex=source_frame_raw_hex,
    )


def _decode_cd101_request(
    raw_tx: RawTransaction,
    *,
    pump_address: int | None,
    line_sequence: int | None,
    source_frame_raw_hex: str | None,
) -> ApplicationTransaction:
    """CD101 request total counters — Pump Interface Rev 2.11, page 19."""
    body = {
        "counter_select": raw_tx.data[0],
        "spec_ref": "Pump Interface Rev 2.11, page 19, CD101",
    }
    return _envelope(
        raw_tx,
        transaction_type=TransactionType.CD101_REQUEST_TOTALS,
        direction=MessageDirection.MASTER_TO_SLAVE,
        decode_status=DecodeStatus.DECODED,
        decoded_body=body,
        warnings=[],
        pump_address=pump_address,
        line_sequence=line_sequence,
        source_frame_raw_hex=source_frame_raw_hex,
    )


def _decode_dc101(
    raw_tx: RawTransaction,
    *,
    pump_address: int | None,
    line_sequence: int | None,
    source_frame_raw_hex: str | None,
    volume_decimals: int | None,
    amount_decimals: int | None,
) -> ApplicationTransaction:
    """DC101 total counters — Pump Interface Rev 2.11, page 25 (LNG=16)."""
    warnings: list[str] = []
    coun = raw_tx.data[0]
    try:
        totval = decode_scaled_bcd(raw_tx.data[1:6], decimals=None)
        totm1 = decode_scaled_bcd(raw_tx.data[6:11], decimals=None)
        totm2 = decode_scaled_bcd(raw_tx.data[11:16], decimals=None)
    except BcdError as exc:
        return _envelope(
            raw_tx,
            transaction_type=TransactionType.DC101_TOTAL_COUNTERS,
            direction=MessageDirection.SLAVE_TO_MASTER,
            decode_status=DecodeStatus.MALFORMED,
            decoded_body=None,
            warnings=[f"invalid BCD in DC101: {exc}"],
            pump_address=pump_address,
            line_sequence=line_sequence,
            source_frame_raw_hex=source_frame_raw_hex,
        )

    # Optional Decimal only if caller supplied decimals; COUN selects meaning.
    decimals_for_value = None
    if 0x01 <= coun <= 0x09:
        decimals_for_value = volume_decimals
    elif 0x11 <= coun <= 0x19:
        decimals_for_value = amount_decimals

    def _maybe_decimal(scaled_raw: bytes) -> dict[str, Any]:
        try:
            return decode_scaled_bcd(scaled_raw, decimals=decimals_for_value).as_report_dict()
        except BcdError as exc:  # pragma: no cover - already validated above
            return {"error": str(exc)}

    if decimals_for_value is None:
        warnings.append(
            "DC101 Decimal presentation omitted: decimal parameters unknown "
            "and/or COUN meaning does not select a provided scale."
        )

    body = {
        "counter_select": coun,
        "total_value": _maybe_decimal(raw_tx.data[1:6]),
        "total_meter1_or_nofill": _maybe_decimal(raw_tx.data[6:11]),
        "total_meter2": _maybe_decimal(raw_tx.data[11:16]),
        "raw_scaled": {
            "total_value": totval.raw_scaled,
            "total_meter1_or_nofill": totm1.raw_scaled,
            "total_meter2": totm2.raw_scaled,
        },
        "spec_ref": "Pump Interface Rev 2.11, page 25, DC101",
    }
    return _envelope(
        raw_tx,
        transaction_type=TransactionType.DC101_TOTAL_COUNTERS,
        direction=MessageDirection.SLAVE_TO_MASTER,
        decode_status=DecodeStatus.DECODED,
        decoded_body=body,
        warnings=warnings,
        pump_address=pump_address,
        line_sequence=line_sequence,
        source_frame_raw_hex=source_frame_raw_hex,
    )
