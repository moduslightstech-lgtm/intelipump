"""Passive DART application transaction parsing for capture analysis.

Uses existing read-only decoders. Does not invent undocumented fields.

NOZIO note (tool-local documented-mask decode for capture analysis):
  logicalNozzle = raw & 0x0F
  nozzlePosition = OUT if raw & 0x10 else IN
  reservedBits = raw & 0xE0  (warn if nonzero)

This is offline/passive evidence labeling, separate from production
state-machine nozzle UNKNOWN profile work.
"""

from __future__ import annotations

from typing import Any

from intelipump_fdc.protocol.dart.application.constants import TransactionType
from intelipump_fdc.protocol.dart.application.decoder import decode_data_payload
from intelipump_fdc.protocol.dart.application.nozio import decode_nozio
from intelipump_fdc.protocol.dart.application.status import describe_wayne_status

# Capture-analysis DC1 labels (Rev 2.11 page 20). Code 0x06 labeled LIMIT_REACHED
# for this tool; production status enum uses MAX_AMOUNT_VOLUME_REACHED.
DC1_STATUS_LABELS: dict[int, str] = {
    0x00: "PUMP_NOT_PROGRAMMED",
    0x01: "RESET",
    0x02: "AUTHORIZED",
    0x04: "FILLING",
    0x05: "FILLING_COMPLETED",
    0x06: "LIMIT_REACHED",
    0x07: "SWITCHED_OFF",
    0x08: "SUSPENDED",
}


def parse_data_payload(
    payload: bytes,
    *,
    pump_address: int | None = None,
    line_sequence: int | None = None,
    source_frame_raw_hex: str | None = None,
) -> list[dict[str, Any]]:
    """Decode DATA-frame payload into evidence-friendly transaction dicts."""
    if not payload:
        return []

    bundle = decode_data_payload(
        payload,
        pump_address=pump_address,
        line_sequence=line_sequence,
        source_frame_raw_hex=source_frame_raw_hex,
    )
    results: list[dict[str, Any]] = []
    for tx in bundle.transactions:
        results.append(_to_evidence_tx(tx))
    if bundle.trailing_bytes:
        results.append(
            {
                "transactionId": None,
                "length": len(bundle.trailing_bytes),
                "dataHex": bundle.trailing_bytes.hex(" ").upper(),
                "label": "TRAILING_BYTES",
                "decoded": None,
                "warnings": list(bundle.warnings),
                "unknown": True,
            }
        )
    elif bundle.warnings and results:
        # Attach splitter warnings to first tx when no trailing record.
        results[0]["warnings"] = [
            *results[0].get("warnings", []),
            *bundle.warnings,
        ]
    return results


def _to_evidence_tx(tx: Any) -> dict[str, Any]:
    tid = int(tx.transaction_id)
    base: dict[str, Any] = {
        "transactionId": tid,
        "transactionIdHex": f"{tid:02X}",
        "length": int(tx.length),
        "dataHex": tx.raw_payload.hex(" ").upper(),
        "rawTransactionHex": tx.raw_transaction.hex(" ").upper(),
        "transactionType": tx.transaction_type.value,
        "decodeStatus": tx.decode_status.value,
        "warnings": list(tx.warnings),
        "unknown": tx.transaction_type is TransactionType.UNKNOWN,
        "decoded": None,
        "label": None,
    }

    body = tx.decoded_body or {}

    if tx.transaction_type is TransactionType.AMBIGUOUS_CD1_OR_DC1:
        code = int(body.get("raw_code", tx.raw_payload[0] if tx.raw_payload else -1))
        label = DC1_STATUS_LABELS.get(code)
        status = describe_wayne_status(code)
        base["label"] = "DC1_STATUS" if label else "DC1_OR_CD1_UNKNOWN"
        base["decoded"] = {
            "kind": "DC1",
            "statusCode": code,
            "statusCodeHex": f"{code:02X}",
            "statusLabel": label or f"UNKNOWN_STATUS_0x{code:02X}",
            "wayneStatusName": status.name,
            "wayneStatusDescription": status.description,
            "known": status.known or label is not None,
            # Preserve CD1 ambiguity from production decoder; do not claim direction.
            "cd1Command": body.get("cd1_command"),
            "note": (
                "Merged-bus: TRANS 0x01 LNG=1 is ambiguous CD1/DC1; "
                "statusLabel is documented DC1 mapping for capture analysis."
            ),
        }
        return base

    if tx.transaction_type is TransactionType.DC2_FILLED_VOLUME_AMOUNT:
        base["label"] = "DC2_VOLUME_AMOUNT"
        volume = body.get("volume") or {}
        amount = body.get("amount") or {}
        base["decoded"] = {
            "kind": "DC2",
            "volumeBcdHex": tx.raw_payload[0:4].hex(" ").upper(),
            "amountBcdHex": tx.raw_payload[4:8].hex(" ").upper(),
            # Only include fields already verified by repo decoder.
            "volumeRawScaled": volume.get("raw_scaled"),
            "amountRawScaled": amount.get("raw_scaled"),
            "volumeValue": volume.get("value"),
            "amountValue": amount.get("value"),
            "specRef": body.get("spec_ref"),
        }
        return base

    if tx.transaction_type in {
        TransactionType.DC3_NOZZLE_STATUS_PRICE,
        TransactionType.AMBIGUOUS_CD3_OR_DC3,
    }:
        return _decode_dc3_view(base, tx.raw_payload, body)

    if tx.transaction_type is TransactionType.DC5_ALARM:
        base["label"] = "DC5"
        base["decoded"] = {
            "kind": "DC5",
            "rawHex": tx.raw_payload.hex(" ").upper(),
            "body": body if body else None,
            "note": "Preserved raw; only fields from verified decoder included.",
        }
        return base

    if tx.transaction_type is TransactionType.UNKNOWN:
        base["label"] = "UNKNOWN"
        base["decoded"] = {
            "kind": "UNKNOWN",
            "transactionId": tid,
            "length": int(tx.length),
            "dataHex": tx.raw_payload.hex(" ").upper(),
            "note": "No verified decoder; preserved without guessing.",
        }
        return base

    # Other verified types (CD5, DC101, …): preserve decoder body, no invention.
    base["label"] = tx.transaction_type.value
    base["decoded"] = {"kind": tx.transaction_type.value, "body": body}
    return base


def _decode_dc3_view(
    base: dict[str, Any],
    raw_payload: bytes,
    body: dict[str, Any],
) -> dict[str, Any]:
    """Documented-mask DC3 view for capture analysis (price + NOZIO)."""
    base["label"] = "DC3_PRICE_NOZIO"
    price_hex = raw_payload[0:3].hex(" ").upper() if len(raw_payload) >= 3 else ""
    nozio_raw = int(raw_payload[3]) if len(raw_payload) >= 4 else 0
    # Tool-local documented-mask decode (also matches intelipump_fdc.nozio).
    nozio = decode_nozio(nozio_raw)
    logical = nozio_raw & 0x0F
    position = "OUT" if (nozio_raw & 0x10) else "IN"
    reserved = nozio_raw & 0xE0
    warnings = list(base.get("warnings") or [])
    warnings.extend(nozio.warnings)
    if reserved:
        warnings.append(
            f"NOZIO reservedBits nonzero: 0x{reserved:02X} (documented-mask decode)"
        )

    price_view = body.get("price") if isinstance(body, dict) else None
    base["warnings"] = warnings
    base["decoded"] = {
        "kind": "DC3",
        "priceBcdHex": price_hex,
        "price": price_view,
        "nozioRaw": nozio_raw,
        "nozioRawHex": f"{nozio_raw:02X}",
        "logicalNozzle": logical,
        "nozzlePosition": position,
        "reservedBits": reserved,
        "reservedBitsHex": f"{reserved:02X}",
        "documentationNote": (
            "Documented-mask decode for capture analysis: "
            "OUT if raw&0x10 else IN; separate from production UNKNOWN nozzle profile."
        ),
        "nozioEvidence": nozio.to_evidence_dict(),
    }
    return base
