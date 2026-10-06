"""Read-only Pi durable-sale ledger vs cloud export reconciliation."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SaleRow:
    identity: str
    transaction_id: str
    station_id: str
    pump_id: str | None
    nozzle_id: str | None
    amount: Decimal | None
    volume: Decimal | None
    price: Decimal | None
    completed_at: str | None
    source: str


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _load_json_rows(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    raw = json.loads(text)
    if isinstance(raw, dict) and isinstance(raw.get("sales"), list):
        return list(raw["sales"])
    if isinstance(raw, list):
        return list(raw)
    raise ValueError(f"{path} must be a JSON list or {{sales: [...]}}")


def _identity_from_row(row: dict[str, Any], *, default_station: str) -> str:
    dedupe = str(
        row.get("deduplicationKey")
        or row.get("deduplication_key")
        or row.get("source_completion_key")
        or ""
    ).strip()
    station = str(row.get("stationId") or row.get("station_id") or default_station).strip()
    tx = str(
        row.get("transactionId")
        or row.get("transaction_id")
        or row.get("transaction_uuid")
        or row.get("id")
        or ""
    ).strip()
    if dedupe:
        return f"{station}|{dedupe}"
    if tx:
        return f"{station}|tx:{tx}"
    raise ValueError(f"row missing identity fields: {row!r}")


def normalize_row(row: dict[str, Any], *, source: str, default_station: str) -> SaleRow:
    station = str(row.get("stationId") or row.get("station_id") or default_station).strip()
    tx = str(
        row.get("transactionId")
        or row.get("transaction_id")
        or row.get("transaction_uuid")
        or row.get("id")
        or ""
    ).strip()
    return SaleRow(
        identity=_identity_from_row(row, default_station=default_station),
        transaction_id=tx,
        station_id=station,
        pump_id=(
            str(row.get("pumpId") or row.get("pump_id")).strip()
            if row.get("pumpId") or row.get("pump_id")
            else None
        ),
        nozzle_id=(
            str(row.get("nozzleId") or row.get("nozzle_id")).strip()
            if row.get("nozzleId") or row.get("nozzle_id")
            else None
        ),
        amount=_dec(row.get("amount")),
        volume=_dec(row.get("volumeLiters") or row.get("volume_liters")),
        price=_dec(row.get("pricePerLiter") or row.get("price_per_liter")),
        completed_at=str(
            row.get("transactionCompletedAt")
            or row.get("transaction_completed_at")
            or row.get("completedAt")
            or row.get("receivedAt")
            or ""
        )
        or None,
        source=source,
    )


def compare_ledgers(
    *,
    pi_rows: list[SaleRow],
    cloud_rows: list[SaleRow],
    manager_reported_amount: Decimal | None = None,
) -> dict[str, Any]:
    pi_by_id = {r.identity: r for r in pi_rows}
    cloud_by_id = {r.identity: r for r in cloud_rows}
    matched = sorted(set(pi_by_id) & set(cloud_by_id))
    pi_only = sorted(set(pi_by_id) - set(cloud_by_id))
    cloud_only = sorted(set(cloud_by_id) - set(pi_by_id))
    conflicts: list[dict[str, Any]] = []
    for key in matched:
        a = pi_by_id[key]
        b = cloud_by_id[key]
        diffs: dict[str, Any] = {}
        if a.amount != b.amount:
            diffs["amount"] = {"pi": str(a.amount), "cloud": str(b.amount)}
        if a.volume != b.volume:
            diffs["volume"] = {"pi": str(a.volume), "cloud": str(b.volume)}
        if a.price is not None and b.price is not None and a.price != b.price:
            diffs["price"] = {"pi": str(a.price), "cloud": str(b.price)}
        if diffs:
            conflicts.append({"identity": key, "diffs": diffs})

    # Suspicious equal amount/time candidates — never auto-confirmed duplicates.
    suspicious: list[dict[str, Any]] = []
    cloud_groups: dict[tuple[str | None, str | None, str | None], list[SaleRow]] = defaultdict(
        list
    )
    for row in cloud_rows:
        cloud_groups[(row.pump_id, str(row.amount), str(row.volume))].append(row)
    for (pump, amount, volume), group in cloud_groups.items():
        if len(group) < 2:
            continue
        suspicious.append(
            {
                "note": "equal_amount_volume_candidates_not_confirmed_duplicates",
                "pumpId": pump,
                "amount": amount,
                "volume": volume,
                "identities": [g.identity for g in group],
            }
        )

    def _sum(rows: list[SaleRow], attr: str) -> str:
        total = Decimal("0")
        for row in rows:
            val = getattr(row, attr)
            if val is not None:
                total += val
        return str(total)

    pi_amount = _sum(pi_rows, "amount")
    cloud_amount = _sum(cloud_rows, "amount")
    report = {
        "generatedAt": datetime.now(UTC).isoformat(),
        "completenessWarnings": [
            "Empty outbox does not prove pump capture completeness.",
            "Matching totals do not prove every individual sale is correct.",
            "Equal amount/time patterns are suspicious candidates only.",
        ],
        "counts": {
            "pi": len(pi_rows),
            "cloud": len(cloud_rows),
            "matchedIdentities": len(matched),
            "piOnly": len(pi_only),
            "cloudOnly": len(cloud_only),
            "conflicts": len(conflicts),
        },
        "totals": {
            "piAmount": pi_amount,
            "cloudAmount": cloud_amount,
            "piVolume": _sum(pi_rows, "volume"),
            "cloudVolume": _sum(cloud_rows, "volume"),
            "amountDeltaCloudMinusPi": str(Decimal(cloud_amount) - Decimal(pi_amount)),
        },
        "matchedIdentities": matched,
        "piOnlyIdentities": pi_only,
        "cloudOnlyIdentities": cloud_only,
        "conflicts": conflicts,
        "suspiciousEqualAmountCandidates": suspicious,
        "perNozzle": _per_nozzle(pi_rows, cloud_rows),
        "managerReportedAmount": str(manager_reported_amount)
        if manager_reported_amount is not None
        else None,
        "managerVsCloudDelta": (
            str(Decimal(cloud_amount) - manager_reported_amount)
            if manager_reported_amount is not None
            else None
        ),
        "rows": {
            "pi": [asdict(r) for r in pi_rows],
            "cloud": [asdict(r) for r in cloud_rows],
        },
    }
    return report


def _per_nozzle(pi_rows: list[SaleRow], cloud_rows: list[SaleRow]) -> list[dict[str, Any]]:
    keys = sorted(
        {
            (r.pump_id or "?", r.nozzle_id or "?")
            for r in (*pi_rows, *cloud_rows)
        }
    )
    out: list[dict[str, Any]] = []
    for pump, nozzle in keys:
        pi = [r for r in pi_rows if (r.pump_id or "?") == pump and (r.nozzle_id or "?") == nozzle]
        cloud = [
            r
            for r in cloud_rows
            if (r.pump_id or "?") == pump and (r.nozzle_id or "?") == nozzle
        ]
        pi_amt = sum((r.amount or Decimal("0") for r in pi), Decimal("0"))
        cloud_amt = sum((r.amount or Decimal("0") for r in cloud), Decimal("0"))
        pi_vol = sum((r.volume or Decimal("0") for r in pi), Decimal("0"))
        cloud_vol = sum((r.volume or Decimal("0") for r in cloud), Decimal("0"))
        out.append(
            {
                "pumpId": pump,
                "nozzleId": nozzle,
                "piCount": len(pi),
                "cloudCount": len(cloud),
                "piAmount": str(pi_amt),
                "cloudAmount": str(cloud_amt),
                "piVolume": str(pi_vol),
                "cloudVolume": str(cloud_vol),
                "amountDelta": str(cloud_amt - pi_amt),
                "volumeDelta": str(cloud_vol - pi_vol),
            }
        )
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="intelipump-sale-reconcile",
        description=(
            "Read-only compare of Pi durable sale export vs cloud sale export. "
            "Never deletes or merges historical rows."
        ),
    )
    p.add_argument("--pi-export", type=Path, required=True, help="JSON export from Pi ledger")
    p.add_argument(
        "--cloud-export", type=Path, required=True, help="JSON export from cloud/dashboard"
    )
    p.add_argument("--station-id", required=True)
    p.add_argument("--manager-reported-amount", type=str, default=None)
    p.add_argument("--output", type=Path, default=None)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    pi_raw = _load_json_rows(args.pi_export)
    cloud_raw = _load_json_rows(args.cloud_export)
    pi_rows = [
        normalize_row(r, source="pi", default_station=args.station_id) for r in pi_raw
    ]
    cloud_rows = [
        normalize_row(r, source="cloud", default_station=args.station_id) for r in cloud_raw
    ]
    manager = _dec(args.manager_reported_amount) if args.manager_reported_amount else None
    report = compare_ledgers(
        pi_rows=pi_rows, cloud_rows=cloud_rows, manager_reported_amount=manager
    )
    body = json.dumps(report, indent=2, default=str)
    if args.output:
        args.output.write_text(body + "\n", encoding="utf-8")
    else:
        print(body)
    # Non-zero when mismatches exist (useful for LAB CI gates).
    counts = report["counts"]
    if counts["piOnly"] or counts["cloudOnly"] or counts["conflicts"]:
        return 2
    return 0


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    run()
