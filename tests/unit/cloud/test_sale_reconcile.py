"""Unit tests for read-only sale reconciliation compare."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from intelipump_fdc.cloud.reconcile.cli import compare_ledgers, main, normalize_row


def test_equal_value_distinct_identities_remain_separate() -> None:
    pi = [
        normalize_row(
            {
                "stationId": "LAB",
                "transactionId": "tx-1",
                "deduplicationKey": "tx-completed:LAB:complete:tx-1",
                "pumpId": "pump-1",
                "nozzleId": "1",
                "amount": "5000",
                "volumeLiters": "3.65",
            },
            source="pi",
            default_station="LAB",
        ),
        normalize_row(
            {
                "stationId": "LAB",
                "transactionId": "tx-2",
                "deduplicationKey": "tx-completed:LAB:complete:tx-2",
                "pumpId": "pump-1",
                "nozzleId": "1",
                "amount": "5000",
                "volumeLiters": "3.65",
            },
            source="pi",
            default_station="LAB",
        ),
    ]
    cloud = list(pi)
    report = compare_ledgers(pi_rows=pi, cloud_rows=cloud)
    assert report["counts"]["matchedIdentities"] == 2
    assert report["counts"]["piOnly"] == 0
    assert report["suspiciousEqualAmountCandidates"]


def test_conflict_and_pi_only(tmp_path: Path) -> None:
    pi_path = tmp_path / "pi.json"
    cloud_path = tmp_path / "cloud.json"
    pi_path.write_text(
        json.dumps(
            [
                {
                    "stationId": "LAB",
                    "transactionId": "tx-a",
                    "deduplicationKey": "k-a",
                    "amount": "1000",
                    "volumeLiters": "1",
                    "pumpId": "pump-1",
                    "nozzleId": "1",
                },
                {
                    "stationId": "LAB",
                    "transactionId": "tx-b",
                    "deduplicationKey": "k-b",
                    "amount": "2000",
                    "volumeLiters": "2",
                    "pumpId": "pump-1",
                    "nozzleId": "2",
                },
            ]
        ),
        encoding="utf-8",
    )
    cloud_path.write_text(
        json.dumps(
            [
                {
                    "stationId": "LAB",
                    "transactionId": "tx-a",
                    "deduplicationKey": "k-a",
                    "amount": "1001",
                    "volumeLiters": "1",
                    "pumpId": "pump-1",
                    "nozzleId": "1",
                }
            ]
        ),
        encoding="utf-8",
    )
    out = tmp_path / "report.json"
    code = main(
        [
            "--pi-export",
            str(pi_path),
            "--cloud-export",
            str(cloud_path),
            "--station-id",
            "LAB",
            "--manager-reported-amount",
            "900",
            "--output",
            str(out),
        ]
    )
    assert code == 2
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["counts"]["conflicts"] == 1
    assert report["counts"]["piOnly"] == 1
    assert report["managerVsCloudDelta"] == str(Decimal("1001") - Decimal("900"))
