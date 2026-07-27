"""Evidence + technician review manifest for CD5 price dry-run."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from intelipump_fdc.bench_poll.guards import software_commit


@dataclass
class DryRunEvidenceBundle:
    session_id: str
    commit: str
    target_type: str
    logical_address: int
    wire_address: int
    serial_config: dict[str, Any]
    status_poll_tx_hex: str
    status_response_raw_hex: str
    decoded_status: dict[str, Any]
    confirmations: dict[str, bool]
    logical_nozzle_count: int
    prices: list[dict[str, Any]]
    cd5_payload_hex: str
    cd5_breakdown: dict[str, Any]
    candidate_frame_hex: str
    crc_hex: str
    sequence: int
    expected_ack_hex: str
    expected_status_after: dict[str, Any]
    dry_run_state: str
    transmitted: bool
    serial_write_called_for_candidate: bool
    write_refused_by_software: bool
    poll_write_count: int
    remaining_uncertainties: list[str]
    refusal_reasons: list[str]


def write_evidence(
    evidence_dir: Path,
    bundle: DryRunEvidenceBundle,
) -> dict[str, Path]:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    jsonl = evidence_dir / "price-dry-run.jsonl"
    md = evidence_dir / "price-dry-run.md"
    review = evidence_dir / "price-programming-review.json"

    record = {
        "schemaVersion": 1,
        "sessionId": bundle.session_id,
        "timestampUtc": datetime.now(UTC).isoformat(),
        "softwareCommit": bundle.commit,
        "targetType": bundle.target_type,
        "candidateOnly": True,
        "transmitted": bundle.transmitted,
        "serialWriteCalledForCandidate": bundle.serial_write_called_for_candidate,
        "writeRefusedBySoftware": bundle.write_refused_by_software,
        "pollWriteCount": bundle.poll_write_count,
        "logicalAddress": bundle.logical_address,
        "wireAddress": f"0x{bundle.wire_address:02X}",
        "serialConfig": bundle.serial_config,
        "statusPollTxHex": bundle.status_poll_tx_hex,
        "statusResponseRawHex": bundle.status_response_raw_hex,
        "decodedStatus": bundle.decoded_status,
        "confirmations": bundle.confirmations,
        "logicalNozzleCount": bundle.logical_nozzle_count,
        "prices": bundle.prices,
        "cd5PayloadHex": bundle.cd5_payload_hex,
        "cd5Breakdown": bundle.cd5_breakdown,
        "candidateFrameHex": bundle.candidate_frame_hex,
        "crc": bundle.crc_hex,
        "sequence": bundle.sequence,
        "expectedAckHypothesis": bundle.expected_ack_hex,
        "expectedStatusAfterValidPrice": bundle.expected_status_after,
        "dryRunState": bundle.dry_run_state,
        "remainingUncertainties": bundle.remaining_uncertainties,
        "refusalReasons": bundle.refusal_reasons,
    }
    jsonl.write_text(json.dumps(record, separators=(",", ":")) + "\n", encoding="utf-8")

    review_obj = {
        "targetType": bundle.target_type,
        "candidateOnly": True,
        "transmitted": False,
        "wireAddress": f"0x{bundle.wire_address:02X}",
        "logicalAddress": bundle.logical_address,
        "currentStatus": {
            "code": (bundle.decoded_status.get("dc1") or {}).get("code"),
            "name": (bundle.decoded_status.get("dc1") or {}).get("name"),
        },
        "logicalNozzleCount": bundle.logical_nozzle_count,
        "logicalNozzleCountSource": "TECHNICIAN_CONFIRMED",
        "logicalNozzleMappingConfirmed": bundle.confirmations.get(
            "logicalNozzleMappingConfirmed", False
        ),
        "priceScaleConfirmed": bundle.confirmations.get("priceScaleConfirmed", False),
        "prices": bundle.prices,
        "cd5PayloadHex": bundle.cd5_payload_hex,
        "candidateFrameHex": bundle.candidate_frame_hex,
        "crc": bundle.crc_hex,
        "expectedStatusAfterValidPrice": bundle.expected_status_after,
        "authorizationIncluded": False,
        "resetIncluded": False,
        "presetIncluded": False,
        "motorIsolated": bundle.confirmations.get("motorIsolated", False),
        "valvesIsolated": bundle.confirmations.get("valvesIsolated", False),
        "productConnected": not bundle.confirmations.get("noProductConnected", False),
        "technicianPresent": bundle.confirmations.get("technicianPresent", False),
        "technicianApproval": "PENDING",
        "writeRefusedBySoftware": True,
        "serialWriteCalledForCandidate": False,
        "dryRunState": bundle.dry_run_state,
        "softwareCommit": bundle.commit,
    }
    review.write_text(json.dumps(review_obj, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# Real-Wayne CD5 price programming dry-run",
        "",
        f"- Software commit: `{bundle.commit}`",
        f"- Session: `{bundle.session_id}`",
        f"- Target: `{bundle.target_type}`",
        f"- Transmitted: `{bundle.transmitted}`",
        f"- serialWriteCalledForCandidate: `{bundle.serial_write_called_for_candidate}`",
        f"- writeRefusedBySoftware: `{bundle.write_refused_by_software}`",
        f"- Dry-run state: `{bundle.dry_run_state}`",
        f"- Logical address: `{bundle.logical_address}`",
        f"- Wire address: `0x{bundle.wire_address:02X}`",
        f"- Status poll TX: `{bundle.status_poll_tx_hex}`",
        f"- Status RX: `{bundle.status_response_raw_hex}`",
        f"- Current DC1: `{review_obj['currentStatus']}`",
        f"- Logical nozzle count: `{bundle.logical_nozzle_count}` (technician-confirmed)",
        f"- Prices: `{bundle.prices}`",
        f"- CD5 payload: `{bundle.cd5_payload_hex}`",
        f"- Candidate frame: `{bundle.candidate_frame_hex}`",
        f"- CRC: `{bundle.crc_hex}`",
        f"- Expected ACK hypothesis: `{bundle.expected_ack_hex}`",
        f"- Expected after valid price: `{bundle.expected_status_after}`",
        "",
        "## Confirmations",
        "",
        "```json",
        json.dumps(bundle.confirmations, indent=2),
        "```",
        "",
        "## Remaining uncertainties",
        "",
    ]
    for u in bundle.remaining_uncertainties:
        lines.append(f"- {u}")
    if bundle.refusal_reasons:
        lines.extend(["", "## Refusal reasons", ""])
        for r in bundle.refusal_reasons:
            lines.append(f"- {r}")
    lines.append("")
    md.write_text("\n".join(lines), encoding="utf-8")
    return {"jsonl": jsonl, "md": md, "review": review}


def new_session_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S") + "-price-dry-run"


def default_uncertainties() -> list[str]:
    return [
        "Exact line-layer ACK behavior for CD5 on this pump is unproven",
        "Required timing after CD5 is unproven",
        "Whether other programming transactions are also required is unproven",
        "Exact meaning of earlier merged-capture Cx frames is unproven",
        "Real pump accept/reject response to this price block is unproven",
        "DC3 selectedLogicalNozzle does not prove configured nozzle count",
    ]


__all__ = [
    "DryRunEvidenceBundle",
    "default_uncertainties",
    "new_session_id",
    "software_commit",
    "write_evidence",
]
