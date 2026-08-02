"""Offline analyzer for passive DART capture JSONL sessions."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tools.passive_dart_capture.direction_inference import (
    annotate_frames,
    build_dc1_transitions,
    build_nozio_transitions,
    build_rejected_address_diagnostics,
    build_window_report,
    compare_nozzle_cycles,
    sequence_around_nozio,
)
from tools.passive_dart_capture.evidence_writer import (
    DEFAULT_EVIDENCE_DIR,
    DEFAULT_REPORTS_DIR,
    ensure_dir,
)

# Focused lab windows for direction-aware reports (inclusive frameSequence).
DIRECTION_AWARE_WINDOWS: tuple[tuple[int, int], ...] = (
    (4070, 4140),
    (4900, 4980),
    (5490, 5570),
    (5960, 6030),
)


@dataclass(slots=True)
class LoadedSession:
    session_id: str
    records: list[dict[str, Any]]
    frames: list[dict[str, Any]]  # deduped complete+incomplete frame records
    complete_frames: list[dict[str, Any]]
    markers: list[dict[str, Any]]
    chunks: list[dict[str, Any]]


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def dedupe_frames(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Dedupe by sessionId+frameSequence; keep first occurrence."""
    seen: set[tuple[str, int]] = set()
    out: list[dict[str, Any]] = []
    for fr in frames:
        key = (str(fr.get("sessionId", "")), int(fr.get("frameSequence", -1)))
        if key in seen:
            continue
        seen.add(key)
        out.append(fr)
    return out


def load_session(path: Path, *, session_id: str | None = None) -> LoadedSession:
    records = load_jsonl(path)
    sid = session_id or _infer_session_id(records, path)
    frames_raw = [r for r in records if r.get("recordType") == "frame"]
    frames = dedupe_frames(frames_raw)
    complete = [f for f in frames if f.get("complete") is True]
    markers = [r for r in records if r.get("recordType") == "operator_marker"]
    chunks = [r for r in records if r.get("recordType") == "serial_chunk"]
    # Sort chronologically by available timestamps.
    frames.sort(key=_frame_sort_key)
    complete.sort(key=_frame_sort_key)
    markers.sort(key=lambda r: (r.get("monotonicTimestampNs", 0), r.get("captureTimestampUtc", "")))
    chunks.sort(key=lambda r: (r.get("chunkSequence", 0), r.get("monotonicTimestampNs", 0)))
    return LoadedSession(
        session_id=sid,
        records=records,
        frames=frames,
        complete_frames=complete,
        markers=markers,
        chunks=chunks,
    )


def _infer_session_id(records: list[dict[str, Any]], path: Path) -> str:
    for r in records:
        if r.get("sessionId"):
            return str(r["sessionId"])
    return path.stem


def _frame_sort_key(fr: dict[str, Any]) -> tuple[Any, ...]:
    return (
        fr.get("firstByteMonotonicNs", 0),
        fr.get("frameSequence", 0),
        fr.get("firstByteTimestampUtc", ""),
    )


def _iter_transactions(frame: dict[str, Any]) -> list[dict[str, Any]]:
    txs = frame.get("transactions") or []
    return list(txs) if isinstance(txs, list) else []


@dataclass(slots=True)
class AnalysisResult:
    session_id: str
    summary: dict[str, Any]
    timeline_rows: list[dict[str, Any]] = field(default_factory=list)
    status_transitions: list[dict[str, Any]] = field(default_factory=list)
    nozio_transitions: list[dict[str, Any]] = field(default_factory=list)
    unknown_transactions: list[dict[str, Any]] = field(default_factory=list)
    report_paths: dict[str, Path] = field(default_factory=dict)


@dataclass(slots=True)
class DirectionAwareAnalysisResult:
    session_id: str
    summary: dict[str, Any]
    status_transitions: list[dict[str, Any]] = field(default_factory=list)
    nozio_transitions: list[dict[str, Any]] = field(default_factory=list)
    rejected_addresses: list[dict[str, Any]] = field(default_factory=list)
    windows: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    nozio_sequences: list[dict[str, Any]] = field(default_factory=list)
    nozzle_cycle_comparison: dict[str, Any] = field(default_factory=dict)
    report_paths: dict[str, Path] = field(default_factory=dict)


def analyze_session(
    session: LoadedSession,
    *,
    reports_dir: Path | None = None,
) -> AnalysisResult:
    """Build reports from a loaded session (complete frames only for analytics)."""
    out_dir = ensure_dir(reports_dir or DEFAULT_REPORTS_DIR)
    sid = session.session_id
    frames = session.complete_frames  # avoid double-counting serial_chunks / partials

    timeline = _build_timeline(session)
    status_tx = _status_transitions(frames)
    nozio_tx = _nozio_transitions(frames)
    unknown = _unknown_transactions(frames)
    per_address = _per_address_summary(frames)
    price_changes = _price_changes(frames)
    cmd_resp = _command_response_timing(frames)
    startup = _startup_sequence(session)
    marker_latency = _marker_latencies(session, status_tx, nozio_tx)
    errors = _error_summary(session)

    summary = {
        "sessionId": sid,
        "recordCounts": {
            "totalRecords": len(session.records),
            "serialChunks": len(session.chunks),
            "frameRecords": len(session.frames),
            "completeFramesAnalyzed": len(frames),
            "markers": len(session.markers),
        },
        "bytesCaptured": sum(int(c.get("byteCount") or 0) for c in session.chunks),
        "perAddress": per_address,
        "dc1TransitionCount": len(status_tx),
        "nozioTransitionCount": len(nozio_tx),
        "priceChangeCount": len(price_changes),
        "priceChanges": price_changes,
        "commandResponseTiming": cmd_resp,
        "startupSequence": startup,
        "markerLatencies": marker_latency,
        "errors": errors,
        "unknownTransactionCount": len(unknown),
    }

    paths = {
        "timeline_csv": out_dir / f"{sid}-timeline.csv",
        "timeline_md": out_dir / f"{sid}-timeline.md",
        "status_csv": out_dir / f"{sid}-status-transitions.csv",
        "nozio_csv": out_dir / f"{sid}-nozio-transitions.csv",
        "unknown_csv": out_dir / f"{sid}-unknown-transactions.csv",
        "summary_json": out_dir / f"{sid}-summary.json",
    }

    _write_csv(
        paths["timeline_csv"],
        timeline,
        fieldnames=[
            "timestampUtc",
            "monotonicNs",
            "kind",
            "addressHex",
            "frameClass",
            "detail",
            "frameSequence",
            "marker",
        ],
    )
    paths["timeline_md"].write_text(_timeline_markdown(sid, timeline, summary), encoding="utf-8")
    _write_csv(
        paths["status_csv"],
        status_tx,
        fieldnames=[
            "timestampUtc",
            "addressHex",
            "frameSequence",
            "fromStatus",
            "toStatus",
            "statusCode",
        ],
    )
    _write_csv(
        paths["nozio_csv"],
        nozio_tx,
        fieldnames=[
            "timestampUtc",
            "addressHex",
            "frameSequence",
            "fromPosition",
            "toPosition",
            "logicalNozzle",
            "nozioRawHex",
        ],
    )
    _write_csv(
        paths["unknown_csv"],
        unknown,
        fieldnames=[
            "timestampUtc",
            "addressHex",
            "frameSequence",
            "transactionIdHex",
            "length",
            "dataHex",
        ],
    )
    paths["summary_json"].write_text(
        json.dumps(summary, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    return AnalysisResult(
        session_id=sid,
        summary=summary,
        timeline_rows=timeline,
        status_transitions=status_tx,
        nozio_transitions=nozio_tx,
        unknown_transactions=unknown,
        report_paths=paths,
    )


def analyze_path(
    evidence_path: Path,
    *,
    reports_dir: Path | None = None,
    session_id: str | None = None,
    direction_aware: bool = False,
) -> AnalysisResult | DirectionAwareAnalysisResult:
    session = load_session(evidence_path, session_id=session_id)
    if direction_aware:
        return analyze_session_direction_aware(session, reports_dir=reports_dir)
    return analyze_session(session, reports_dir=reports_dir)


def analyze_session_direction_aware(
    session: LoadedSession,
    *,
    reports_dir: Path | None = None,
    windows: tuple[tuple[int, int], ...] = DIRECTION_AWARE_WINDOWS,
) -> DirectionAwareAnalysisResult:
    """Direction-aware offline analysis (additive; does not replace legacy reports)."""
    out_dir = ensure_dir(reports_dir or DEFAULT_REPORTS_DIR)
    sid = session.session_id
    # Prefer complete frames; include incomplete only in diagnostics via rejected addrs.
    directed = annotate_frames(session.complete_frames)
    status_tx = build_dc1_transitions(directed)
    nozio_tx = build_nozio_transitions(directed)
    # Diagnostics: rejected addresses from all complete frames (incl. non-state).
    rejected = build_rejected_address_diagnostics(directed)
    window_reports = {
        f"{start}-{end}": build_window_report(directed, start_seq=start, end_seq=end)
        for start, end in windows
    }
    nozio_seqs = sequence_around_nozio(directed, nozio_tx)
    cycle_cmp = compare_nozzle_cycles(directed, nozio_tx)

    legacy_dc1 = len(_status_transitions(session.complete_frames))
    summary = {
        "sessionId": sid,
        "analysisMode": "direction-aware",
        "evidenceUnmodified": True,
        "recordCounts": {
            "totalRecords": len(session.records),
            "serialChunks": len(session.chunks),
            "frameRecords": len(session.frames),
            "completeFramesAnalyzed": len(session.complete_frames),
            "markers": len(session.markers),
        },
        "dc1TransitionCount": len(status_tx),
        "dc1TransitionCountLegacyNaive": legacy_dc1,
        "nozioTransitionCount": len(nozio_tx),
        "rejectedAddressFrameCount": len(rejected),
        "stateReportAddresses": ["50", "51"],
        "nozioTransitionFrameSequences": [t.get("frameSequence") for t in nozio_tx],
        "nozzleCycleComparison": {
            "cycleCount": cycle_cmp.get("cycleCount"),
            "repeatedEPumpSequence": cycle_cmp.get("repeatedEPumpSequence"),
            "narrative": cycle_cmp.get("narrative"),
        },
        "windows": [f"{a}-{b}" for a, b in windows],
        "rules": [
            "DATA after same-address POLL (before ACK) → high-confidence PUMP_TO_CONTROLLER",
            "DATA after EOT/turnaround, not same-address POLL response → CONTROLLER_TO_PUMP",
            "ACK after DATA is from the opposite side of that DATA",
            "TRANS 01 never counted as DC1 unless high-confidence PUMP_TO_CONTROLLER",
            "TRANS 05 never CD5 unless CONTROLLER_TO_PUMP and LNG=3*n",
            "DC1/NOZIO state reports: complete + CRC valid + address 50/51 only",
            "Ordered DC3 transactions within one DATA frame are preserved",
            "Implausible addresses excluded from state reports; retained in diagnostics",
        ],
    }

    prefix = f"{sid}-direction-aware"
    paths: dict[str, Path] = {
        "summary_json": out_dir / f"{prefix}-summary.json",
        "status_csv": out_dir / f"{prefix}-status-transitions.csv",
        "nozio_csv": out_dir / f"{prefix}-nozio-transitions.csv",
        "diagnostics_json": out_dir / f"{prefix}-diagnostics-rejected-addresses.json",
        "nozio_sequences_json": out_dir / f"{prefix}-nozio-sequences.json",
        "nozzle_cycles_json": out_dir / f"{prefix}-nozzle-cycle-comparison.json",
        "nozzle_cycles_md": out_dir / f"{prefix}-nozzle-cycle-comparison.md",
    }
    for label in window_reports:
        paths[f"window_{label}"] = out_dir / f"{prefix}-window-{label}.json"

    paths["summary_json"].write_text(
        json.dumps(summary, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(
        paths["status_csv"],
        status_tx,
        fieldnames=[
            "timestampUtc",
            "addressHex",
            "frameSequence",
            "fromStatus",
            "toStatus",
            "statusCode",
            "inferredDirection",
            "confidence",
        ],
    )
    _write_csv(
        paths["nozio_csv"],
        nozio_tx,
        fieldnames=[
            "timestampUtc",
            "addressHex",
            "frameSequence",
            "transactionIndex",
            "fromPosition",
            "toPosition",
            "logicalNozzle",
            "nozioRawHex",
            "inferredDirection",
            "confidence",
        ],
    )
    paths["diagnostics_json"].write_text(
        json.dumps(
            {
                "sessionId": sid,
                "rejectedAddressFrameCount": len(rejected),
                "frames": rejected,
            },
            indent=2,
            ensure_ascii=True,
        )
        + "\n",
        encoding="utf-8",
    )
    paths["nozio_sequences_json"].write_text(
        json.dumps(nozio_seqs, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    paths["nozzle_cycles_json"].write_text(
        json.dumps(cycle_cmp, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    paths["nozzle_cycles_md"].write_text(
        str(cycle_cmp.get("narrative") or "") + "\n",
        encoding="utf-8",
    )
    for label, rows in window_reports.items():
        paths[f"window_{label}"].write_text(
            json.dumps(
                {
                    "sessionId": sid,
                    "window": label,
                    "frameCount": len(rows),
                    "frames": rows,
                },
                indent=2,
                ensure_ascii=True,
            )
            + "\n",
            encoding="utf-8",
        )

    return DirectionAwareAnalysisResult(
        session_id=sid,
        summary=summary,
        status_transitions=status_tx,
        nozio_transitions=nozio_tx,
        rejected_addresses=rejected,
        windows=window_reports,
        nozio_sequences=nozio_seqs,
        nozzle_cycle_comparison=cycle_cmp,
        report_paths=paths,
    )


def compare_sessions(
    path_a: Path,
    path_b: Path,
    *,
    reports_dir: Path | None = None,
) -> dict[str, Any]:
    """Compare two sessions at a high level (counts and transition shapes)."""
    a = analyze_session(load_session(path_a), reports_dir=reports_dir)
    b = analyze_session(load_session(path_b), reports_dir=reports_dir)
    comparison = {
        "sessionA": a.session_id,
        "sessionB": b.session_id,
        "completeFrames": {
            "a": a.summary["recordCounts"]["completeFramesAnalyzed"],
            "b": b.summary["recordCounts"]["completeFramesAnalyzed"],
            "delta": (
                b.summary["recordCounts"]["completeFramesAnalyzed"]
                - a.summary["recordCounts"]["completeFramesAnalyzed"]
            ),
        },
        "dc1Transitions": {
            "a": a.summary["dc1TransitionCount"],
            "b": b.summary["dc1TransitionCount"],
        },
        "nozioTransitions": {
            "a": a.summary["nozioTransitionCount"],
            "b": b.summary["nozioTransitionCount"],
        },
        "crcInvalid": {
            "a": a.summary["errors"].get("crcInvalidFrames", 0),
            "b": b.summary["errors"].get("crcInvalidFrames", 0),
        },
        "unknownTransactions": {
            "a": a.summary["unknownTransactionCount"],
            "b": b.summary["unknownTransactionCount"],
        },
        "addressesA": sorted(a.summary["perAddress"].keys()),
        "addressesB": sorted(b.summary["perAddress"].keys()),
    }
    out_dir = ensure_dir(reports_dir or DEFAULT_REPORTS_DIR)
    out_path = out_dir / f"compare-{a.session_id}-vs-{b.session_id}.json"
    out_path.write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
    comparison["reportPath"] = str(out_path)
    return comparison


def resolve_evidence_path(
    session_id: str,
    *,
    evidence_dir: Path | None = None,
    evidence_file: Path | None = None,
) -> Path:
    if evidence_file is not None:
        return evidence_file
    return (evidence_dir or DEFAULT_EVIDENCE_DIR) / f"{session_id}.jsonl"


def _build_timeline(session: LoadedSession) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for fr in session.complete_frames:
        detail_parts = [fr.get("frameClass", "")]
        for tx in _iter_transactions(fr):
            label = tx.get("label") or tx.get("transactionType") or "tx"
            decoded = tx.get("decoded") or {}
            if decoded.get("statusLabel"):
                detail_parts.append(f"DC1={decoded['statusLabel']}")
            elif decoded.get("nozzlePosition"):
                detail_parts.append(
                    f"NOZIO={decoded['nozzlePosition']} n={decoded.get('logicalNozzle')}"
                )
            elif label:
                detail_parts.append(str(label))
        rows.append(
            {
                "timestampUtc": fr.get("firstByteTimestampUtc"),
                "monotonicNs": fr.get("firstByteMonotonicNs"),
                "kind": "frame",
                "addressHex": fr.get("addressHex"),
                "frameClass": fr.get("frameClass"),
                "detail": "; ".join(p for p in detail_parts if p),
                "frameSequence": fr.get("frameSequence"),
                "marker": "",
            }
        )
    for m in session.markers:
        rows.append(
            {
                "timestampUtc": m.get("captureTimestampUtc"),
                "monotonicNs": m.get("monotonicTimestampNs"),
                "kind": "marker",
                "addressHex": "",
                "frameClass": "",
                "detail": m.get("note") or "",
                "frameSequence": "",
                "marker": m.get("marker"),
            }
        )
    rows.sort(key=lambda r: (r.get("monotonicNs") or 0, str(r.get("timestampUtc") or "")))
    return rows


def _status_transitions(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    last: dict[str, str] = {}
    out: list[dict[str, Any]] = []
    for fr in frames:
        addr = str(fr.get("addressHex") or "?")
        for tx in _iter_transactions(fr):
            decoded = tx.get("decoded") or {}
            if decoded.get("kind") != "DC1":
                continue
            label = str(decoded.get("statusLabel") or "")
            code = decoded.get("statusCode")
            prev = last.get(addr)
            if prev is not None and prev != label:
                out.append(
                    {
                        "timestampUtc": fr.get("firstByteTimestampUtc"),
                        "addressHex": addr,
                        "frameSequence": fr.get("frameSequence"),
                        "fromStatus": prev,
                        "toStatus": label,
                        "statusCode": code,
                    }
                )
            last[addr] = label
    return out


def _nozio_transitions(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    last: dict[str, str] = {}
    out: list[dict[str, Any]] = []
    for fr in frames:
        addr = str(fr.get("addressHex") or "?")
        for tx in _iter_transactions(fr):
            decoded = tx.get("decoded") or {}
            if decoded.get("kind") != "DC3":
                continue
            pos = str(decoded.get("nozzlePosition") or "")
            prev = last.get(addr)
            if prev is not None and prev != pos:
                out.append(
                    {
                        "timestampUtc": fr.get("firstByteTimestampUtc"),
                        "addressHex": addr,
                        "frameSequence": fr.get("frameSequence"),
                        "fromPosition": prev,
                        "toPosition": pos,
                        "logicalNozzle": decoded.get("logicalNozzle"),
                        "nozioRawHex": decoded.get("nozioRawHex"),
                    }
                )
            last[addr] = pos
    return out


def _unknown_transactions(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for fr in frames:
        for tx in _iter_transactions(fr):
            if not tx.get("unknown") and tx.get("label") not in {"UNKNOWN", "TRAILING_BYTES"}:
                continue
            tid = tx.get("transactionId")
            out.append(
                {
                    "timestampUtc": fr.get("firstByteTimestampUtc"),
                    "addressHex": fr.get("addressHex"),
                    "frameSequence": fr.get("frameSequence"),
                    "transactionIdHex": (
                        f"{int(tid):02X}" if isinstance(tid, int) else tx.get("transactionIdHex")
                    ),
                    "length": tx.get("length"),
                    "dataHex": tx.get("dataHex"),
                }
            )
    return out


def _per_address_summary(frames: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "frameCount": 0,
            "pollCount": 0,
            "shortAckCount": 0,
            "dataCount": 0,
            "crcInvalid": 0,
            "lastDc1": None,
            "lastNozio": None,
        }
    )
    for fr in frames:
        addr = str(fr.get("addressHex") or "?")
        s = summary[addr]
        s["frameCount"] += 1
        cls = fr.get("frameClass")
        if cls == "POLL":
            s["pollCount"] += 1
        elif cls == "SHORT_ACK":
            s["shortAckCount"] += 1
        elif cls in {"DATA", "DATA_CRC_INVALID"}:
            s["dataCount"] += 1
        if fr.get("crcValid") is False:
            s["crcInvalid"] += 1
        for tx in _iter_transactions(fr):
            decoded = tx.get("decoded") or {}
            if decoded.get("kind") == "DC1":
                s["lastDc1"] = decoded.get("statusLabel")
            if decoded.get("kind") == "DC3":
                s["lastNozio"] = decoded.get("nozzlePosition")
    return dict(summary)


def _price_changes(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    last: dict[str, str] = {}
    out: list[dict[str, Any]] = []
    for fr in frames:
        addr = str(fr.get("addressHex") or "?")
        for tx in _iter_transactions(fr):
            decoded = tx.get("decoded") or {}
            if decoded.get("kind") != "DC3":
                continue
            price_hex = str(decoded.get("priceBcdHex") or "")
            prev = last.get(addr)
            if prev is not None and prev != price_hex and price_hex:
                out.append(
                    {
                        "timestampUtc": fr.get("firstByteTimestampUtc"),
                        "addressHex": addr,
                        "fromPriceBcdHex": prev,
                        "toPriceBcdHex": price_hex,
                        "frameSequence": fr.get("frameSequence"),
                    }
                )
            if price_hex:
                last[addr] = price_hex
    return out


def _command_response_timing(frames: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Correlate POLL → next non-POLL response per address (shape-based timing)."""
    pending_poll: dict[str, dict[str, Any]] = {}
    out: list[dict[str, Any]] = []
    for fr in frames:
        addr = str(fr.get("addressHex") or "?")
        cls = fr.get("frameClass")
        mono = fr.get("firstByteMonotonicNs")
        if cls == "POLL":
            pending_poll[addr] = fr
            continue
        poll = pending_poll.pop(addr, None)
        if poll is None or mono is None:
            continue
        poll_mono = poll.get("firstByteMonotonicNs")
        if poll_mono is None:
            continue
        latency_ns = int(mono) - int(poll_mono)
        out.append(
            {
                "addressHex": addr,
                "pollFrameSequence": poll.get("frameSequence"),
                "responseFrameSequence": fr.get("frameSequence"),
                "responseClass": cls,
                "latencyNs": latency_ns,
                "latencyMs": round(latency_ns / 1_000_000, 3),
            }
        )
    return out


def _startup_sequence(session: LoadedSession) -> list[dict[str, Any]]:
    """First N timeline events after STARTUP marker (or session start)."""
    events: list[dict[str, Any]] = []
    for fr in session.complete_frames[:40]:
        events.append(
            {
                "timestampUtc": fr.get("firstByteTimestampUtc"),
                "addressHex": fr.get("addressHex"),
                "frameClass": fr.get("frameClass"),
                "controlHex": fr.get("controlHex"),
                "frameSequence": fr.get("frameSequence"),
            }
        )
    return events


def _marker_latencies(
    session: LoadedSession,
    status_tx: list[dict[str, Any]],
    nozio_tx: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Latency from operator marker to first matching observed change."""
    matching = {
        "NOZZLE_LIFTED": ("nozio", "OUT"),
        "NOZZLE_RETURNED": ("nozio", "IN"),
        "RESET_OBSERVED": ("dc1", "RESET"),
        "AUTHORIZED_OBSERVED": ("dc1", "AUTHORIZED"),
        "FILLING_OBSERVED": ("dc1", "FILLING"),
        "TRANSACTION_COMPLETED": ("dc1", "FILLING_COMPLETED"),
    }
    out: list[dict[str, Any]] = []
    for m in session.markers:
        name = m.get("marker")
        rule = matching.get(str(name))
        if not rule:
            continue
        kind, target = rule
        m_mono = m.get("monotonicTimestampNs")
        if m_mono is None:
            continue
        candidates = nozio_tx if kind == "nozio" else status_tx
        hit = None
        for row in candidates:
            # Approximate: find transition *to* target after marker time via frame seq lookup
            to_key = "toPosition" if kind == "nozio" else "toStatus"
            if row.get(to_key) != target:
                continue
            # Resolve frame monotonic from complete frames
            seq = row.get("frameSequence")
            fr = next(
                (f for f in session.complete_frames if f.get("frameSequence") == seq),
                None,
            )
            if fr is None:
                continue
            fr_mono = fr.get("firstByteMonotonicNs")
            if fr_mono is None or int(fr_mono) < int(m_mono):
                continue
            hit = fr
            break
        if hit is None:
            out.append(
                {
                    "marker": name,
                    "target": target,
                    "matched": False,
                    "latencyMs": None,
                }
            )
        else:
            latency_ns = int(hit["firstByteMonotonicNs"]) - int(m_mono)
            out.append(
                {
                    "marker": name,
                    "target": target,
                    "matched": True,
                    "latencyMs": round(latency_ns / 1_000_000, 3),
                    "frameSequence": hit.get("frameSequence"),
                }
            )
    return out


def _error_summary(session: LoadedSession) -> dict[str, Any]:
    crc_invalid = sum(1 for f in session.frames if f.get("crcValid") is False)
    incomplete = sum(1 for f in session.frames if f.get("complete") is not True)
    warnings = 0
    for f in session.frames:
        warnings += len(f.get("parseWarnings") or [])
    return {
        "crcInvalidFrames": crc_invalid,
        "incompleteFrames": incomplete,
        "parseWarningCount": warnings,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]], *, fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _timeline_markdown(session_id: str, rows: list[dict[str, Any]], summary: dict[str, Any]) -> str:
    lines = [
        f"# Passive DART capture timeline — `{session_id}`",
        "",
        f"- Complete frames analyzed: {summary['recordCounts']['completeFramesAnalyzed']}",
        f"- DC1 transitions: {summary['dc1TransitionCount']}",
        f"- NOZIO transitions: {summary['nozioTransitionCount']}",
        f"- CRC invalid frames: {summary['errors'].get('crcInvalidFrames', 0)}",
        "",
        "| Timestamp UTC | Kind | Addr | Class/Marker | Detail |",
        "|---|---|---|---|---|",
    ]
    for r in rows:
        marker_or_class = r.get("marker") or r.get("frameClass") or ""
        lines.append(
            "| {ts} | {kind} | {addr} | {cls} | {detail} |".format(
                ts=r.get("timestampUtc") or "",
                kind=r.get("kind") or "",
                addr=r.get("addressHex") or "",
                cls=marker_or_class,
                detail=str(r.get("detail") or "").replace("|", "/"),
            )
        )
    lines.append("")
    return "\n".join(lines)
