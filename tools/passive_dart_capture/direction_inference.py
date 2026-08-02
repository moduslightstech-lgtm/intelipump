"""Bus-sequence direction inference for offline passive DART analysis.

Pure offline module: reads assembled frame dicts, never transmits, and does
not import active-controller builders. Direction is inferred from POLL / EOT /
DATA / ACK ordering on the merged bus — never treated as measured PHY direction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from intelipump_fdc.protocol.dart.application.constants import PumpControlCommand
from intelipump_fdc.protocol.dart.line.addressing import is_legacy_igem_wire_address
from tools.passive_dart_capture.dart_parser import DC1_STATUS_LABELS

# State-machine reports only include captured legacy iGEM wire addresses.
STATE_REPORT_ADDRESSES: frozenset[str] = frozenset({"50", "51"})

DIRECTION_PUMP_TO_CONTROLLER = "PUMP_TO_CONTROLLER"
DIRECTION_CONTROLLER_TO_PUMP = "CONTROLLER_TO_PUMP"
DIRECTION_UNKNOWN = "UNKNOWN"

CONFIDENCE_HIGH = "HIGH"
CONFIDENCE_MEDIUM = "MEDIUM"
CONFIDENCE_LOW = "LOW"
CONFIDENCE_NONE = "NONE"

_EOT_CLASSES = frozenset({"SHORT_CONTROL_EOT", "SHORT_CONTROL_70"})
_DATA_CLASSES = frozenset({"DATA", "DATA_CRC_INVALID"})


@dataclass(slots=True)
class DirectedFrame:
    """One complete frame with bus-sequence direction annotation."""

    frame_sequence: int
    timestamp_utc: str | None
    monotonic_ns: int | None
    raw_hex: str | None
    address_hex: str
    control_hex: str | None
    frame_class: str
    crc_valid: bool | None
    complete: bool
    direction: str
    confidence: str
    inference_reason: str
    correlated_poll_sequence: int | None = None
    correlated_data_sequence: int | None = None
    correlated_ack_sequence: int | None = None
    transactions: list[dict[str, Any]] = field(default_factory=list)
    source_frame: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_report_dict(self) -> dict[str, Any]:
        return {
            "frameSequence": self.frame_sequence,
            "timestampUtc": self.timestamp_utc,
            "monotonicNs": self.monotonic_ns,
            "rawHex": self.raw_hex,
            "addressHex": self.address_hex,
            "controlHex": self.control_hex,
            "frameClass": self.frame_class,
            "crcValid": self.crc_valid,
            "complete": self.complete,
            "inferredDirection": self.direction,
            "confidence": self.confidence,
            "inferenceReason": self.inference_reason,
            "correlatedPollSequence": self.correlated_poll_sequence,
            "correlatedDataSequence": self.correlated_data_sequence,
            "correlatedAckSequence": self.correlated_ack_sequence,
            "transactions": self.transactions,
            "decodedMeaning": _summarize_meaning(self.transactions),
        }


def annotate_frames(frames: list[dict[str, Any]]) -> list[DirectedFrame]:
    """Infer direction for each frame from bus sequence context.

    Rules:
    1. DATA immediately after same-address POLL (before ACK) → high-confidence
       PUMP_TO_CONTROLLER.
    2. DATA after EOT/turnaround and not a same-address POLL response →
       candidate CONTROLLER_TO_PUMP (high when EOT precedes).
    3. SHORT_ACK after DATA is from the opposite side of that DATA.
    4. UNKNOWN whenever sequence context is insufficient.
    """
    ordered = sorted(frames, key=_frame_sort_key)
    last_global: dict[str, Any] | None = None
    # addr -> (direction of pending DATA awaiting ACK, data frameSequence)
    pending_data: dict[str, tuple[str, int]] = {}
    # Forward-fill ACK correlation in a second pass via pending map.
    directed: list[DirectedFrame] = []
    # Track last POLL seq per address for correlation on pump DATA.
    last_poll_seq: dict[str, int] = {}
    # data_seq -> index in directed for back-filling ACK correlation
    data_index_by_seq: dict[int, int] = {}

    for fr in ordered:
        addr = str(fr.get("addressHex") or "?")
        cls = str(fr.get("frameClass") or "")
        seq = int(fr.get("frameSequence", -1))
        direction = DIRECTION_UNKNOWN
        confidence = CONFIDENCE_NONE
        reason = "insufficient sequence context"
        corr_poll: int | None = None
        corr_data: int | None = None
        corr_ack: int | None = None

        if cls == "POLL":
            direction = DIRECTION_CONTROLLER_TO_PUMP
            confidence = CONFIDENCE_HIGH
            reason = "POLL is controller-originated"
            last_poll_seq[addr] = seq
        elif cls in _EOT_CLASSES:
            direction = DIRECTION_PUMP_TO_CONTROLLER
            confidence = CONFIDENCE_MEDIUM
            reason = "EOT/turnaround short control (pump or line turnaround)"
        elif cls == "SHORT_ACK":
            pending = pending_data.pop(addr, None)
            if pending is not None:
                data_dir, data_seq = pending
                corr_data = data_seq
                if data_dir == DIRECTION_PUMP_TO_CONTROLLER:
                    direction = DIRECTION_CONTROLLER_TO_PUMP
                    confidence = CONFIDENCE_HIGH
                    reason = "ACK after pump DATA; controller acknowledges"
                elif data_dir == DIRECTION_CONTROLLER_TO_PUMP:
                    direction = DIRECTION_PUMP_TO_CONTROLLER
                    confidence = CONFIDENCE_HIGH
                    reason = "ACK after controller DATA; pump acknowledges"
                else:
                    direction = DIRECTION_UNKNOWN
                    confidence = CONFIDENCE_LOW
                    reason = "ACK after DATA with unknown direction"
                # Back-fill ACK seq onto the DATA frame.
                idx = data_index_by_seq.get(data_seq)
                if idx is not None:
                    directed[idx].correlated_ack_sequence = seq
            else:
                reason = "ACK without correlated preceding DATA on this address"
                confidence = CONFIDENCE_LOW
        elif cls in _DATA_CLASSES:
            prev = last_global
            if (
                prev is not None
                and str(prev.get("addressHex") or "") == addr
                and prev.get("frameClass") == "POLL"
            ):
                direction = DIRECTION_PUMP_TO_CONTROLLER
                confidence = CONFIDENCE_HIGH
                reason = (
                    "DATA immediately after same-address POLL, before ACK "
                    "(high-confidence pump response)"
                )
                corr_poll = int(prev.get("frameSequence", -1))
            elif prev is not None and prev.get("frameClass") in _EOT_CLASSES:
                direction = DIRECTION_CONTROLLER_TO_PUMP
                confidence = CONFIDENCE_HIGH
                reason = (
                    "DATA after EOT/turnaround and not following a same-address "
                    "POLL (candidate controller command)"
                )
                # Optional: correlate the POLL that led to the EOT if same addr.
                poll_seq = last_poll_seq.get(addr)
                if poll_seq is not None:
                    corr_poll = poll_seq
            else:
                direction = DIRECTION_UNKNOWN
                confidence = CONFIDENCE_LOW
                prev_cls = prev.get("frameClass") if prev is not None else None
                reason = (
                    f"DATA after {prev_cls!r}; sequence context insufficient "
                    "for high-confidence direction"
                )
            pending_data[addr] = (direction, seq)
            data_index_by_seq[seq] = len(directed)
        else:
            reason = f"frame class {cls!r} has no direction rule"

        txs = reclassify_transactions(
            list(fr.get("transactions") or []),
            direction=direction,
            confidence=confidence,
        )
        directed.append(
            DirectedFrame(
                frame_sequence=seq,
                timestamp_utc=fr.get("firstByteTimestampUtc"),
                monotonic_ns=fr.get("firstByteMonotonicNs"),
                raw_hex=fr.get("rawHex"),
                address_hex=addr,
                control_hex=fr.get("controlHex"),
                frame_class=cls,
                crc_valid=fr.get("crcValid"),
                complete=bool(fr.get("complete")),
                direction=direction,
                confidence=confidence,
                inference_reason=reason,
                correlated_poll_sequence=corr_poll,
                correlated_data_sequence=corr_data,
                correlated_ack_sequence=corr_ack,
                transactions=txs,
                source_frame=fr,
            )
        )
        last_global = fr

    return directed


def reclassify_transactions(
    transactions: list[dict[str, Any]],
    *,
    direction: str,
    confidence: str,
) -> list[dict[str, Any]]:
    """Direction-aware reinterpretation of ambiguous TRANS encodings.

    - TRANS 01 / LNG 1 → DC1 only when PUMP_TO_CONTROLLER + HIGH confidence.
    - TRANS 01 / LNG 1 → CD1 when CONTROLLER_TO_PUMP (documented DCC).
    - TRANS 05 → CD5 only when CONTROLLER_TO_PUMP and LNG matches 3*n (≥3).
    - TRANS 03 / LNG 4 → DC3 when PUMP_TO_CONTROLLER; CD3 candidate otherwise.
    - Preserve UNKNOWN when direction/confidence is insufficient.
    """
    out: list[dict[str, Any]] = []
    for tx in transactions:
        out.append(_reclassify_one(tx, direction=direction, confidence=confidence))
    return out


def _reclassify_one(
    tx: dict[str, Any],
    *,
    direction: str,
    confidence: str,
) -> dict[str, Any]:
    tid = tx.get("transactionId")
    lng = tx.get("length")
    data_hex = str(tx.get("dataHex") or "")
    base = dict(tx)
    decoded = dict(tx.get("decoded") or {}) if isinstance(tx.get("decoded"), dict) else {}

    if tid == 0x01 and lng == 1:
        code = _first_data_byte(data_hex, decoded.get("statusCode"))
        if (
            direction == DIRECTION_PUMP_TO_CONTROLLER
            and confidence == CONFIDENCE_HIGH
            and code is not None
        ):
            label = DC1_STATUS_LABELS.get(code)
            base["label"] = "DC1_STATUS" if label else "DC1_UNKNOWN"
            base["directionResolved"] = DIRECTION_PUMP_TO_CONTROLLER
            base["decoded"] = {
                "kind": "DC1",
                "statusCode": code,
                "statusCodeHex": f"{code:02X}",
                "statusLabel": label or f"UNKNOWN_STATUS_0x{code:02X}",
                "known": label is not None,
                "note": (
                    "Direction-resolved DC1: TRANS 01 LNG=1 on high-confidence "
                    "PUMP_TO_CONTROLLER DATA"
                ),
            }
            base["unknown"] = label is None
            return base

        if direction == DIRECTION_CONTROLLER_TO_PUMP and code is not None:
            try:
                cmd = PumpControlCommand(code)
                cmd_name = cmd.name
                known = True
            except ValueError:
                cmd_name = f"UNKNOWN_DCC_0x{code:02X}"
                known = False
            base["label"] = "CD1_COMMAND" if known else "CD1_UNKNOWN"
            base["directionResolved"] = DIRECTION_CONTROLLER_TO_PUMP
            base["decoded"] = {
                "kind": "CD1",
                "dcc": code,
                "dccHex": f"{code:02X}",
                "commandName": cmd_name,
                "known": known,
                "note": (
                    "Direction-resolved CD1: TRANS 01 LNG=1 on CONTROLLER_TO_PUMP "
                    "DATA (not DC1)"
                ),
            }
            base["unknown"] = not known
            return base

        # Insufficient direction — do not treat as DC1.
        base["label"] = "AMBIGUOUS_CD1_OR_DC1"
        base["directionResolved"] = DIRECTION_UNKNOWN
        base["decoded"] = {
            "kind": "AMBIGUOUS_CD1_OR_DC1",
            "rawCode": code,
            "rawCodeHex": f"{code:02X}" if code is not None else None,
            "dc1StatusLabelIfPump": (
                DC1_STATUS_LABELS.get(code) if code is not None else None
            ),
            "cd1CommandIfController": _cd1_name(code) if code is not None else None,
            "note": (
                "TRANS 01 LNG=1 left unresolved: direction confidence insufficient "
                "to choose DC1 vs CD1"
            ),
        }
        base["unknown"] = True
        return base

    if tid == 0x05:
        if (
            direction == DIRECTION_CONTROLLER_TO_PUMP
            and isinstance(lng, int)
            and lng >= 3
            and lng % 3 == 0
        ):
            base["label"] = "CD5_PRICE_UPDATE"
            base["directionResolved"] = DIRECTION_CONTROLLER_TO_PUMP
            body = decoded.get("body") if isinstance(decoded.get("body"), dict) else decoded
            base["decoded"] = {
                "kind": "CD5",
                "body": body,
                "note": (
                    "Direction-resolved CD5: CONTROLLER_TO_PUMP with "
                    "LNG=3*n structure"
                ),
            }
            return base
        if direction == DIRECTION_PUMP_TO_CONTROLLER and lng == 1:
            base["label"] = "DC5"
            base["directionResolved"] = DIRECTION_PUMP_TO_CONTROLLER
            base["decoded"] = {
                "kind": "DC5",
                "rawHex": data_hex,
                "note": "Direction-resolved DC5 alarm on PUMP_TO_CONTROLLER",
            }
            return base
        # Do not accept CD5 without controller direction + structure.
        if decoded.get("kind") == "CD5" or base.get("label") == "CD5_PRICE_UPDATE":
            base["label"] = "AMBIGUOUS_CD5_OR_DC5"
            base["directionResolved"] = DIRECTION_UNKNOWN
            base["decoded"] = {
                "kind": "AMBIGUOUS_CD5_OR_DC5",
                "rawHex": data_hex,
                "length": lng,
                "note": (
                    "TRANS 05 not accepted as CD5: requires CONTROLLER_TO_PUMP "
                    "and LNG=3*n"
                ),
            }
            base["unknown"] = True
        return base

    if tid == 0x03 and lng == 4:
        if direction == DIRECTION_PUMP_TO_CONTROLLER:
            # Keep existing DC3/NOZIO decode from capture parser.
            if decoded.get("kind") != "DC3":
                # Fall back to documented-mask fields if present under another shape.
                pass
            base["label"] = "DC3_PRICE_NOZIO"
            base["directionResolved"] = DIRECTION_PUMP_TO_CONTROLLER
            if decoded:
                decoded = dict(decoded)
                decoded["kind"] = "DC3"
                decoded["note"] = (
                    "Direction-resolved DC3 on PUMP_TO_CONTROLLER "
                    "(ordered transactions preserved)"
                )
                base["decoded"] = decoded
            return base
        if direction == DIRECTION_CONTROLLER_TO_PUMP:
            base["label"] = "CD3_PRESET_VOLUME"
            base["directionResolved"] = DIRECTION_CONTROLLER_TO_PUMP
            base["decoded"] = {
                "kind": "CD3",
                "dataHex": data_hex,
                "note": (
                    "Direction-resolved CD3 candidate on CONTROLLER_TO_PUMP "
                    "(TRANS 03 LNG=4); not counted as NOZIO"
                ),
            }
            return base
        base["label"] = "AMBIGUOUS_CD3_OR_DC3"
        base["directionResolved"] = DIRECTION_UNKNOWN
        base["decoded"] = {
            "kind": "AMBIGUOUS_CD3_OR_DC3",
            "dataHex": data_hex,
            "note": "TRANS 03 LNG=4 unresolved without direction confidence",
        }
        base["unknown"] = True
        return base

    # Non-ambiguous types: attach direction tag only.
    if decoded:
        decoded = dict(decoded)
        decoded.setdefault("directionResolved", direction)
        base["decoded"] = decoded
    base["directionResolved"] = direction
    return base


def build_dc1_transitions(directed: list[DirectedFrame]) -> list[dict[str, Any]]:
    """DC1 state changes: complete + CRC valid + addr 50/51 + high-conf pump DATA."""
    last: dict[str, str] = {}
    out: list[dict[str, Any]] = []
    for fr in directed:
        if not _eligible_state_data(fr):
            continue
        if fr.direction != DIRECTION_PUMP_TO_CONTROLLER or fr.confidence != CONFIDENCE_HIGH:
            continue
        for tx in fr.transactions:
            decoded = tx.get("decoded") or {}
            if decoded.get("kind") != "DC1":
                continue
            label = str(decoded.get("statusLabel") or "")
            code = decoded.get("statusCode")
            prev = last.get(fr.address_hex)
            if prev is not None and prev != label:
                out.append(
                    {
                        "timestampUtc": fr.timestamp_utc,
                        "addressHex": fr.address_hex,
                        "frameSequence": fr.frame_sequence,
                        "fromStatus": prev,
                        "toStatus": label,
                        "statusCode": code,
                        "inferredDirection": fr.direction,
                        "confidence": fr.confidence,
                    }
                )
            last[fr.address_hex] = label
    return out


def build_nozio_transitions(directed: list[DirectedFrame]) -> list[dict[str, Any]]:
    """NOZIO changes from ordered DC3 transactions (multiple DC3 per frame kept)."""
    last: dict[str, str] = {}
    out: list[dict[str, Any]] = []
    for fr in directed:
        if not _eligible_state_data(fr):
            continue
        if fr.direction != DIRECTION_PUMP_TO_CONTROLLER or fr.confidence != CONFIDENCE_HIGH:
            continue
        for tx_index, tx in enumerate(fr.transactions):
            decoded = tx.get("decoded") or {}
            if decoded.get("kind") != "DC3":
                continue
            pos = str(decoded.get("nozzlePosition") or "")
            prev = last.get(fr.address_hex)
            if prev is not None and prev != pos:
                out.append(
                    {
                        "timestampUtc": fr.timestamp_utc,
                        "addressHex": fr.address_hex,
                        "frameSequence": fr.frame_sequence,
                        "transactionIndex": tx_index,
                        "fromPosition": prev,
                        "toPosition": pos,
                        "logicalNozzle": decoded.get("logicalNozzle"),
                        "nozioRawHex": decoded.get("nozioRawHex"),
                        "inferredDirection": fr.direction,
                        "confidence": fr.confidence,
                    }
                )
            last[fr.address_hex] = pos
    return out


def build_rejected_address_diagnostics(
    directed: list[DirectedFrame],
) -> list[dict[str, Any]]:
    """Frames with implausible/recovery-artifact addresses (excluded from state reports)."""
    rows: list[dict[str, Any]] = []
    for fr in directed:
        if fr.address_hex in STATE_REPORT_ADDRESSES:
            continue
        try:
            wire = int(fr.address_hex, 16)
            legacy_ok = is_legacy_igem_wire_address(wire)
        except ValueError:
            legacy_ok = False
        rows.append(
            {
                "timestampUtc": fr.timestamp_utc,
                "frameSequence": fr.frame_sequence,
                "addressHex": fr.address_hex,
                "frameClass": fr.frame_class,
                "controlHex": fr.control_hex,
                "rawHex": fr.raw_hex,
                "crcValid": fr.crc_valid,
                "legacyIgemAddress": legacy_ok,
                "reason": (
                    "address excluded from state-machine reports "
                    "(not legacy iGEM 0x50/0x51; likely frame-recovery artifact)"
                ),
                "inferredDirection": fr.direction,
                "confidence": fr.confidence,
                "decodedMeaning": _summarize_meaning(fr.transactions),
            }
        )
    return rows


def build_window_report(
    directed: list[DirectedFrame],
    *,
    start_seq: int,
    end_seq: int,
) -> list[dict[str, Any]]:
    """Focused direction-aware window for inclusive frameSequence range."""
    return [
        fr.to_report_dict()
        for fr in directed
        if start_seq <= fr.frame_sequence <= end_seq
    ]


def sequence_around_nozio(
    directed: list[DirectedFrame],
    nozio_transitions: list[dict[str, Any]],
    *,
    before: int = 8,
    after: int = 8,
) -> list[dict[str, Any]]:
    """Passive description of frames immediately before/after each NOZIO transition."""
    by_seq = {fr.frame_sequence: fr for fr in directed}
    seqs = [fr.frame_sequence for fr in directed]
    out: list[dict[str, Any]] = []
    for tr in nozio_transitions:
        center = int(tr["frameSequence"])
        try:
            idx = seqs.index(center)
        except ValueError:
            continue
        before_frames = seqs[max(0, idx - before) : idx]
        after_frames = seqs[idx + 1 : idx + 1 + after]
        out.append(
            {
                "nozioTransition": tr,
                "before": [_passive_brief(by_seq[s]) for s in before_frames],
                "at": _passive_brief(by_seq[center]),
                "after": [_passive_brief(by_seq[s]) for s in after_frames],
            }
        )
    return out


def compare_nozzle_cycles(
    directed: list[DirectedFrame],
    nozio_transitions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compare OUT/IN nozzle cycles and extract repeated passive ePump pattern.

    Passive description only — no replay bytes, no active-controller commands.
    """
    addr50 = [t for t in nozio_transitions if t.get("addressHex") == "50"]
    # Pair OUT then IN as a cycle.
    cycles: list[dict[str, Any]] = []
    i = 0
    while i < len(addr50):
        lift = addr50[i]
        if lift.get("toPosition") == "OUT" and i + 1 < len(addr50):
            ret = addr50[i + 1]
            if ret.get("toPosition") == "IN":
                lift_seq = int(lift["frameSequence"])
                return_seq = int(ret["frameSequence"])
                peri_lift = _cycle_passive_steps(
                    directed,
                    lift_seq - 10,
                    lift_seq + 40,
                    highlight_seq=lift_seq,
                    highlight_label=(
                        "PUMP_TO_CONTROLLER: DC3 NOZIO OUT (lift)"
                    ),
                )
                peri_return = _cycle_passive_steps(
                    directed,
                    return_seq - 10,
                    return_seq + 40,
                    highlight_seq=return_seq,
                    highlight_label=(
                        "PUMP_TO_CONTROLLER: DC3 NOZIO IN (return)"
                    ),
                )
                mid_fill = _summarize_mid_fill(directed, lift_seq + 40, return_seq - 10)
                cycles.append(
                    {
                        "liftFrameSequence": lift_seq,
                        "returnFrameSequence": return_seq,
                        "lift": lift,
                        "return": ret,
                        "periLiftSteps": peri_lift,
                        "periReturnSteps": peri_return,
                        "midFillSummary": mid_fill,
                        # Keep full span for evidence; pattern uses peri windows.
                        "passiveSteps": [*peri_lift, mid_fill, *peri_return],
                    }
                )
                i += 2
                continue
        i += 1

    repeated = _extract_repeated_skeleton(cycles)
    return {
        "cycleCount": len(cycles),
        "cycles": cycles,
        "repeatedEPumpSequence": repeated,
        "narrative": _cycle_narrative(cycles, repeated),
    }


def _cycle_passive_steps(
    directed: list[DirectedFrame],
    start_seq: int,
    end_seq: int,
    *,
    highlight_seq: int | None = None,
    highlight_label: str | None = None,
) -> list[str]:
    """Compact passive labels for addr 50 in an inclusive frameSequence window."""
    steps: list[str] = []
    for fr in directed:
        if fr.address_hex != "50":
            continue
        if fr.frame_sequence < start_seq or fr.frame_sequence > end_seq:
            continue
        if fr.frame_class in {"POLL", "SHORT_ACK"}:
            continue
        if (
            highlight_seq is not None
            and highlight_label is not None
            and fr.frame_sequence == highlight_seq
        ):
            label = highlight_label
        else:
            meaning = _summarize_meaning(fr.transactions)
            if fr.frame_class in _EOT_CLASSES:
                label = "EOT/turnaround"
            elif meaning:
                label = f"{fr.direction}: {meaning}"
            else:
                label = f"{fr.frame_class}/{fr.direction}"
        if steps and steps[-1] == label:
            continue
        steps.append(label)
    return steps


def _summarize_mid_fill(
    directed: list[DirectedFrame],
    start_seq: int,
    end_seq: int,
) -> str:
    """Compress long fill-phase polling into one passive description."""
    saw_dc2 = False
    saw_authorized = False
    saw_fill_info = False
    for fr in directed:
        if fr.address_hex != "50":
            continue
        if fr.frame_sequence < start_seq or fr.frame_sequence > end_seq:
            continue
        meaning = _summarize_meaning(fr.transactions)
        if "DC2" in meaning:
            saw_dc2 = True
        if "AUTHORIZED" in meaning:
            saw_authorized = True
        if "RETURN_FILLING_INFORMATION" in meaning:
            saw_fill_info = True
    parts = ["mid-fill poll loop (passive)"]
    if saw_authorized:
        parts.append("DC1 AUTHORIZED sticky")
    if saw_fill_info:
        parts.append("CD1 RETURN_FILLING_INFORMATION")
    if saw_dc2:
        parts.append("DC2 volume/amount + DC3 OUT")
    return "; ".join(parts)


def _extract_repeated_skeleton(cycles: list[dict[str, Any]]) -> list[str]:
    if not cycles:
        return []
    if len(cycles) == 1:
        return list(cycles[0].get("passiveSteps") or [])
    lift_lcs = _lcs(cycles[0]["periLiftSteps"], cycles[1]["periLiftSteps"])
    return_lcs = _lcs(cycles[0]["periReturnSteps"], cycles[1]["periReturnSteps"])
    mid = cycles[0].get("midFillSummary") or "mid-fill poll loop (passive)"
    # Prefer shared mid-fill summary text when both cycles report the same shape.
    mid_b = cycles[1].get("midFillSummary")
    if mid_b and mid_b == mid:
        mid_step = mid
    else:
        mid_step = "mid-fill poll loop (passive; details differ by cycle)"
    return [*lift_lcs, mid_step, *return_lcs]


def _lcs(a: list[str], b: list[str]) -> list[str]:
    m, n = len(a), len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if a[i - 1] == b[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
    out: list[str] = []
    i, j = m, n
    while i > 0 and j > 0:
        if a[i - 1] == b[j - 1]:
            out.append(a[i - 1])
            i -= 1
            j -= 1
        elif dp[i - 1][j] >= dp[i][j - 1]:
            i -= 1
        else:
            j -= 1
    out.reverse()
    return out


def _cycle_narrative(cycles: list[dict[str, Any]], repeated: list[str]) -> str:
    lines = [
        "Passive ePump nozzle-cycle comparison (no replay / no active commands).",
        f"Observed cycles on address 0x50: {len(cycles)}.",
    ]
    for idx, c in enumerate(cycles, start=1):
        lines.append(
            f"Cycle {idx}: NOZIO OUT @ frame {c['liftFrameSequence']}, "
            f"NOZIO IN @ frame {c['returnFrameSequence']}."
        )
    if repeated:
        lines.append(
            "Repeated passive sequence across cycles "
            "(peri-lift / mid-fill / peri-return):"
        )
        for step in repeated:
            lines.append(f"  - {step}")
    else:
        lines.append("No shared subsequence extracted (need ≥2 cycles).")
    return "\n".join(lines)


def _eligible_state_data(fr: DirectedFrame) -> bool:
    if not fr.complete:
        return False
    if fr.frame_class != "DATA":
        return False
    if fr.crc_valid is not True:
        return False
    return fr.address_hex in STATE_REPORT_ADDRESSES


def _passive_brief(fr: DirectedFrame) -> dict[str, Any]:
    return {
        "frameSequence": fr.frame_sequence,
        "timestampUtc": fr.timestamp_utc,
        "addressHex": fr.address_hex,
        "frameClass": fr.frame_class,
        "inferredDirection": fr.direction,
        "confidence": fr.confidence,
        "decodedMeaning": _summarize_meaning(fr.transactions),
        "inferenceReason": fr.inference_reason,
    }


def _summarize_meaning(transactions: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for tx in transactions:
        decoded = tx.get("decoded") or {}
        kind = decoded.get("kind")
        if kind == "DC1":
            parts.append(f"DC1 {decoded.get('statusLabel')}")
        elif kind == "CD1":
            parts.append(f"CD1 {decoded.get('commandName')}")
        elif kind == "DC3":
            parts.append(
                f"DC3 NOZIO {decoded.get('nozioRawHex')} "
                f"({decoded.get('nozzlePosition')})"
            )
        elif kind == "CD3":
            parts.append("CD3 preset")
        elif kind == "CD5":
            parts.append("CD5 price update")
        elif kind == "DC5":
            parts.append("DC5 alarm")
        elif kind == "AMBIGUOUS_CD1_OR_DC1":
            parts.append("ambiguous CD1/DC1")
        elif tx.get("label"):
            parts.append(str(tx["label"]))
    return "; ".join(parts)


def _first_data_byte(data_hex: str, fallback: Any) -> int | None:
    if isinstance(fallback, int):
        return fallback
    parts = data_hex.split()
    if not parts:
        return None
    try:
        return int(parts[0], 16)
    except ValueError:
        return None


def _cd1_name(code: int | None) -> str | None:
    if code is None:
        return None
    try:
        return PumpControlCommand(code).name
    except ValueError:
        return None


def _frame_sort_key(fr: dict[str, Any]) -> tuple[Any, ...]:
    return (
        fr.get("firstByteMonotonicNs", 0),
        fr.get("frameSequence", 0),
        fr.get("firstByteTimestampUtc", ""),
    )

