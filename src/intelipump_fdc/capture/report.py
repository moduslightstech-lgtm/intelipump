"""Validation report template for passive Wayne lab captures."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from intelipump_fdc.capture.decode_offline import OfflineDecodeResult, decode_capture_file


class PassiveConclusion(StrEnum):
    PASS = "PASSIVE_CAPTURE_PASS"
    INCONCLUSIVE = "PASSIVE_CAPTURE_INCONCLUSIVE"
    FAIL = "PASSIVE_CAPTURE_FAIL"


@dataclass(frozen=True, slots=True)
class OperatorObservations:
    hardware_setup: str = "(operator to fill)"
    adapter_model: str = "(operator to fill)"
    interface_type: str = "(operator to fill; RS-232 or RS-485 — confirm with manual)"
    baud_config: str = "9600 8O1 (assumed DART; confirm on site)"
    tx_inhibit_method: str = "(operator to fill)"
    dispenser_mode: str = "standalone/local"
    displayed_pump_address: str = "(operator to fill)"
    displayed_nozzle: str = "(operator to fill)"
    displayed_product: str = "(operator to fill)"
    displayed_unit_price: str = "(operator to fill)"
    displayed_volume: str = "(operator to fill)"
    displayed_amount: str = "(operator to fill)"
    start_time: str = "(operator to fill)"
    end_time: str = "(operator to fill)"
    dispenser_operation_changed: str = "no / yes — describe"
    safety_observations: str = "(operator to fill)"
    discrepancies: str = "(operator to fill)"
    unknown_fields: str = "(see decode notes)"


def _candidate_field_summary(result: OfflineDecodeResult) -> tuple[str, str, str]:
    """Extract uncertain candidate amount/volume/price strings from decode."""
    amounts: list[str] = []
    volumes: list[str] = []
    prices: list[str] = []
    for frame in result.candidate_frames:
        app = frame.application
        if not app:
            continue
        for tx in app.get("transactions") or []:
            fields = tx.get("decodedBody") or {}
            if not isinstance(fields, dict):
                continue
            if "amount" in fields:
                amounts.append(f"uncertain: {fields['amount']}")
            if "volume" in fields:
                volumes.append(f"uncertain: {fields['volume']}")
            if "unit_price" in fields or "price" in fields:
                prices.append(
                    f"uncertain: {fields.get('unit_price', fields.get('price'))}"
                )
    def _join(items: list[str]) -> str:
        if not items:
            return "none decoded (or uncertain / not present)"
        return "; ".join(items[:8])

    return _join(amounts), _join(volumes), _join(prices)


def suggest_conclusion(
    result: OfflineDecodeResult,
    *,
    operator_interference: bool | None = None,
    operator_force: PassiveConclusion | None = None,
) -> PassiveConclusion:
    """Never auto-PASS. Default INCONCLUSIVE; FAIL on clear capture faults."""
    if operator_force is not None:
        if operator_force is PassiveConclusion.PASS:
            # Hard gates for PASS even when operator requests it.
            if operator_interference:
                return PassiveConclusion.FAIL
            if result.total_rx_bytes <= 0:
                return PassiveConclusion.INCONCLUSIVE
            if result.data_frame_count + result.control_frame_count <= 0:
                return PassiveConclusion.INCONCLUSIVE
        return operator_force
    if operator_interference:
        return PassiveConclusion.FAIL
    if result.total_rx_bytes <= 0:
        return PassiveConclusion.INCONCLUSIVE
    return PassiveConclusion.INCONCLUSIVE


def render_validation_report(
    *,
    capture_path: Path,
    result: OfflineDecodeResult,
    observations: OperatorObservations | None = None,
    conclusion: PassiveConclusion | None = None,
    duration_s: float | None = None,
) -> str:
    obs = observations or OperatorObservations()
    conclusion = conclusion or suggest_conclusion(result)
    amount_s, volume_s, price_s = _candidate_field_summary(result)
    idle_notes = [
        e.get("notes")
        for e in result.serial_events
        if e.get("event") in {"serial_disconnected", "serial_reconnected", "error"}
    ]
    lines = [
        "# Passive Wayne lab validation report",
        "",
        f"**Conclusion:** `{conclusion.value}`",
        "",
        "> PASS requires: no operational interference, reliable byte capture,",
        "> reproducible frame boundaries, consistent CRC behavior, and",
        "> observed display values matched without guesswork.",
        "> This generator never marks PASS automatically.",
        "",
        "## Capture metadata",
        "",
        f"- Capture ID: `{result.capture_id or 'unknown'}`",
        f"- Capture file: `{capture_path}`",
        f"- Duration (s): {duration_s if duration_s is not None else '(operator to fill)'}",
        f"- Total RX bytes: {result.total_rx_bytes}",
        f"- Candidate frames: {len(result.candidate_frames)}",
        f"- Control frames: {result.control_frame_count}",
        f"- DATA frames: {result.data_frame_count}",
        f"- Rejected candidates: {result.rejected_count}",
        f"- CRC valid DATA: {result.crc_valid_count}",
        f"- CRC invalid DATA: {result.crc_invalid_count}",
        f"- Possible addresses (candidate): {result.possible_addresses or 'none'}",
        "",
        "## Hardware setup",
        "",
        f"- Setup notes: {obs.hardware_setup}",
        f"- Adapter model: {obs.adapter_model}",
        f"- Interface type: {obs.interface_type}",
        f"- Baud/config: {obs.baud_config}",
        f"- TX-inhibit method: {obs.tx_inhibit_method}",
        f"- Dispenser mode: {obs.dispenser_mode}",
        "",
        "## Detected idle / serial events",
        "",
    ]
    if result.serial_events:
        for ev in result.serial_events:
            lines.append(
                f"- `{ev.get('event')}` @ {ev.get('timestampUtc')} "
                f"state={ev.get('serialState')} notes={ev.get('notes')}"
            )
    else:
        lines.append("- (no serial event records)")
    if idle_notes:
        lines.append(f"- Event notes summary: {idle_notes}")
    lines.extend(
        [
            "",
            "## Candidate frames (offline)",
            "",
            "| offset | type | addr | seq | crc | certainty |",
            "|-------:|------|-----:|----:|-----|-----------|",
        ]
    )
    for fr in result.candidate_frames[:50]:
        lines.append(
            f"| {fr.offset} | {fr.control_type or 'REJECTED'} | "
            f"{fr.address if fr.address is not None else '-'} | "
            f"{fr.sequence if fr.sequence is not None else '-'} | "
            f"{fr.crc_valid} | {fr.certainty} |"
        )
    if len(result.candidate_frames) > 50:
        lines.append(f"| … | ({len(result.candidate_frames) - 50} more) | | | | |")
    lines.extend(
        [
            "",
            "## Undecoded bytes (preserved)",
            "",
            f"- Trailing hex: `{result.undecoded_trailing_hex or '(none)'}`",
            f"- Noise chunks: {len(result.undecoded_noise_hex)}",
            "",
            "## Operator-observed display values",
            "",
            f"- Pump address: {obs.displayed_pump_address}",
            f"- Nozzle: {obs.displayed_nozzle}",
            f"- Product: {obs.displayed_product}",
            f"- Unit price: {obs.displayed_unit_price}",
            f"- Volume: {obs.displayed_volume}",
            f"- Amount: {obs.displayed_amount}",
            f"- Start time: {obs.start_time}",
            f"- End time: {obs.end_time}",
            "",
            "## Decoded candidates (uncertain unless matched)",
            "",
            f"- Candidate amount: {amount_s}",
            f"- Candidate volume: {volume_s}",
            f"- Candidate unit price: {price_s}",
            "",
            "## Discrepancies / unknown fields",
            "",
            f"- Discrepancies: {obs.discrepancies}",
            f"- Unknown fields: {obs.unknown_fields}",
            "",
            "## Safety observations",
            "",
            f"- Dispenser operation changed unexpectedly: {obs.dispenser_operation_changed}",
            f"- Notes: {obs.safety_observations}",
            "",
            "## Decode warnings",
            "",
        ]
    )
    for w in result.warnings:
        lines.append(f"- {w}")
    lines.extend(["", f"**Final conclusion:** `{conclusion.value}`", ""])
    return "\n".join(lines)


def write_validation_report(
    capture_path: Path,
    report_path: Path,
    *,
    observations: OperatorObservations | None = None,
    conclusion: PassiveConclusion | None = None,
    duration_s: float | None = None,
) -> OfflineDecodeResult:
    result = decode_capture_file(capture_path)
    text = render_validation_report(
        capture_path=capture_path,
        result=result,
        observations=observations,
        conclusion=conclusion,
        duration_s=duration_s,
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(text, encoding="utf-8")
    return result
