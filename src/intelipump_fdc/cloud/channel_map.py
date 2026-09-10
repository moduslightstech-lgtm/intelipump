"""Configuration-driven mapping from DART controller address to cloud identifiers.

Decoder output stays address-based. This layer assigns canonical pumpId / nozzleId
for MQTT payloads without changing serial protocol interpretation.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

US_LAB_STATION_IDS = frozenset({"InteliPump-US-Lab", "US-LAB-001"})


class DuplicateChannelMappingError(ValueError):
    """Two source channels mapped to the same physical pump/nozzle."""


@dataclass(frozen=True, slots=True)
class ChannelMapping:
    address: int
    pump_id: str
    nozzle_id: str
    side_id: str | None
    product: str | None
    source_identifier: str


def default_mapping(address: int) -> ChannelMapping:
    return ChannelMapping(
        address=address,
        pump_id=f"pump-{address}",
        nozzle_id="nozzle-1",
        side_id=None,
        product=None,
        source_identifier=f"pump-{address}",
    )


def _clean(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    return text or fallback


def validate_unique_nozzle_mappings(mappings: dict[int, ChannelMapping]) -> None:
    seen: dict[tuple[str, str], int] = {}
    for addr, mapping in mappings.items():
        key = (mapping.pump_id, mapping.nozzle_id)
        prior = seen.get(key)
        if prior is not None:
            raise DuplicateChannelMappingError(
                f"DART addresses {prior} and {addr} both map to {mapping.pump_id}/{mapping.nozzle_id}"
            )
        seen[key] = addr


def parse_channel_map(
    raw: str | dict[str, Any] | None,
    addresses: tuple[int, ...],
) -> dict[int, ChannelMapping]:
    """Parse INTELIPUMP_CHANNEL_MAP JSON or a dict.

    Example US Lab::

        {"1": {"pump_id": "pump-1", "nozzle_id": "nozzle-1"},
         "2": {"pump_id": "pump-1", "nozzle_id": "nozzle-2"}}
    """
    parsed: dict[str, Any] = {}
    if isinstance(raw, dict):
        parsed = raw
    elif isinstance(raw, str) and raw.strip():
        parsed = json.loads(raw)
    out: dict[int, ChannelMapping] = {a: default_mapping(a) for a in addresses}
    for key, spec in parsed.items():
        try:
            addr = int(key)
        except (TypeError, ValueError):
            continue
        if not isinstance(spec, dict):
            continue
        base = out.get(addr, default_mapping(addr))
        nozzle = _clean(spec.get("nozzle_id") or spec.get("nozzleId"), base.nozzle_id)
        pump = _clean(spec.get("pump_id") or spec.get("pumpId"), base.pump_id)
        source = _clean(
            spec.get("source_identifier")
            or spec.get("sourceIdentifier")
            or spec.get("source"),
            f"pump-{addr}",
        )
        side = spec.get("side_id") or spec.get("sideId")
        out[addr] = ChannelMapping(
            address=addr,
            pump_id=pump,
            nozzle_id=nozzle,
            side_id=str(side).strip() if side else None,
            product=(str(spec["product"]).strip() if spec.get("product") else None),
            source_identifier=source,
        )
    validate_unique_nozzle_mappings(out)
    return out


def load_channel_map_file(path: str | Path, addresses: tuple[int, ...]) -> dict[int, ChannelMapping]:
    text = Path(path).read_text(encoding="utf-8")
    return parse_channel_map(text, addresses)


def _bundled_us_lab_map_path() -> Path | None:
    here = Path(__file__).resolve()
    candidates = [
        here.parents[3] / "config" / "channel_map.us-lab.json",
        here.parents[1] / "config" / "channel_map.us-lab.json",
        Path.cwd() / "config" / "channel_map.us-lab.json",
    ]
    for path in candidates:
        if path.is_file():
            return path
    return None


def mappings_from_settings(settings: Any, addresses: tuple[int, ...]) -> dict[int, ChannelMapping]:
    """Load INTELIPUMP_CHANNEL_MAP / CHANNEL_MAP_PATH. Default stays pump-{addr}."""
    path = getattr(settings, "channel_map_path", None)
    raw = getattr(settings, "channel_map", None)
    station = str(getattr(getattr(settings, "controller", None), "station_id", "") or "").strip()
    if path:
        path_obj = Path(str(path))
        if path_obj.is_file():
            return load_channel_map_file(path_obj, addresses)
        logger.warning(
            "channel_map_path_missing",
            path=str(path_obj),
            station_id=station,
            reason="file_not_found_continue",
        )
        # Fall through: US Lab keeps embedded map; others use default.
    if raw:
        return parse_channel_map(raw, addresses)
    if station in US_LAB_STATION_IDS:
        bundled = _bundled_us_lab_map_path()
        if bundled is not None:
            return load_channel_map_file(bundled, addresses)
        return parse_channel_map(US_LAB_CHANNEL_MAP, addresses)
    return parse_channel_map(None, addresses)


def us_lab_channel_mappings(addresses: tuple[int, ...]) -> dict[int, ChannelMapping]:
    """Canonical US Lab map: DART address 2 → physical pump-1 / nozzle-2."""
    return parse_channel_map(US_LAB_CHANNEL_MAP, addresses)


def safe_mappings_from_settings(settings: Any, addresses: tuple[int, ...]) -> dict[int, ChannelMapping]:
    """Load channel map; never fall back to default pump-2 for US Lab stations.

    The default map treats DART address 2 as a second physical pump (`pump-2` /
    `nozzle-1`). That breaks live Nozzle 2. US Lab must keep
    `pump-1` / `nozzle-2` / source `pump-2` even when the config file fails.
    """
    station = str(getattr(getattr(settings, "controller", None), "station_id", "") or "").strip()
    try:
        return mappings_from_settings(settings, addresses)
    except Exception:
        logger.exception("channel_map_load_failed", station_id=station)
        if station in US_LAB_STATION_IDS:
            logger.warning(
                "channel_map_us_lab_fallback",
                station_id=station,
                reason="load_failed_keep_us_lab_map",
            )
            return us_lab_channel_mappings(addresses)
        return parse_channel_map(None, addresses)


def index_mappings_by_source(mappings: dict[int, ChannelMapping]) -> dict[str, ChannelMapping]:
    out: dict[str, ChannelMapping] = {}
    for mapping in mappings.values():
        out[str(mapping.address)] = mapping
        out[f"pump-{mapping.address}"] = mapping
        out[mapping.source_identifier] = mapping
    return out


def enrich_transaction_payload(
    payload: dict[str, Any],
    mappings: dict[int, ChannelMapping] | dict[str, ChannelMapping],
) -> dict[str, Any]:
    """Rewrite MQTT sale fields using the address/source mapping.

    Legacy queued rows keep their original source identifiers; this layer
    assigns canonical pumpId / nozzleId at publish time.
    """
    out = dict(payload)
    if not mappings:
        return out
    first_key = next(iter(mappings.keys()))
    by_source = (
        mappings
        if not isinstance(first_key, int)
        else index_mappings_by_source(mappings)  # type: ignore[arg-type]
    )
    received_pump = str(out.get("pumpId") or out.get("pump_id") or "").strip()
    received_nozzle = out.get("nozzleId")
    if received_nozzle is None:
        received_nozzle = out.get("nozzle_id")
    source = str(
        out.get("sourceIdentifier")
        or out.get("source_identifier")
        or received_pump
        or ""
    ).strip()
    mapping = by_source.get(source) if source else None
    if mapping is None:
        if os.environ.get("INTELIPUMP_DEBUG_LIVE", "").strip() in {"1", "true", "TRUE", "yes"}:
            logger.warning(
                "channel_map_unmapped",
                source=source,
                received_pump=received_pump,
                received_nozzle=received_nozzle,
                accepted=False,
                rejection_reason="unmapped_source_channel",
            )
        return out
    hose = out.get("nozzle_id")
    if isinstance(hose, int):
        out["wayneNozzleIndex"] = hose
    out["sourceIdentifier"] = mapping.source_identifier
    out["source_identifier"] = mapping.source_identifier
    out["pumpId"] = mapping.pump_id
    out["pump_id"] = mapping.pump_id
    out["nozzleId"] = mapping.nozzle_id
    out["nozzle_id"] = mapping.nozzle_id
    if mapping.side_id:
        out["sideId"] = mapping.side_id
        out["side_id"] = mapping.side_id
    if mapping.product and not out.get("product"):
        out["product"] = mapping.product
    if os.environ.get("INTELIPUMP_DEBUG_LIVE", "").strip() in {"1", "true", "TRUE", "yes"}:
        logger.info(
            "channel_map_enrich",
            source_channel=source,
            received_pump=received_pump,
            received_nozzle=received_nozzle,
            normalized_pump=mapping.pump_id,
            normalized_nozzle=mapping.nozzle_id,
            transaction_id=out.get("transactionId") or out.get("transaction_uuid"),
            accepted=True,
        )
    return out


US_LAB_CHANNEL_MAP = {
    "1": {"pump_id": "pump-1", "nozzle_id": "nozzle-1", "source_identifier": "pump-1"},
    "2": {"pump_id": "pump-1", "nozzle_id": "nozzle-2", "source_identifier": "pump-2"},
}
