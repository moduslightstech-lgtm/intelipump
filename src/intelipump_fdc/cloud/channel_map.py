"""Configuration-driven mapping from DART controller address to cloud identifiers.

Decoder output stays address-based. This layer assigns canonical pumpId / nozzleId
for MQTT payloads without changing serial protocol interpretation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
        nozzle = str(spec.get("nozzle_id") or spec.get("nozzleId") or base.nozzle_id)
        pump = str(spec.get("pump_id") or spec.get("pumpId") or base.pump_id)
        source = str(
            spec.get("source_identifier")
            or spec.get("sourceIdentifier")
            or spec.get("source")
            or f"pump-{addr}"
        )
        side = spec.get("side_id") or spec.get("sideId")
        out[addr] = ChannelMapping(
            address=addr,
            pump_id=pump,
            nozzle_id=nozzle,
            side_id=str(side) if side else None,
            product=(str(spec["product"]) if spec.get("product") else None),
            source_identifier=source,
        )
    return out


def load_channel_map_file(path: str | Path, addresses: tuple[int, ...]) -> dict[int, ChannelMapping]:
    text = Path(path).read_text(encoding="utf-8")
    return parse_channel_map(text, addresses)


def mappings_from_settings(settings: Any, addresses: tuple[int, ...]) -> dict[int, ChannelMapping]:
    """Load INTELIPUMP_CHANNEL_MAP / CHANNEL_MAP_PATH. Default stays pump-{addr}."""
    path = getattr(settings, "channel_map_path", None)
    raw = getattr(settings, "channel_map", None)
    if path:
        return load_channel_map_file(path, addresses)
    return parse_channel_map(raw, addresses)


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
    source = str(
        out.get("sourceIdentifier")
        or out.get("source_identifier")
        or out.get("pump_id")
        or out.get("pumpId")
        or ""
    ).strip()
    mapping = by_source.get(source) if source else None
    if mapping is None:
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
    return out


US_LAB_CHANNEL_MAP = {
    "1": {"pump_id": "pump-1", "nozzle_id": "nozzle-1", "source_identifier": "pump-1"},
    "2": {"pump_id": "pump-1", "nozzle_id": "nozzle-2", "source_identifier": "pump-2"},
}
