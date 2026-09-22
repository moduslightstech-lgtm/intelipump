"""Which logical pumpIds a Pi may apply for cloud SET_PRICE."""

from __future__ import annotations

import json
import os
import re
from typing import Any


def owned_logical_pump_ids(*, device_id: str = "") -> set[str] | None:
    """Return locked logical pump ids for one-Pi-per-pump, or None for lab multi-pump.

    Priority:
    1. ``INTELIPUMP_OWNED_PUMP_IDS`` / ``INTELIPUMP_LOGICAL_PUMP_ID`` (comma list)
    2. device id patterns: ``…-pi-00N``, ``intelipump-N``, ``…pump-N``
    3. ``INTELIPUMP_PRODUCT=AGO`` → pump-8 (SAO diesel default when id is ambiguous)

    When this returns a set, SET_PRICE for any other pumpId must be ignored —
    including when local SQLite still lists every station pump (common after
    cloning a PMS Pi image onto AGO).
    """
    env = (
        os.environ.get("INTELIPUMP_OWNED_PUMP_IDS")
        or os.environ.get("INTELIPUMP_LOGICAL_PUMP_ID")
        or ""
    ).strip()
    if env:
        return {p.strip() for p in re.split(r"[,;\s]+", env) if p.strip()}

    text = (device_id or os.environ.get("INTELIPUMP_CONTROLLER__DEVICE_ID") or "").strip()
    for pattern in (
        r"pi-0*(\d+)\s*$",
        r"intelipump-0*(\d+)\s*$",
        r"(?:^|[^a-z])pump-?0*(\d+)\s*$",
    ):
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            n = int(m.group(1))
            return {f"pump-{n}", f"pump-{n:02d}", f"pump-{n:03d}"}

    product = (os.environ.get("INTELIPUMP_PRODUCT") or "").strip().upper()
    if product in {"AGO", "DIESEL"}:
        return {"pump-8", "pump-08", "pump-008"}

    return None


def local_set_price_pump_ids(*, device_id: str, pumps: list[Any]) -> set[str]:
    """Pump ids this Pi is allowed to queue for CD5."""
    ids: set[str] = set()
    owned_logicals = owned_logical_pump_ids(device_id=device_id)
    if owned_logicals is not None:
        ids |= set(owned_logicals)

    def _is_owned_logical(logical: str) -> bool:
        if owned_logicals is None:
            return True
        return logical in owned_logicals

    for p in pumps:
        logical = str(getattr(p, "logical_pump_id", None) or "").strip()
        if owned_logicals is not None:
            if not logical or not _is_owned_logical(logical):
                continue
        if logical:
            ids.add(logical)
        pid = getattr(p, "id", None)
        if pid:
            ids.add(str(pid).strip())
        # Never accept bare dart addresses for one-Pi-per-pump — All-PMS uses
        # pump-N ids; matching "1"/"2" would be unsafe if a bad publisher sent them.
        if owned_logicals is None:
            addr = getattr(p, "dart_address", None)
            if addr is not None:
                ids.add(str(addr).strip())

    map_path = (
        os.environ.get("INTELIPUMP_CHANNEL_MAP_PATH")
        or os.environ.get("INTELIPUMP_CHANNEL_MAP")
        or os.environ.get("CHANNEL_MAP_PATH")
        or ""
    ).strip()
    if map_path:
        try:
            from pathlib import Path as _P

            raw = json.loads(_P(map_path).read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for spec in raw.values():
                    if not isinstance(spec, dict):
                        continue
                    for key in ("pump_id", "pumpId"):
                        val = str(spec.get(key) or "").strip()
                        if not val:
                            continue
                        if not _is_owned_logical(val):
                            continue
                        ids.add(val)
        except (OSError, ValueError, TypeError):
            pass
    return {i for i in ids if i}


def pump_id_allowed_for_device(*, pump_id: str, device_id: str = "") -> bool:
    """True if this Pi may apply SET_PRICE for ``pump_id``."""
    wanted = (pump_id or "").strip()
    if not wanted:
        return False
    owned = owned_logical_pump_ids(device_id=device_id)
    if owned is None:
        return True
    if wanted in owned:
        return True
    # Allow uuid-looking ids only when ownership is unlocked (lab).
    return False
