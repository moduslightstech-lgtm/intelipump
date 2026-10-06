"""File bridge: cloud-sync queues SET_PRICE; controller applies CD5 on the bus.

Cloud-sync and the RS-485 controller are separate processes. The sidecar never
opens the serial port. A single JSON request under /var/lib/intelipump lets the
controller (sole bus master) apply CD5 when safe.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import contextlib


DEFAULT_REQUEST_NAME = "set-price-request.json"
DEFAULT_STORED_PRICE_NAME = "unit-price.json"
DEFAULT_ACK_HOLD_DIR_NAME = "set-price-ack-holds"


def request_dir() -> Path:
    return Path(os.environ.get("INTELIPUMP_SET_PRICE_REQUEST_DIR", "/var/lib/intelipump"))


def request_path() -> Path:
    return request_dir() / DEFAULT_REQUEST_NAME


def stored_price_path() -> Path:
    return request_dir() / DEFAULT_STORED_PRICE_NAME


@dataclass(frozen=True, slots=True)
class SetPriceRequest:
    correlation_id: str
    command_id: str
    unit_price_raw: int
    prices_raw: tuple[int, ...]
    requested_by: str | None = None
    requested_at: str | None = None
    pump_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "correlationId": self.correlation_id,
            "commandId": self.command_id,
            "unitPriceRaw": self.unit_price_raw,
            "pricesRaw": list(self.prices_raw),
            "requestedBy": self.requested_by,
            "requestedAt": self.requested_at or datetime.now(UTC).isoformat(),
            "pumpId": self.pump_id,
        }


@dataclass(frozen=True, slots=True)
class PersistedUnitPrice:
    unit_price_raw: int
    prices_raw: tuple[int, ...]
    source: str = "cloud"
    updated_at: str | None = None


def parse_prices_from_payload(payload: dict[str, Any]) -> tuple[int, tuple[int, ...]]:
    """Return (unit_price_raw, prices_raw tuple) from a cloud command payload."""
    prices = payload.get("pricesRaw") or payload.get("prices_raw")
    unit = payload.get("unitPriceRaw")
    if unit is None:
        unit = payload.get("unit_price_raw")
    if prices is None and unit is None:
        raise ValueError("SET_PRICE payload requires pricesRaw or unitPriceRaw")
    if prices is not None:
        if not isinstance(prices, list) or not prices:
            raise ValueError("pricesRaw must be a non-empty list of integers")
        raw_list: list[int] = []
        for item in prices:
            if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
                raise ValueError("pricesRaw entries must be positive integers")
            raw_list.append(item)
        unit_price = int(unit) if isinstance(unit, int) and not isinstance(unit, bool) else raw_list[0]
        if unit_price <= 0:
            raise ValueError("unitPriceRaw must be a positive integer")
        return unit_price, tuple(raw_list)
    if isinstance(unit, bool) or not isinstance(unit, int) or unit <= 0:
        raise ValueError("unitPriceRaw must be a positive integer")
    return unit, (unit,)


def write_set_price_request(req: SetPriceRequest) -> Path:
    """Best-effort request write (intake). Prefer durable restore helpers at finalize."""
    path = request_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(req.to_dict(), separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)
    return path


def write_set_price_request_durable(req: SetPriceRequest) -> Path:
    """Power-loss durable request write (temp → fsync → replace → fsync dir)."""
    return _atomic_write_json(request_path(), req.to_dict())


def write_persisted_unit_price(
    unit_price_raw: int,
    prices_raw: tuple[int, ...] | list[int] | None = None,
    *,
    source: str = "cloud",
) -> Path:
    """Persist last applied unit price so controller restart does not revert to --price.

    Uses the same temp → fsync → replace → fsync-dir path as outcomes so a
    crash mid-write cannot leave a half-applied price file.
    """
    if isinstance(unit_price_raw, bool) or not isinstance(unit_price_raw, int) or unit_price_raw <= 0:
        raise ValueError("unit_price_raw must be a positive integer")
    prices = tuple(prices_raw) if prices_raw else (unit_price_raw,)
    for p in prices:
        if isinstance(p, bool) or not isinstance(p, int) or p <= 0:
            raise ValueError("prices_raw entries must be positive integers")
    payload = {
        "unitPriceRaw": unit_price_raw,
        "pricesRaw": list(prices),
        "source": source,
        "updatedAt": datetime.now(UTC).isoformat(),
    }
    return _atomic_write_json(stored_price_path(), payload)


def persisted_unit_price_matches(
    unit_price_raw: int,
    prices_raw: tuple[int, ...] | list[int] | None = None,
) -> bool:
    """True when durable unit-price.json already holds this price vector."""
    existing = read_persisted_unit_price()
    if existing is None:
        return False
    prices = tuple(prices_raw) if prices_raw else (unit_price_raw,)
    return (
        existing.unit_price_raw == unit_price_raw
        and tuple(existing.prices_raw) == prices
    )


def read_persisted_unit_price() -> PersistedUnitPrice | None:
    """Load last applied unit price, if present."""
    path = stored_price_path()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        unit, prices = parse_prices_from_payload(
            {
                "unitPriceRaw": raw.get("unitPriceRaw"),
                "pricesRaw": raw.get("pricesRaw"),
            }
        )
    except ValueError:
        return None
    return PersistedUnitPrice(
        unit_price_raw=unit,
        prices_raw=prices,
        source=str(raw.get("source") or "cloud"),
        updated_at=str(raw["updatedAt"]) if raw.get("updatedAt") else None,
    )


def read_set_price_request() -> SetPriceRequest | None:
    """Read a pending request without removing it."""
    path = request_path()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        unit, prices = parse_prices_from_payload(
            {
                "unitPriceRaw": raw.get("unitPriceRaw"),
                "pricesRaw": raw.get("pricesRaw"),
            }
        )
    except ValueError:
        return None
    corr = str(raw.get("correlationId") or "").strip()
    cmd = str(raw.get("commandId") or corr or "set-price").strip()
    if not corr:
        corr = cmd
    return SetPriceRequest(
        correlation_id=corr,
        command_id=cmd,
        unit_price_raw=unit,
        prices_raw=prices,
        requested_by=str(raw["requestedBy"]) if raw.get("requestedBy") else None,
        requested_at=str(raw["requestedAt"]) if raw.get("requestedAt") else None,
        pump_id=str(raw["pumpId"]) if raw.get("pumpId") else None,
    )


def consume_set_price_request() -> SetPriceRequest | None:
    """Read and remove a pending request. Returns None if absent or invalid.

    Prefer ``consume_set_price_request_durable`` at finalization so the unlink
    is power-loss durable before cloud-sync may ACK-delete the outcome.
    """
    path = request_path()
    req = read_set_price_request()
    if req is None:
        if path.is_file():
            with contextlib.suppress(OSError):
                path.unlink()
        return None
    try:
        path.unlink()
    except OSError:
        return None
    return req


def consume_set_price_request_durable() -> SetPriceRequest | None:
    """Unlink the pending request, then fsync its directory.

    On directory sync failure the request is restored with a durable write so
    cloud-sync cannot ACK-delete the paired outcome while the unlink is not
    durable. If that restore cannot be made durable, a durable ACK-hold marker
    is retained instead (still blocking outcome deletion). Raises OSError when
    the unlink was not confirmed durable.
    """
    path = request_path()
    req = read_set_price_request()
    if req is None:
        if path.is_file():
            try:
                path.unlink()
                _fsync_dir(path)
            except OSError:
                pass
        return None
    path.unlink()
    try:
        _fsync_dir(path)
    except OSError as unlink_sync_exc:
        # Unlink may not be durable — restore request durably, or hold ACK.
        try:
            write_set_price_request_durable(req)
            clear_ack_hold(req.correlation_id)
        except (OSError, ValueError) as restore_exc:
            try:
                write_ack_hold(
                    req.correlation_id,
                    reason="request_restore_not_durable_after_unlink_sync_failure",
                )
            except (OSError, ValueError):
                # Last resort: best-effort non-durable request so a live
                # cloud-sync still sees a block; durability is not guaranteed.
                with contextlib.suppress(OSError):
                    write_set_price_request(req)
                raise OSError(
                    "set-price request unlink dir sync failed and neither "
                    "durable restore nor durable ACK-hold could be written"
                ) from restore_exc
            raise OSError(
                "set-price request unlink dir sync failed; durable restore "
                "failed — ACK-hold retained to block outcome deletion"
            ) from restore_exc
        raise OSError(
            "set-price request unlink dir sync failed; request restored durably"
        ) from unlink_sync_exc
    clear_ack_hold(req.correlation_id)
    return req


DEFAULT_OUTCOME_NAME = "set-price-outcome.json"
DEFAULT_OUTCOMES_DIR_NAME = "set-price-outcomes"


def outcome_path() -> Path:
    """Legacy single-file path (migrated into outcomes_dir on read)."""
    return request_dir() / DEFAULT_OUTCOME_NAME


def outcomes_dir() -> Path:
    return request_dir() / DEFAULT_OUTCOMES_DIR_NAME


@dataclass(frozen=True, slots=True)
class SetPriceOutcome:
    """Final CD5 apply outcome for cloud-sync to publish as COMMAND_RESULT."""

    correlation_id: str
    command_id: str
    station_id: str | None
    pump_id: str | None
    unit_price_raw: int
    execution_status: str
    accepted: bool
    applied_addresses: tuple[int, ...]
    gave_up_addresses: tuple[int, ...]
    deferred_addresses: tuple[int, ...]
    detail: str | None = None
    # LINK_ACK sent; DC3 never matched (idle readback often 0).
    unverified_addresses: tuple[int, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "correlationId": self.correlation_id,
            "commandId": self.command_id,
            "stationId": self.station_id,
            "pumpId": self.pump_id,
            "unitPriceRaw": self.unit_price_raw,
            "executionStatus": self.execution_status,
            "accepted": self.accepted,
            "appliedAddresses": list(self.applied_addresses),
            "gaveUpAddresses": list(self.gave_up_addresses),
            "deferredAddresses": list(self.deferred_addresses),
            "unverifiedAddresses": list(self.unverified_addresses),
            "detail": self.detail,
            "updatedAt": datetime.now(UTC).isoformat(),
        }


def _outcome_file_path(correlation_id: str) -> Path:
    safe = "".join(
        ch if ch.isalnum() or ch in "-_" else "_"
        for ch in (correlation_id or "").strip()
    )
    if not safe:
        raise ValueError("correlation_id required for outcome file")
    return outcomes_dir() / f"{safe}.json"


def _parse_outcome_dict(raw: dict[str, Any]) -> SetPriceOutcome | None:
    try:
        unit = int(raw.get("unitPriceRaw") or 0)
    except (TypeError, ValueError):
        unit = 0
    if unit <= 0:
        return None
    corr = str(raw.get("correlationId") or "").strip()
    cmd = str(raw.get("commandId") or corr).strip()
    if not corr:
        return None

    def _addrs(key: str) -> tuple[int, ...]:
        val = raw.get(key) or []
        if not isinstance(val, list):
            return ()
        out: list[int] = []
        for item in val:
            try:
                out.append(int(item))
            except (TypeError, ValueError):
                continue
        return tuple(out)

    return SetPriceOutcome(
        correlation_id=corr,
        command_id=cmd or corr,
        station_id=str(raw["stationId"]) if raw.get("stationId") else None,
        pump_id=str(raw["pumpId"]) if raw.get("pumpId") else None,
        unit_price_raw=unit,
        execution_status=str(raw.get("executionStatus") or "UNKNOWN").strip().upper(),
        accepted=bool(raw.get("accepted", False)),
        applied_addresses=_addrs("appliedAddresses"),
        gave_up_addresses=_addrs("gaveUpAddresses"),
        deferred_addresses=_addrs("deferredAddresses"),
        unverified_addresses=_addrs("unverifiedAddresses"),
        detail=str(raw["detail"]) if raw.get("detail") else None,
    )


def _migrate_legacy_outcome_file() -> None:
    """Move legacy single-file outcome into the durable multi-correlation dir."""
    legacy = outcome_path()
    if not legacy.is_file():
        return
    try:
        raw = json.loads(legacy.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        with contextlib.suppress(OSError):
            legacy.unlink()
        return
    if not isinstance(raw, dict):
        with contextlib.suppress(OSError):
            legacy.unlink()
        return
    outcome = _parse_outcome_dict(raw)
    with contextlib.suppress(OSError):
        legacy.unlink()
    if outcome is not None:
        write_set_price_outcome(outcome)


def _fsync_dir(path: Path) -> None:
    """Fsync the parent directory so the rename is power-loss durable."""
    dir_fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> Path:
    """Write JSON with: temp → fsync file → atomic replace → fsync directory.

    If the post-replace directory sync fails, the final path is removed so
    callers (``has_set_price_outcome``) never treat a non-durable write as done.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    body = json.dumps(payload, separators=(",", ":"))
    replaced = False
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(body)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        replaced = True
        _fsync_dir(path)
    except OSError:
        with contextlib.suppress(OSError):
            if tmp.is_file():
                tmp.unlink()
        if replaced:
            with contextlib.suppress(OSError):
                if path.is_file():
                    path.unlink()
            with contextlib.suppress(OSError):
                _fsync_dir(path)
        raise
    return path


def write_set_price_outcome(outcome: SetPriceOutcome) -> Path:
    """Persist a final outcome until cloud-sync MQTT delivery is acknowledged.

    One file per correlationId so concurrent / sequential SET_PRICE results
    never overwrite each other. Power-loss durable before return so the
    controller may safely clear the pending request afterward. A failed
    directory sync after replace removes the file so it is not treated as
    durable on the next tick.
    """
    return _atomic_write_json(_outcome_file_path(outcome.correlation_id), outcome.to_dict())


def ack_hold_dir() -> Path:
    return request_dir() / DEFAULT_ACK_HOLD_DIR_NAME


def _ack_hold_file_path(correlation_id: str) -> Path:
    safe = "".join(
        ch if ch.isalnum() or ch in "-_" else "_"
        for ch in (correlation_id or "").strip()
    )
    if not safe:
        raise ValueError("correlation_id required for ACK-hold file")
    return ack_hold_dir() / f"{safe}.json"


def write_ack_hold(correlation_id: str, *, reason: str) -> Path:
    """Durable marker: cloud-sync must not ACK-delete this correlation's outcome."""
    return _atomic_write_json(
        _ack_hold_file_path(correlation_id),
        {
            "correlationId": correlation_id,
            "reason": reason,
            "updatedAt": datetime.now(UTC).isoformat(),
        },
    )


def clear_ack_hold(correlation_id: str) -> None:
    """Best-effort remove of an ACK-hold after durable request unlink succeeds."""
    try:
        path = _ack_hold_file_path(correlation_id)
    except ValueError:
        return
    if not path.is_file():
        return
    with contextlib.suppress(OSError):
        path.unlink()
    with contextlib.suppress(OSError):
        _fsync_dir(path)


def has_ack_hold(correlation_id: str) -> bool:
    try:
        return _ack_hold_file_path(correlation_id).is_file()
    except ValueError:
        return False


def pending_request_blocks_outcome_ack(correlation_id: str) -> bool:
    """True when cloud-sync must not ACK-delete this outcome yet.

    Blocks while the matching request is still present, or while a durable
    ACK-hold marker remains (request restore after unlink sync could not be
    made durable).
    """
    if has_ack_hold(correlation_id):
        return True
    pending = read_set_price_request()
    if pending is None:
        return False
    return pending.correlation_id.strip() == (correlation_id or "").strip()


def has_set_price_outcome(correlation_id: str) -> bool:
    """True when a durable outcome file already exists for this correlation."""
    _migrate_legacy_outcome_file()
    try:
        return _outcome_file_path(correlation_id).is_file()
    except ValueError:
        return False


def list_set_price_outcomes() -> list[SetPriceOutcome]:
    """Read pending outcomes without removing them (ordered by filename)."""
    _migrate_legacy_outcome_file()
    directory = outcomes_dir()
    if not directory.is_dir():
        return []
    out: list[SetPriceOutcome] = []
    for path in sorted(directory.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(raw, dict):
            continue
        parsed = _parse_outcome_dict(raw)
        if parsed is not None:
            out.append(parsed)
    return out


def ack_set_price_outcome(correlation_id: str) -> bool:
    """Remove a retained outcome after MQTT publish was acknowledged."""
    path = _outcome_file_path(correlation_id)
    if not path.is_file():
        return False
    with contextlib.suppress(OSError):
        path.unlink()
        return not path.is_file()
    return False


def consume_set_price_outcome() -> SetPriceOutcome | None:
    """Compatibility helper: peek the oldest pending outcome and ACK it.

    Prefer ``list_set_price_outcomes`` + ``ack_set_price_outcome`` when the
    publisher must retain until MQTT ACK.
    """
    pending = list_set_price_outcomes()
    if not pending:
        return None
    first = pending[0]
    ack_set_price_outcome(first.correlation_id)
    return first


DEFAULT_PENDING_VERIFY_DIR_NAME = "set-price-pending-verify"


def pending_verify_dir() -> Path:
    """Durable late-verification metadata after the request file is removed."""
    return request_dir() / DEFAULT_PENDING_VERIFY_DIR_NAME


def _pending_verify_file_path(correlation_id: str) -> Path:
    safe = "".join(
        ch if ch.isalnum() or ch in "-_" else "_"
        for ch in (correlation_id or "").strip()
    )
    if not safe:
        raise ValueError("correlation_id required for pending-verify file")
    return pending_verify_dir() / f"{safe}.json"


@dataclass(frozen=True, slots=True)
class SetPricePendingVerify:
    """Retained after SENT_UNVERIFIED so a later matching DC3 can upgrade.

    Lives independently of the execution request and of the publishable
    outcome file (which cloud-sync ACK-deletes after MQTT delivery).
    """

    correlation_id: str
    command_id: str
    station_id: str | None
    pump_id: str | None
    unit_price_raw: int
    required_addresses: tuple[int, ...]
    verified_addresses: tuple[int, ...] = ()
    gave_up_addresses: tuple[int, ...] = ()
    # Snapshot last written as a publishable outcome (idempotent re-emit).
    emitted_verified_addresses: tuple[int, ...] = ()
    outcome_revision: int = 0
    created_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "correlationId": self.correlation_id,
            "commandId": self.command_id,
            "stationId": self.station_id,
            "pumpId": self.pump_id,
            "unitPriceRaw": self.unit_price_raw,
            "requiredAddresses": list(self.required_addresses),
            "verifiedAddresses": list(self.verified_addresses),
            "gaveUpAddresses": list(self.gave_up_addresses),
            "emittedVerifiedAddresses": list(self.emitted_verified_addresses),
            "outcomeRevision": self.outcome_revision,
            "createdAt": self.created_at or datetime.now(UTC).isoformat(),
            "updatedAt": datetime.now(UTC).isoformat(),
        }


def _parse_pending_verify_dict(raw: dict[str, Any]) -> SetPricePendingVerify | None:
    try:
        unit = int(raw.get("unitPriceRaw") or 0)
    except (TypeError, ValueError):
        unit = 0
    if unit <= 0:
        return None
    corr = str(raw.get("correlationId") or "").strip()
    cmd = str(raw.get("commandId") or corr).strip()
    if not corr:
        return None

    def _addrs(key: str) -> tuple[int, ...]:
        val = raw.get(key) or []
        if not isinstance(val, list):
            return ()
        out: list[int] = []
        for item in val:
            try:
                out.append(int(item))
            except (TypeError, ValueError):
                continue
        return tuple(sorted(set(out)))

    try:
        revision = int(raw.get("outcomeRevision") or 0)
    except (TypeError, ValueError):
        revision = 0
    required = _addrs("requiredAddresses")
    if not required:
        # Legacy / incomplete — nothing left to verify.
        return None
    return SetPricePendingVerify(
        correlation_id=corr,
        command_id=cmd or corr,
        station_id=str(raw["stationId"]) if raw.get("stationId") else None,
        pump_id=str(raw["pumpId"]) if raw.get("pumpId") else None,
        unit_price_raw=unit,
        required_addresses=required,
        verified_addresses=_addrs("verifiedAddresses"),
        gave_up_addresses=_addrs("gaveUpAddresses"),
        emitted_verified_addresses=_addrs("emittedVerifiedAddresses"),
        outcome_revision=max(0, revision),
        created_at=str(raw["createdAt"]) if raw.get("createdAt") else None,
    )


def write_set_price_pending_verify(pending: SetPricePendingVerify) -> Path:
    """Persist late-verification metadata (survives request clear + outcome ACK)."""
    return _atomic_write_json(
        _pending_verify_file_path(pending.correlation_id), pending.to_dict()
    )


def read_set_price_pending_verify(correlation_id: str) -> SetPricePendingVerify | None:
    try:
        path = _pending_verify_file_path(correlation_id)
    except ValueError:
        return None
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    return _parse_pending_verify_dict(raw)


def list_set_price_pending_verifies() -> list[SetPricePendingVerify]:
    directory = pending_verify_dir()
    if not directory.is_dir():
        return []
    out: list[SetPricePendingVerify] = []
    for path in sorted(directory.glob("*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(raw, dict):
            continue
        parsed = _parse_pending_verify_dict(raw)
        if parsed is not None:
            out.append(parsed)
    return out


def clear_set_price_pending_verify(correlation_id: str) -> bool:
    try:
        path = _pending_verify_file_path(correlation_id)
    except ValueError:
        return False
    if not path.is_file():
        return False
    with contextlib.suppress(OSError):
        path.unlink()
        return not path.is_file()
    return False


def supersede_set_price_pending_verifies(*, except_correlation_id: str | None = None) -> int:
    """Drop late-verify records superseded by a newer SET_PRICE command."""
    cleared = 0
    for pending in list_set_price_pending_verifies():
        if (
            except_correlation_id
            and pending.correlation_id.strip() == except_correlation_id.strip()
        ):
            continue
        if clear_set_price_pending_verify(pending.correlation_id):
            cleared += 1
    return cleared


def pending_verify_from_outcome(outcome: SetPriceOutcome) -> SetPricePendingVerify | None:
    """Build late-verify metadata from a SENT_UNVERIFIED / partial outcome."""
    open_addrs = tuple(sorted(set(outcome.unverified_addresses)))
    applied = tuple(sorted(set(outcome.applied_addresses)))
    if outcome.execution_status == "PRICE_CONFIRMED":
        return None
    if not open_addrs and outcome.execution_status != "SENT_UNVERIFIED":
        return None
    required = tuple(sorted(set(open_addrs) | set(applied)))
    if not required:
        return None
    return SetPricePendingVerify(
        correlation_id=outcome.correlation_id,
        command_id=outcome.command_id,
        station_id=outcome.station_id,
        pump_id=outcome.pump_id,
        unit_price_raw=outcome.unit_price_raw,
        required_addresses=required,
        verified_addresses=applied,
        gave_up_addresses=tuple(sorted(set(outcome.gave_up_addresses))),
        emitted_verified_addresses=applied,
        outcome_revision=0,
    )

