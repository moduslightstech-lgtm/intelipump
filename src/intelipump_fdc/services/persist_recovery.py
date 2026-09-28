"""Durable spill for CRITICAL persistence jobs that must survive restart.

Sale-completion jobs are write-ahead logged by stable identity
``{kind}:{address}:{transactionId}:{event}`` so a restart (or in-process
queue-full recovery) can replay them. Handlers remain idempotent via
transaction UUID / complete_once.

Durability protocol: write temp → fsync file → atomic replace → fsync dir.
Corrupt JSONL or I/O errors raise visibly (never silent skip).
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any


class PersistRecoveryError(Exception):
    """Base error for durable persist recovery store failures."""


class PersistRecoveryIOError(PersistRecoveryError):
    """Unreadable / unwritable recovery storage."""


class PersistRecoveryCorruptError(PersistRecoveryError):
    """Recovery file contains undecodable or invalid records."""


class PersistRecoveryIdentityConflict(PersistRecoveryError):
    """identity_key already holds a different pending sale/event payload."""


@dataclass(frozen=True)
class RecoverablePersistJob:
    identity_key: str
    kind: str
    payload: dict[str, Any]
    attempt: int
    last_error: str | None
    updated_at: str
    payload_digest: str


def _payload_digest(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
    return sha256(body.encode("utf-8")).hexdigest()


def _fsync_dir(path: Path) -> None:
    dir_fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _atomic_write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    body = "\n".join(
        json.dumps(r, separators=(",", ":"), sort_keys=True, default=str) for r in rows
    )
    if body:
        body += "\n"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(body)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        _fsync_dir(path)
    except OSError as exc:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise PersistRecoveryIOError(f"failed durable write {path}: {exc}") from exc


class PersistRecoveryStore:
    """JSONL-backed pending CRITICAL jobs (one logical row per identity_key)."""

    def __init__(self, path: Path) -> None:
        if path.exists() and path.is_dir():
            raise PersistRecoveryIOError(f"recovery path is a directory: {path}")
        # Refuse accidental /tmp use outside explicit test opt-in.
        try:
            resolved = path.expanduser().resolve()
        except OSError:
            resolved = path
        text = str(resolved)
        ephemeral = (
            text == "/tmp"
            or text.startswith("/tmp/")
            or text == "/private/tmp"
            or text.startswith("/private/tmp/")
        )
        if ephemeral:
            allow = os.environ.get("INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP", "") == "1"
            if not allow:
                raise PersistRecoveryIOError(
                    f"persist recovery must not use /tmp ({path}); "
                    "set INTELIPUMP_PERSIST_RECOVERY_ALLOW_TMP=1 only for tests"
                )
        self._path = path
        self._lock = threading.Lock()
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise PersistRecoveryIOError(
                f"cannot create recovery directory {path.parent}: {exc}"
            ) from exc

    @property
    def path(self) -> Path:
        return self._path

    def upsert(
        self,
        *,
        identity_key: str,
        kind: str,
        payload: dict[str, Any],
        attempt: int = 0,
        last_error: str | None = None,
    ) -> None:
        digest = _payload_digest(payload)
        record = {
            "identity_key": identity_key,
            "kind": kind,
            "payload": payload,
            "payload_digest": digest,
            "attempt": int(attempt),
            "last_error": last_error,
            "status": "PENDING",
            "updated_at": datetime.now(UTC).isoformat(),
        }
        with self._lock:
            rows = self._read_all_unlocked()
            kept: list[dict[str, Any]] = []
            for row in rows:
                if row.get("identity_key") != identity_key:
                    kept.append(row)
                    continue
                existing_digest = row.get("payload_digest") or _payload_digest(
                    row["payload"] if isinstance(row.get("payload"), dict) else {}
                )
                if existing_digest != digest:
                    raise PersistRecoveryIdentityConflict(
                        f"refusing to overwrite pending sale identity={identity_key} "
                        f"existing_digest={existing_digest} new_digest={digest}"
                    )
                # Same sale+event: refresh attempt / error only (drop old row).
            kept.append(record)
            _atomic_write_jsonl(self._path, kept)

    def mark_done(self, identity_key: str) -> None:
        with self._lock:
            rows = self._read_all_unlocked()
            kept = [r for r in rows if r.get("identity_key") != identity_key]
            if len(kept) != len(rows):
                _atomic_write_jsonl(self._path, kept)

    def list_pending(self) -> list[RecoverablePersistJob]:
        with self._lock:
            rows = self._read_all_unlocked()
        out: list[RecoverablePersistJob] = []
        for row in rows:
            if row.get("status") != "PENDING":
                continue
            key = row.get("identity_key")
            kind = row.get("kind")
            payload = row.get("payload")
            if not isinstance(key, str) or not isinstance(kind, str):
                raise PersistRecoveryCorruptError(
                    f"invalid recovery row missing identity/kind: {row!r}"
                )
            if not isinstance(payload, dict):
                raise PersistRecoveryCorruptError(
                    f"invalid recovery payload for {key}: {payload!r}"
                )
            digest = str(row.get("payload_digest") or _payload_digest(payload))
            out.append(
                RecoverablePersistJob(
                    identity_key=key,
                    kind=kind,
                    payload=payload,
                    attempt=int(row.get("attempt") or 0),
                    last_error=row.get("last_error")
                    if isinstance(row.get("last_error"), str)
                    else None,
                    updated_at=str(row.get("updated_at") or ""),
                    payload_digest=digest,
                )
            )
        return out

    def pending_count(self) -> int:
        return len(self.list_pending())

    def _read_all_unlocked(self) -> list[dict[str, Any]]:
        if not self._path.exists():
            return []
        try:
            text = self._path.read_text(encoding="utf-8")
        except OSError as exc:
            raise PersistRecoveryIOError(f"cannot read recovery file {self._path}: {exc}") from exc
        rows: list[dict[str, Any]] = []
        for line_no, line in enumerate(text.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PersistRecoveryCorruptError(
                    f"corrupt recovery JSONL {self._path}:{line_no}: {exc}"
                ) from exc
            if not isinstance(obj, dict):
                raise PersistRecoveryCorruptError(
                    f"corrupt recovery row type at {self._path}:{line_no}"
                )
            rows.append(obj)
        return rows


def sale_persist_identity(kind: str, payload: dict[str, Any]) -> str | None:
    """Stable identity unique per sale *and* event (never collapses distinct sales)."""
    if kind == "state_changed":
        inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        addr = payload.get("address")
        tx = (
            inner.get("active_transaction_id")
            or inner.get("transaction_id")
            or inner.get("completion_evidence_key")
        )
        detail = str(payload.get("detail") or "")
        event = str(inner.get("normalized_state") or detail or "STATE_CHANGED")
        if event not in {"FILLING_COMPLETE", "LIMIT_REACHED"} and "COMPLETE" not in detail.upper():
            return None
        if not tx:
            return None
        return f"state_changed:{addr}:{tx}:{event}"
    if kind == "app_decoded":
        detail = str(payload.get("detail") or "")
        if "COMPLETE" not in detail.upper():
            return None
        inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        addr = payload.get("address")
        tx = inner.get("transaction_id") or inner.get("active_transaction_id")
        if not tx:
            return None
        return f"app_decoded:{addr}:{tx}:{detail or 'COMPLETE'}"
    return None
