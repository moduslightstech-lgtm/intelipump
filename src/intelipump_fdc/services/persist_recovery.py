"""Durable spill for CRITICAL persistence jobs that must survive restart.

In-memory PersistenceWorker jobs are lost on process exit. Sale-completion
jobs are write-ahead logged here (by stable identity_key) so a restart can
replay them. Handlers remain idempotent via transaction UUID / complete_once.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RecoverablePersistJob:
    identity_key: str
    kind: str
    payload: dict[str, Any]
    attempt: int
    last_error: str | None
    updated_at: str


class PersistRecoveryStore:
    """JSONL-backed pending CRITICAL jobs (one logical row per identity_key)."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._path.parent.mkdir(parents=True, exist_ok=True)

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
        record = {
            "identity_key": identity_key,
            "kind": kind,
            "payload": payload,
            "attempt": int(attempt),
            "last_error": last_error,
            "status": "PENDING",
            "updated_at": datetime.now(UTC).isoformat(),
        }
        with self._lock:
            rows = self._read_all_unlocked()
            rows = [r for r in rows if r.get("identity_key") != identity_key]
            rows.append(record)
            self._write_all_unlocked(rows)

    def mark_done(self, identity_key: str) -> None:
        with self._lock:
            rows = self._read_all_unlocked()
            changed = False
            kept: list[dict[str, Any]] = []
            for row in rows:
                if row.get("identity_key") == identity_key:
                    changed = True
                    continue
                kept.append(row)
            if changed:
                self._write_all_unlocked(kept)

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
                continue
            if not isinstance(payload, dict):
                continue
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
                )
            )
        return out

    def pending_count(self) -> int:
        return len(self.list_pending())

    def _read_all_unlocked(self) -> list[dict[str, Any]]:
        if not self._path.exists():
            return []
        rows: list[dict[str, Any]] = []
        try:
            text = self._path.read_text(encoding="utf-8")
        except OSError:
            return []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                rows.append(obj)
        return rows

    def _write_all_unlocked(self, rows: list[dict[str, Any]]) -> None:
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        body = "\n".join(json.dumps(r, separators=(",", ":"), sort_keys=True) for r in rows)
        if body:
            body += "\n"
        tmp.write_text(body, encoding="utf-8")
        tmp.replace(self._path)


def sale_persist_identity(kind: str, payload: dict[str, Any]) -> str | None:
    """Stable identity for sale-completion jobs (prevents duplicate recovery)."""
    if kind == "state_changed":
        inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        addr = payload.get("address")
        tx = (
            inner.get("active_transaction_id")
            or inner.get("transaction_id")
            or inner.get("completion_evidence_key")
        )
        detail = str(payload.get("detail") or "")
        normalized = str(inner.get("normalized_state") or "")
        if normalized not in {"FILLING_COMPLETE", "LIMIT_REACHED"} and "COMPLETE" not in detail.upper():
            return None
        if tx:
            return f"sale:{addr}:{tx}"
        return f"state_changed:{addr}:{detail}:{normalized}"
    if kind == "app_decoded":
        detail = str(payload.get("detail") or "")
        if "COMPLETE" not in detail.upper():
            return None
        inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
        addr = payload.get("address")
        tx = inner.get("transaction_id") or inner.get("active_transaction_id")
        if tx:
            return f"sale-app:{addr}:{tx}"
        return f"app_decoded:{addr}:{detail}"
    return None
