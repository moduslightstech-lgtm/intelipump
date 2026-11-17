#!/usr/bin/env bash
# Watchdog: if shared SQLite is corrupt (or cloud-sync is crashed because of it),
# run pi_sqlite_recover.sh, clear poison CRITICAL spill, prune delivered outbox,
# checkpoint WAL, and bring cloud-sync back.
#
# Intended as a systemd timer (every few minutes). Idempotent when DB is healthy.
#
#   sudo ./scripts/pi_sqlite_auto_recover.sh
#   sudo INTELIPUMP_AUTO_RECOVER_DRY_RUN=1 ./scripts/pi_sqlite_auto_recover.sh
#
# Does NOT invent sales. Hose controller is stopped only during recover swap
# (exclusive access), then restarted.
set -euo pipefail

DB="${INTELIPUMP_DB_PATH:-/var/lib/intelipump/intelipump.db}"
SQLITE="${SQLITE3_BIN:-sqlite3}"
CLOUD_UNIT="${INTELIPUMP_CLOUD_SYNC_UNIT:-intelipump-cloud-sync.service}"
CTRL_UNIT="${INTELIPUMP_CONTROLLER_UNIT:-intelipump.service}"
SPILL="${INTELIPUMP_PERSIST_RECOVERY_PATH:-/var/lib/intelipump/intelipump.persist_recovery.jsonl}"
STATE_DIR="${INTELIPUMP_AUTO_RECOVER_STATE_DIR:-/var/lib/intelipump}"
COOLDOWN_FILE="${STATE_DIR}/.sqlite_auto_recover_last"
COOLDOWN_SEC="${INTELIPUMP_AUTO_RECOVER_COOLDOWN_SEC:-1800}"
DRY_RUN="${INTELIPUMP_AUTO_RECOVER_DRY_RUN:-0}"
RETENTION_DAYS="${INTELIPUMP_AUTO_RECOVER_DELIVERED_RETENTION_DAYS:-2}"
OWNER="${INTELIPUMP_DB_OWNER:-intelipump}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RECOVER_SH="${INTELIPUMP_RECOVER_SCRIPT:-${SCRIPT_DIR}/pi_sqlite_recover.sh}"

log() { echo "[pi_sqlite_auto_recover] $*"; }

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root (sudo)" >&2
  exit 1
fi
if ! command -v "$SQLITE" >/dev/null 2>&1; then
  echo "Install sqlite3: apt-get install -y sqlite3" >&2
  exit 1
fi
if [[ ! -f "$DB" ]]; then
  log "MISSING $DB — nothing to do"
  exit 0
fi

mkdir -p "$STATE_DIR"
chown "${OWNER}:${OWNER}" "$STATE_DIR" 2>/dev/null || true

db_ok=0
CHECK="$("$SQLITE" "$DB" 'PRAGMA quick_check;' 2>&1 | head -1 || true)"
if [[ "$CHECK" == "ok" ]]; then
  db_ok=1
fi

cloud_active=0
if systemctl is-active --quiet "$CLOUD_UNIT" 2>/dev/null; then
  cloud_active=1
fi

# Healthy DB + cloud-sync up → noop.
if [[ "$db_ok" -eq 1 && "$cloud_active" -eq 1 ]]; then
  log "healthy (quick_check=ok, ${CLOUD_UNIT}=active)"
  exit 0
fi

# Cloud-sync failed but DB ok → just reset rate-limit and start (no recover).
if [[ "$db_ok" -eq 1 && "$cloud_active" -eq 0 ]]; then
  log "DB ok but ${CLOUD_UNIT} inactive — restarting sync only"
  if [[ "$DRY_RUN" == "1" ]]; then
    log "DRY_RUN: would reset-failed + start ${CLOUD_UNIT}"
    exit 0
  fi
  systemctl reset-failed "$CLOUD_UNIT" 2>/dev/null || true
  systemctl start "$CLOUD_UNIT" || true
  sleep 1
  systemctl is-active --quiet "$CLOUD_UNIT" && log "cloud-sync started" || log "WARN: cloud-sync still inactive"
  exit 0
fi

# Corrupt (or unreadable) DB.
log "CORRUPT or unreadable: quick_check='${CHECK}' cloud_active=${cloud_active}"

now="$(date +%s)"
if [[ -f "$COOLDOWN_FILE" ]]; then
  last="$(cat "$COOLDOWN_FILE" 2>/dev/null || echo 0)"
  if [[ "$last" =~ ^[0-9]+$ ]] && (( now - last < COOLDOWN_SEC )); then
    log "cooldown active (${COOLDOWN_SEC}s) — skip recover (last=$(($now - last))s ago)"
    exit 0
  fi
fi

if [[ "$DRY_RUN" == "1" ]]; then
  log "DRY_RUN: would recover with STOP_CONTROLLER=1, clear spill, prune, start services"
  exit 0
fi

if [[ ! -x "$RECOVER_SH" && ! -f "$RECOVER_SH" ]]; then
  echo "Missing recover script: $RECOVER_SH" >&2
  exit 2
fi

echo "$now" >"$COOLDOWN_FILE"

log "=== running recover (STOP_CONTROLLER=1) ==="
INTELIPUMP_DB_PATH="$DB" \
INTELIPUMP_RECOVER_STOP_CONTROLLER=1 \
  bash "$RECOVER_SH"

# Poison CRITICAL spill from corrupt window — leave aside so queue does not storm.
if [[ -f "$SPILL" ]]; then
  poison="${SPILL}.poison.$(date +%Y%m%d-%H%M%S)"
  log "moving spill → $poison"
  mv -f "$SPILL" "$poison" || true
fi
rm -f "${SPILL}.tmp" 2>/dev/null || true

log "=== prune DELIVERED (>${RETENTION_DAYS}d) + WAL checkpoint ==="
systemctl stop "$CLOUD_UNIT" 2>/dev/null || true
"$SQLITE" "$DB" <<SQL
DELETE FROM sync_queue
WHERE status='DELIVERED'
  AND created_at < datetime('now', '-${RETENTION_DAYS} days');
PRAGMA wal_checkpoint(TRUNCATE);
SQL
POST="$("$SQLITE" "$DB" 'PRAGMA integrity_check;' 2>&1 | head -1 || true)"
log "integrity_check=${POST}"

systemctl reset-failed "$CLOUD_UNIT" 2>/dev/null || true
systemctl start "$CTRL_UNIT" 2>/dev/null || true
systemctl start "$CLOUD_UNIT" 2>/dev/null || true
sleep 1

log "controller=$(systemctl is-active "$CTRL_UNIT" 2>/dev/null || true) cloud-sync=$(systemctl is-active "$CLOUD_UNIT" 2>/dev/null || true)"
if [[ "$POST" != "ok" ]]; then
  log "ERROR: DB still bad after recover"
  exit 3
fi
log "done"
exit 0
