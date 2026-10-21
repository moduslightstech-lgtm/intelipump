#!/usr/bin/env bash
# Daily SQLite backup + integrity check for intelipump Pi DBs.
# Safe while services run (uses sqlite3 .backup / online-friendly).
# Does NOT restart or stop intelipump.service.
#
# Install example:
#   sudo cp scripts/pi_sqlite_backup_and_check.sh /usr/local/sbin/
#   sudo chmod +x /usr/local/sbin/pi_sqlite_backup_and_check.sh
#   echo '15 3 * * * root /usr/local/sbin/pi_sqlite_backup_and_check.sh' \
#     | sudo tee /etc/cron.d/intelipump-sqlite
set -euo pipefail

DB="${INTELIPUMP_DB_PATH:-/var/lib/intelipump/intelipump.db}"
DEST="${INTELIPUMP_DB_BACKUP_DIR:-/var/lib/intelipump/backups}"
KEEP_DAYS="${INTELIPUMP_DB_BACKUP_KEEP_DAYS:-7}"
SQLITE="${SQLITE3_BIN:-sqlite3}"
TAG="intelipump-db"

if [[ ! -f "$DB" ]]; then
  logger -t "$TAG" "MISSING db=$DB"
  echo "MISSING $DB" >&2
  exit 1
fi

if ! command -v "$SQLITE" >/dev/null 2>&1; then
  logger -t "$TAG" "sqlite3 not installed — apt-get install -y sqlite3"
  echo "Install sqlite3 first" >&2
  exit 1
fi

mkdir -p "$DEST"
STAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP="$DEST/intelipump-${STAMP}.db"

# Online-safe copy (WAL-aware).
"$SQLITE" "$DB" ".backup '$BACKUP'"
chmod 640 "$BACKUP" || true

INTEGRITY="$("$SQLITE" "$DB" 'PRAGMA integrity_check;' 2>&1 | head -1 || true)"
if [[ "$INTEGRITY" != "ok" ]]; then
  logger -t "$TAG" "CORRUPT integrity=$INTEGRITY db=$DB backup=$BACKUP"
  echo "CORRUPT: $INTEGRITY" >&2
  # Exit non-zero so cron mail / monitoring notices; do not touch controller.
  exit 2
fi

TOTAL="$("$SQLITE" "$DB" 'SELECT COUNT(*) FROM sync_queue;' 2>/dev/null || echo '?')"
PENDING="$("$SQLITE" "$DB" \
  "SELECT COUNT(*) FROM sync_queue WHERE status IN ('PENDING','CLAIMED','AWAITING_APP_ACK');" \
  2>/dev/null || echo '?')"
logger -t "$TAG" "ok backup=$BACKUP sync_queue_total=$TOTAL pending=$PENDING"

find "$DEST" -name 'intelipump-*.db' -mtime "+${KEEP_DAYS}" -delete 2>/dev/null || true
echo "ok backup=$BACKUP integrity=ok total=$TOTAL pending=$PENDING"
