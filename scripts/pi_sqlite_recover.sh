#!/usr/bin/env bash
# Recover a corrupt intelipump SQLite DB on a pump Pi.
# Prefer stopping cloud-sync first; only stop the controller if recover needs
# exclusive access (documented quiet window).
#
# Usage (on Pi):
#   sudo INTELIPUMP_DB_PATH=/var/lib/intelipump/intelipump.db \
#     ./scripts/pi_sqlite_recover.sh
#
# After success: restart cloud-sync; optionally re-touch meter-read-result*.json
# so today's OPENING republishes.
set -euo pipefail

DB="${INTELIPUMP_DB_PATH:-/var/lib/intelipump/intelipump.db}"
SQLITE="${SQLITE3_BIN:-sqlite3}"
STOP_CONTROLLER="${INTELIPUMP_RECOVER_STOP_CONTROLLER:-0}"
EMPTY_SYNC_QUEUE="${INTELIPUMP_RECOVER_EMPTY_SYNC_QUEUE:-0}"
OWNER="${INTELIPUMP_DB_OWNER:-intelipump}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root (sudo)" >&2
  exit 1
fi
if [[ ! -f "$DB" ]]; then
  echo "MISSING $DB" >&2
  exit 1
fi
if ! command -v "$SQLITE" >/dev/null 2>&1; then
  echo "Install sqlite3: apt-get install -y sqlite3" >&2
  exit 1
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
BAK="${DB}.bak.${STAMP}"
RECOVERED="${DB}.recovered.${STAMP}"

echo "=== stopping cloud-sync ==="
systemctl stop intelipump-cloud-sync.service || true
if [[ "$STOP_CONTROLLER" == "1" ]]; then
  echo "=== stopping controller (quiet window) ==="
  systemctl stop intelipump.service || true
fi

echo "=== copying live DB → $BAK ==="
cp -a "$DB" "$BAK"

echo "=== sqlite3 .recover → $RECOVERED ==="
"$SQLITE" "$BAK" ".recover" | "$SQLITE" "$RECOVERED"

CHECK="$("$SQLITE" "$RECOVERED" 'PRAGMA integrity_check;' 2>&1 | head -1 || true)"
echo "integrity_check=$CHECK"
if [[ "$CHECK" != "ok" ]]; then
  echo "Recovered file still bad — keep $BAK; do not swap." >&2
  exit 2
fi

if [[ "$EMPTY_SYNC_QUEUE" == "1" ]]; then
  echo "=== emptying sync_queue on recovered DB (outbox rebuild) ==="
  "$SQLITE" "$RECOVERED" "DELETE FROM sync_queue;"
fi

echo "=== swapping recovered DB into place ==="
# Preserve WAL companions if present after stop.
mv -f "$DB" "${DB}.pre-recover.${STAMP}" 2>/dev/null || true
rm -f "${DB}-wal" "${DB}-shm" 2>/dev/null || true
mv -f "$RECOVERED" "$DB"
chown "${OWNER}:${OWNER}" "$DB" 2>/dev/null || true
chmod 640 "$DB" || true

echo "=== starting services ==="
if [[ "$STOP_CONTROLLER" == "1" ]]; then
  systemctl start intelipump.service || true
fi
systemctl start intelipump-cloud-sync.service || true

echo
echo "Done. Backup of pre-recover live file: ${DB}.pre-recover.${STAMP}"
echo "Original copy: $BAK"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Republish meter OPENING (controller untouched):"
echo "  sudo ${SCRIPT_DIR}/pi_sqlite_republish_meter_results.sh"
