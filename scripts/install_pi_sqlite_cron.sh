#!/usr/bin/env bash
# Install daily SQLite backup + integrity cron on a pump Pi.
# Does not modify intelipump.service.
#
#   sudo ./scripts/install_pi_sqlite_cron.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SRC="${REPO_ROOT}/scripts/pi_sqlite_backup_and_check.sh"
DST="/usr/local/sbin/pi_sqlite_backup_and_check.sh"
CRON_DST="/etc/cron.d/intelipump-sqlite"
HOUR="${INTELIPUMP_DB_BACKUP_HOUR:-3}"
MINUTE="${INTELIPUMP_DB_BACKUP_MINUTE:-15}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root (sudo)" >&2
  exit 1
fi
if [[ ! -f "$SRC" ]]; then
  echo "Missing $SRC" >&2
  exit 1
fi

install -o root -g root -m 0755 "$SRC" "$DST"
install -d -o intelipump -g intelipump -m 0750 /var/lib/intelipump/backups 2>/dev/null \
  || install -d -m 0750 /var/lib/intelipump/backups

cat >"$CRON_DST" <<EOF
# Daily intelipump SQLite backup + integrity_check (controller untouched).
SHELL=/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
${MINUTE} ${HOUR} * * * root ${DST}
EOF
chmod 644 "$CRON_DST"

echo "Installed ${DST}"
echo "Installed ${CRON_DST} (daily ${HOUR}:$(printf '%02d' "$MINUTE") local)"
echo "Manual run: ${DST}"
