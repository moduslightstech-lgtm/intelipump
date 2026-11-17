#!/usr/bin/env bash
# Install systemd timer that auto-recovers a corrupt intelipump SQLite DB.
#
#   sudo ./scripts/install_pi_sqlite_auto_recover.sh
#
# Timer default: every 5 minutes (OnUnitActiveSec=5min).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SRC="${REPO_ROOT}/scripts/pi_sqlite_auto_recover.sh"
DST="/usr/local/sbin/pi_sqlite_auto_recover.sh"
UNIT_DIR="/etc/systemd/system"
INTERVAL="${INTELIPUMP_AUTO_RECOVER_INTERVAL:-5min}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root (sudo)" >&2
  exit 1
fi
if [[ ! -f "$SRC" ]]; then
  echo "Missing $SRC" >&2
  exit 1
fi

install -o root -g root -m 0755 "$SRC" "$DST"
install -o root -g root -m 0755 \
  "${REPO_ROOT}/scripts/pi_sqlite_recover.sh" \
  /usr/local/sbin/pi_sqlite_recover.sh

mkdir -p /etc/intelipump
CONF=/etc/intelipump/sqlite-auto-recover.env
if [[ ! -f "$CONF" ]]; then
  cat >"$CONF" <<'ENVEOF'
INTELIPUMP_DB_PATH=/var/lib/intelipump/intelipump.db
INTELIPUMP_RECOVER_SCRIPT=/usr/local/sbin/pi_sqlite_recover.sh
INTELIPUMP_AUTO_RECOVER_COOLDOWN_SEC=1800
INTELIPUMP_AUTO_RECOVER_DELIVERED_RETENTION_DAYS=2
ENVEOF
  chmod 644 "$CONF"
fi

cat >"${UNIT_DIR}/intelipump-sqlite-auto-recover.service" <<EOF
[Unit]
Description=InteliPump SQLite auto-recover (corrupt DB / failed cloud-sync)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
EnvironmentFile=-/etc/intelipump/sqlite-auto-recover.env
ExecStart=${DST}
Nice=10
IOSchedulingClass=best-effort
IOSchedulingPriority=7
EOF

cat >"${UNIT_DIR}/intelipump-sqlite-auto-recover.timer" <<EOF
[Unit]
Description=Run InteliPump SQLite auto-recover periodically

[Timer]
OnBootSec=2min
OnUnitActiveSec=${INTERVAL}
AccuracySec=30s
Persistent=true
Unit=intelipump-sqlite-auto-recover.service

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable --now intelipump-sqlite-auto-recover.timer

echo "Installed ${DST}"
echo "Installed timer intelipump-sqlite-auto-recover.timer (every ${INTERVAL})"
echo "Manual: systemctl start intelipump-sqlite-auto-recover.service"
echo "Dry-run: sudo INTELIPUMP_AUTO_RECOVER_DRY_RUN=1 ${DST}"
systemctl list-timers 'intelipump-sqlite-auto-recover*' --no-pager || true
