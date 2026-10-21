#!/usr/bin/env bash
# Deploy SQLite resilience (outbox prune, integrity degrade, daily backup cron)
# to one SAO pump Pi. Restarts cloud-sync only by default — hose controller stays up.
#
# From intelipump-fdc/ on your Mac:
#   ./scripts/deploy_sqlite_resilience_to_pi.sh --pump 3 intelipump@<pi-ip>
#   ./scripts/deploy_sqlite_resilience_to_pi.sh --pump 2 intelipump@<pi-ip>
#   ./scripts/deploy_sqlite_resilience_to_pi.sh --pump 4 intelipump@<pi-ip> --restart-controller
#
# Env:
#   INTELIPUMP_REMOTE_DIR  Remote repo (default /home/intelipump/intelipump/intelipump-fdc)
#
set -euo pipefail

PUMP_NUM=""
HOST=""
RESTART_CONTROLLER=0

usage() {
  cat <<'EOF'
Deploy SQLite resilience fix to one pump Pi (cloud-sync path).

Required:
  --pump N          Physical pump number (1..12)
  user@pi-host      SSH target

Optional:
  --restart-controller   Also restart intelipump.service (default: cloud-sync only)
  -h, --help

Examples:
  ./scripts/deploy_sqlite_resilience_to_pi.sh --pump 3 intelipump@100.x.x.x
  ./scripts/deploy_sqlite_resilience_to_pi.sh --pump 2 intelipump@100.x.x.y
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pump)
      PUMP_NUM="${2:?}"
      shift
      ;;
    --restart-controller) RESTART_CONTROLLER=1 ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ -z "$HOST" ]]; then
        HOST="$1"
      else
        echo "Unexpected argument: $1" >&2
        exit 2
      fi
      ;;
  esac
  shift
done

if [[ -z "$PUMP_NUM" || -z "$HOST" ]]; then
  usage >&2
  exit 2
fi
if ! [[ "$PUMP_NUM" =~ ^[1-9]$|^1[0-2]$ ]]; then
  echo "Invalid --pump ${PUMP_NUM}; expected 1..12." >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REMOTE_DIR="${INTELIPUMP_REMOTE_DIR:-/home/intelipump/intelipump/intelipump-fdc}"
printf -v DEVICE_ID "InteliPump-SAO-RS1-pi-%03d" "$PUMP_NUM"

echo "==> SQLite resilience deploy"
echo "    pump=${PUMP_NUM}  device=${DEVICE_ID}"
echo "    host=${HOST}"
echo "    remote=${REMOTE_DIR}"
echo "    restart_controller=${RESTART_CONTROLLER}"

echo
echo "==> 1/4 rsync code (keep remote .venv)"
ssh "$HOST" "mkdir -p '$(dirname "$REMOTE_DIR")'"
rsync -az --delete \
  --exclude '.venv' \
  --exclude '__pycache__' \
  --exclude '.pytest_cache' \
  --exclude '.git' \
  --exclude '*.pyc' \
  --exclude '.mypy_cache' \
  "${REPO_ROOT}/" "${HOST}:${REMOTE_DIR}/"

echo
echo "==> 2/4 remote: deps + sqlite3 + backup cron"
# shellcheck disable=SC2029
ssh -t "$HOST" "bash -s" <<REMOTE
set -euo pipefail
cd '${REMOTE_DIR}'
if [[ -x .venv/bin/python ]]; then
  if command -v uv >/dev/null 2>&1; then
    uv sync
  else
    echo "(uv missing — using existing .venv)"
  fi
else
  echo "ERROR: missing ${REMOTE_DIR}/.venv — run full pump install first" >&2
  exit 1
fi
if ! command -v sqlite3 >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y sqlite3
fi
sudo ./scripts/install_pi_sqlite_cron.sh
REMOTE

echo
echo "==> 3/4 restart services"
if [[ "$RESTART_CONTROLLER" -eq 1 ]]; then
  # shellcheck disable=SC2029
  ssh "$HOST" "sudo systemctl restart intelipump.service intelipump-cloud-sync.service"
else
  # shellcheck disable=SC2029
  ssh "$HOST" "sudo systemctl restart intelipump-cloud-sync.service"
fi

echo
echo "==> 4/4 verify"
# shellcheck disable=SC2029
ssh "$HOST" "bash -s" <<'REMOTE'
set -euo pipefail
echo "--- units ---"
systemctl is-active intelipump.service intelipump-cloud-sync.service || true
echo "--- cron ---"
ls -l /etc/cron.d/intelipump-sqlite /usr/local/sbin/pi_sqlite_backup_and_check.sh 2>/dev/null || true
echo "--- integrity (quick) ---"
DB=/var/lib/intelipump/intelipump.db
if [[ -f "$DB" ]] && command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "$DB" 'PRAGMA quick_check;' | head -1
  sqlite3 "$DB" "SELECT status, COUNT(*) FROM sync_queue GROUP BY status;" || true
else
  echo "skip (no db or sqlite3)"
fi
echo "--- cloud-sync journal (last resilience lines) ---"
journalctl -u intelipump-cloud-sync -n 60 --no-pager 2>/dev/null \
  | grep -E 'sync_queue_pruned|sync_queue_size_alert|database_degraded|cloud_sync_sqlite|api_sqlite_outbox|sqlite' \
  || echo "(no resilience log lines yet — normal right after restart)"
REMOTE

echo
echo "OK: pump-${PUMP_NUM} SQLite resilience deployed on ${HOST}"
echo "Controller was $([ "$RESTART_CONTROLLER" -eq 1 ] && echo 'restarted' || echo 'left running')."
echo "Watch: ssh ${HOST} 'journalctl -u intelipump-cloud-sync -f'"
