#!/usr/bin/env bash
# Push intelipump-fdc to a new SAO pump-2 Pi and run the install script.
#
# Usage (from your Mac, in intelipump-fdc/):
#   ./scripts/deploy_sao_rs1_pump2_to_pi.sh intelipump@<pi-tailscale-or-lan-ip>
#   ./scripts/deploy_sao_rs1_pump2_to_pi.sh intelipump@100.x.x.x --start
#
# Requirements on the Pi: SSH access, sudo without interactive prompts preferred.
# The pump-2 Pi must have its OWN RS-485 USB adapter — not the pump-1 bus.
#
set -euo pipefail

HOST="${1:-}"
START_FLAG=""
shift || true
for arg in "$@"; do
  case "$arg" in
    --start) START_FLAG="--start" ;;
    -h|--help)
      echo "Usage: $0 user@pi-host [--start]"
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$HOST" ]]; then
  echo "Usage: $0 user@pi-host [--start]" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
REMOTE_DIR="${INTELIPUMP_REMOTE_DIR:-/home/intelipump/intelipump/intelipump-fdc}"

echo "==> Sync ${REPO_ROOT} → ${HOST}:${REMOTE_DIR}"
ssh "$HOST" "mkdir -p '$(dirname "$REMOTE_DIR")'"
rsync -az --delete \
  --exclude '.venv' \
  --exclude '__pycache__' \
  --exclude '.pytest_cache' \
  --exclude '.git' \
  --exclude '*.pyc' \
  --exclude '.mypy_cache' \
  "${REPO_ROOT}/" "${HOST}:${REMOTE_DIR}/"

echo "==> Run install_sao_rs1_pump2_pi.sh on ${HOST}"
ssh -t "$HOST" "cd '${REMOTE_DIR}' && ./scripts/install_sao_rs1_pump2_pi.sh --confirm-sao-authorize-install ${START_FLAG}"

echo
echo "Next:"
echo "  1) On Pi: sudo nano /etc/intelipump/intelipump-cloud-sync.env  # set MQTT password"
echo "  2) On Pi: sudo systemctl enable --now intelipump-cloud-sync.service"
echo "  3) Twin:  cd DigitalTwin && TWIN_ADMIN_EMAIL=... TWIN_ADMIN_PASSWORD=... ./scripts/provision_sao_rs1_pump2.sh"
echo "  4) Verify: ssh ${HOST} 'journalctl -u intelipump -u intelipump-cloud-sync -n 50 --no-pager'"
