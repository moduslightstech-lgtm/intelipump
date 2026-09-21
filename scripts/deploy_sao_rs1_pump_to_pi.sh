#!/usr/bin/env bash
# Push intelipump-fdc to one SAO pump Pi and install (one Pi per physical pump).
#
# Sales path on the Pi:
#   intelipump.service       → talks to Wayne pump on RS-485
#   intelipump-cloud-sync    → publishes TRANSACTION_* to Mosquitto (cloud)
#
# Usage (from your Mac, in intelipump-fdc/):
#   ./scripts/deploy_sao_rs1_pump_to_pi.sh --pump 2 intelipump@<pi-ip>
#   ./scripts/deploy_sao_rs1_pump_to_pi.sh --pump 2 intelipump@<pi-ip> --start
#   MQTT_PASSWORD='...' ./scripts/deploy_sao_rs1_pump_to_pi.sh --pump 2 intelipump@<pi-ip> --start
#
set -euo pipefail

PUMP_NUM=""
HOST=""
START_FLAG=""
PORT_ARGS=()
PRICE_ARGS=()

usage() {
  cat <<'EOF'
Deploy SAO pump controller to a dedicated Pi and enable MQTT sales streaming.

Required:
  --pump N          Physical pump number (1..12)
  user@pi-host      SSH target

Optional:
  --start           Start controller (+ cloud-sync if MQTT password set)
  --port PATH       Serial device on the Pi (default /dev/ttyUSB0)
  --price N         Raw BCD unit price (default 1400)
  MQTT_PASSWORD     If set, written into cloud-sync env on the Pi
  INTELIPUMP_REMOTE_DIR  Remote repo path (default ~/intelipump/intelipump-fdc)

Example:
  MQTT_PASSWORD='secret' ./scripts/deploy_sao_rs1_pump_to_pi.sh --pump 2 intelipump@100.x.x.x --start
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pump)
      PUMP_NUM="${2:?}"
      shift
      ;;
    --start) START_FLAG="--start" ;;
    --port)
      PORT_ARGS=(--port "$2")
      shift
      ;;
    --price)
      PRICE_ARGS=(--price "$2")
      shift
      ;;
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

echo "==> Sync ${REPO_ROOT} → ${HOST}:${REMOTE_DIR}"
echo "==> Pump ${PUMP_NUM}  device ${DEVICE_ID}"
ssh "$HOST" "mkdir -p '$(dirname "$REMOTE_DIR")'"
rsync -az --delete \
  --exclude '.venv' \
  --exclude '__pycache__' \
  --exclude '.pytest_cache' \
  --exclude '.git' \
  --exclude '*.pyc' \
  --exclude '.mypy_cache' \
  "${REPO_ROOT}/" "${HOST}:${REMOTE_DIR}/"

INSTALL_CMD=(
  ./scripts/install_sao_rs1_pump_pi.sh
  --pump "$PUMP_NUM"
  --confirm-sao-authorize-install
)
if [[ -n "$START_FLAG" ]]; then
  INSTALL_CMD+=(--start)
fi
INSTALL_CMD+=("${PORT_ARGS[@]}" "${PRICE_ARGS[@]}")

echo "==> Run install on ${HOST}"
ssh -t "$HOST" "cd '${REMOTE_DIR}' && $(printf '%q ' "${INSTALL_CMD[@]}")"

if [[ -n "${MQTT_PASSWORD:-}" ]]; then
  echo "==> Writing MQTT password on ${HOST}"
  # shellcheck disable=SC2029
  ssh "$HOST" "sudo sed -i 's|^INTELIPUMP_MQTT__PASSWORD=.*|INTELIPUMP_MQTT__PASSWORD=${MQTT_PASSWORD}|' /etc/intelipump/intelipump-cloud-sync.env && sudo systemctl enable --now intelipump-cloud-sync.service && sudo systemctl restart intelipump-cloud-sync.service"
fi

echo
echo "Pi services (sales → MQTT → cloud):"
echo "  ssh ${HOST} 'systemctl is-active intelipump intelipump-cloud-sync'"
echo "  ssh ${HOST} 'journalctl -u intelipump-cloud-sync -n 40 --no-pager'"
echo
echo "Twin catalog (once per pump):"
echo "  cd DigitalTwin && ./scripts/provision_sao_rs1_pump.sh --pump ${PUMP_NUM}"
