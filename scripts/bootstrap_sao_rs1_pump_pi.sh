#!/usr/bin/env bash
# One-shot SAO pump Pi setup (run ON the Pi).
#
#   git pull → install controller + cloud-sync for --pump N → set MQTT password → start
#
# Usage:
#   cd ~/intelipump-fdc/intelipump   # or your clone path
#   ./scripts/bootstrap_sao_rs1_pump_pi.sh --pump 3 --mqtt-password 'SECRET'
#
# Or:
#   MQTT_PASSWORD='SECRET' ./scripts/bootstrap_sao_rs1_pump_pi.sh --pump 3
#
# Optional:
#   --branch v4-cloud-deploy
#   --port /dev/serial/by-id/usb-1a86_USB_Serial-if00-port0
#   --price 1400
#   --no-pull
#   --no-start
#
# Does NOT change controller source — only pulls + runs install_sao_rs1_pump_pi.sh.
#
set -euo pipefail

PUMP_NUM=""
MQTT_PW="${MQTT_PASSWORD:-}"
BRANCH="${INTELIPUMP_GIT_BRANCH:-v4-cloud-deploy}"
PORT="${INTELIPUMP_SERIAL_PORT:-}"
PRICE="${INTELIPUMP_UNIT_PRICE:-1400}"
DO_PULL=1
DO_START=1
ADDRESSES="${INTELIPUMP_ADDRESSES:-1,2}"

usage() {
  cat <<'EOF'
Bootstrap one SAO physical-pump Pi (pull + install + MQTT password + start).

Required:
  --pump N
  --mqtt-password SECRET   (or env MQTT_PASSWORD)

Optional:
  --branch NAME            default v4-cloud-deploy
  --port PATH              serial device (auto-picks by-id / ttyUSB* if omitted)
  --price N                default 1400
  --addresses LIST         default 1,2
  --no-pull                skip git fetch/checkout/pull
  --no-start               install only; do not enable/start services
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pump)
      PUMP_NUM="${2:?}"
      shift
      ;;
    --mqtt-password)
      MQTT_PW="${2:?}"
      shift
      ;;
    --branch)
      BRANCH="${2:?}"
      shift
      ;;
    --port)
      PORT="${2:?}"
      shift
      ;;
    --price)
      PRICE="${2:?}"
      shift
      ;;
    --addresses)
      ADDRESSES="${2:?}"
      shift
      ;;
    --no-pull) DO_PULL=0 ;;
    --no-start) DO_START=0 ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ -z "$PUMP_NUM" ]]; then
  echo "Refused: pass --pump N" >&2
  usage >&2
  exit 2
fi
if ! [[ "$PUMP_NUM" =~ ^[1-9]$|^1[0-2]$ ]]; then
  echo "Invalid --pump ${PUMP_NUM}; expected 1..12." >&2
  exit 2
fi
if [[ -z "$MQTT_PW" ]]; then
  echo "Refused: pass --mqtt-password SECRET or set MQTT_PASSWORD" >&2
  usage >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

detect_port() {
  if [[ -n "$PORT" ]]; then
    echo "$PORT"
    return 0
  fi
  local by_id
  by_id="$(ls -1 /dev/serial/by-id/usb-* 2>/dev/null | head -1 || true)"
  if [[ -n "$by_id" ]]; then
    echo "$by_id"
    return 0
  fi
  if [[ -e /dev/ttyUSB0 ]]; then
    echo /dev/ttyUSB0
    return 0
  fi
  if [[ -e /dev/ttyUSB1 ]]; then
    echo /dev/ttyUSB1
    return 0
  fi
  if [[ -e /dev/ttyACM0 ]]; then
    echo /dev/ttyACM0
    return 0
  fi
  echo /dev/ttyUSB0
}

PORT="$(detect_port)"
printf -v DEVICE_ID "InteliPump-SAO-RS1-pi-%03d" "$PUMP_NUM"

echo "==> SAO pump bootstrap"
echo "==> Repo:   ${REPO_ROOT}"
echo "==> Branch: ${BRANCH}"
echo "==> Pump:   ${PUMP_NUM}  device ${DEVICE_ID}"
echo "==> Port:   ${PORT}"
echo "==> Price:  ${PRICE}"

if [[ "$DO_PULL" -eq 1 ]]; then
  if [[ ! -d .git ]]; then
    echo "ERROR: ${REPO_ROOT} is not a git checkout. Clone first, then rerun." >&2
    exit 1
  fi
  echo "==> git fetch / checkout / pull"
  git fetch --prune origin
  if git show-ref --verify --quiet "refs/remotes/origin/${BRANCH}"; then
    git checkout "$BRANCH"
    git pull --ff-only origin "$BRANCH"
  elif git show-ref --verify --quiet "refs/heads/${BRANCH}"; then
    git checkout "$BRANCH"
    git pull --ff-only || true
  else
    echo "ERROR: branch ${BRANCH} not found locally or on origin." >&2
    exit 1
  fi
fi

INSTALL="${REPO_ROOT}/scripts/install_sao_rs1_pump_pi.sh"
if [[ ! -x "$INSTALL" ]]; then
  echo "ERROR: missing ${INSTALL}. Are you on the right branch?" >&2
  exit 1
fi

INSTALL_ARGS=(
  --pump "$PUMP_NUM"
  --confirm-sao-authorize-install
  --port "$PORT"
  --price "$PRICE"
  --addresses "$ADDRESSES"
)
if [[ "$DO_START" -eq 1 ]]; then
  INSTALL_ARGS+=(--start)
fi

echo "==> Running install_sao_rs1_pump_pi.sh"
"$INSTALL" "${INSTALL_ARGS[@]}"

SYNC_ENV="/etc/intelipump/intelipump-cloud-sync.env"
echo "==> Writing MQTT password into ${SYNC_ENV}"
if [[ ! -f "$SYNC_ENV" ]]; then
  echo "ERROR: ${SYNC_ENV} missing after install." >&2
  exit 1
fi

# Escape for sed replacement (basic)
ESCAPED_PW="$(printf '%s' "$MQTT_PW" | sed -e 's/[\\/&]/g')"
if sudo grep -qE '^INTELIPUMP_MQTT__PASSWORD=' "$SYNC_ENV"; then
  sudo sed -i "s|^INTELIPUMP_MQTT__PASSWORD=.*|INTELIPUMP_MQTT__PASSWORD=${ESCAPED_PW}|" "$SYNC_ENV"
else
  echo "INTELIPUMP_MQTT__PASSWORD=${MQTT_PW}" | sudo tee -a "$SYNC_ENV" >/dev/null
fi

if [[ "$DO_START" -eq 1 ]]; then
  echo "==> enable --now controller + cloud-sync"
  sudo systemctl enable --now intelipump.service
  sudo systemctl enable --now intelipump-cloud-sync.service
  sudo systemctl restart intelipump-cloud-sync.service
  sleep 1
  echo "==> Status"
  systemctl is-active intelipump.service intelipump-cloud-sync.service || true
  journalctl -u intelipump -u intelipump-cloud-sync -n 25 --no-pager || true
fi

echo
echo "Done. Pump ${PUMP_NUM} / ${DEVICE_ID}"
echo "Next (cloud Twin catalog + tank pipe) from Mac/droplet:"
echo "  ./scripts/provision_sao_rs1_pump.sh --pump ${PUMP_NUM}"
echo "  ./scripts/provision_sao_rs1_hardware.sh --pump ${PUMP_NUM}"
