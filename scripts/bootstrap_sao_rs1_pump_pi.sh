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
PRICE="${INTELIPUMP_UNIT_PRICE:-}"
PRODUCT="${INTELIPUMP_PRODUCT:-PMS}"
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
  --price N                default 1400 PMS / 1875 AGO
  --product CODE           PMS (default) or AGO
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
    --product)
      PRODUCT="${2:?}"
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
PRODUCT="$(echo "$PRODUCT" | tr '[:lower:]' '[:upper:]')"
if [[ "$PRODUCT" != "PMS" && "$PRODUCT" != "AGO" ]]; then
  echo "Invalid --product ${PRODUCT}; expected PMS or AGO." >&2
  exit 2
fi
if [[ -z "$PRICE" ]]; then
  if [[ "$PRODUCT" == "AGO" ]]; then PRICE=1875; else PRICE=1400; fi
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
echo "==> Product: ${PRODUCT}  price: ${PRICE}"
echo "==> Port:   ${PORT}"

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
  --product "$PRODUCT"
  --addresses "$ADDRESSES"
)
# Install units/binaries first; we always set MQTT password before starting cloud-sync.
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

# Write password without printing it (handles special characters; verifies non-empty).
MQTT_PASSWORD="$MQTT_PW" SYNC_ENV="$SYNC_ENV" sudo -E python3 - <<'PY'
import os
from pathlib import Path

path = Path(os.environ["SYNC_ENV"])
password = os.environ["MQTT_PASSWORD"]
if not password:
    raise SystemExit("MQTT password is empty")
text = path.read_text() if path.exists() else ""
lines = []
found = False
for line in text.splitlines():
    if line.startswith("INTELIPUMP_MQTT__PASSWORD="):
        lines.append("INTELIPUMP_MQTT__PASSWORD=" + password)
        found = True
    else:
        lines.append(line)
if not found:
    lines.append("INTELIPUMP_MQTT__PASSWORD=" + password)
path.write_text("\n".join(lines) + "\n")
path.chmod(0o640)
written = next(
    (ln.split("=", 1)[1] for ln in path.read_text().splitlines() if ln.startswith("INTELIPUMP_MQTT__PASSWORD=")),
    "",
)
if not written:
    raise SystemExit(f"Failed to persist MQTT password in {path}")
print(f"OK: MQTT password set in {path} (length={len(written)})")
PY

if [[ "$DO_START" -eq 1 ]]; then
  echo "==> enable --now controller + cloud-sync"
  sudo systemctl enable intelipump.service intelipump-cloud-sync.service
  sudo systemctl restart intelipump.service
  sudo systemctl restart intelipump-cloud-sync.service
  sleep 2
  echo "==> Status"
  systemctl is-active intelipump.service intelipump-cloud-sync.service || true
  if [[ "$(systemctl is-active intelipump-cloud-sync.service)" != "active" ]]; then
    echo "ERROR: intelipump-cloud-sync failed to start. Last logs:" >&2
    journalctl -u intelipump-cloud-sync -n 40 --no-pager || true
    exit 1
  fi
  journalctl -u intelipump -u intelipump-cloud-sync -n 25 --no-pager || true
fi

echo
echo "Done. Pump ${PUMP_NUM} / ${DEVICE_ID}"
echo "Next (cloud Twin catalog + tank pipe) from Mac/droplet:"
echo "  ./scripts/bootstrap_sao_rs1_pump_cloud.sh --pump ${PUMP_NUM}"
