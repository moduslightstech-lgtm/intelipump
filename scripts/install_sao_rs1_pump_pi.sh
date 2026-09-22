#!/usr/bin/env bash
# Install InteliPump on one SAO Redeemed Station 1 Pi that owns a single physical pump.
#
# Architecture (every pump = its own Pi + own RS-485 bus):
#   intelipump.service          — Wayne/DART controller (MQTT off)
#   intelipump-cloud-sync       — streams sales to Mosquitto → Digital Twin
#
# Shared station id for all pumps at this site:
#   SAO-Redeemed-Station-1
# Per-Pi device id:
#   InteliPump-SAO-RS1-pi-00N
#
# Usage (on the Pi, from intelipump-fdc):
#   ./scripts/install_sao_rs1_pump_pi.sh --pump 2 --confirm-sao-authorize-install --start
#   ./scripts/install_sao_rs1_pump_pi.sh --pump 3 --confirm-sao-authorize-install --port /dev/ttyUSB0 --start
#
# Then set MQTT password if still empty:
#   sudo nano /etc/intelipump/intelipump-cloud-sync.env
#   sudo systemctl enable --now intelipump-cloud-sync.service
#
set -euo pipefail

CONFIRM=0
START=0
INSTALL_SYNC=1
PUMP_NUM=""
PORT="${INTELIPUMP_SERIAL_PORT:-/dev/ttyUSB0}"
PRICE="${INTELIPUMP_UNIT_PRICE:-}"
PRODUCT="${INTELIPUMP_PRODUCT:-PMS}"
ADDRESSES="${INTELIPUMP_ADDRESSES:-1,2}"
REPO_USER="${INTELIPUMP_RUN_USER:-intelipump}"
STATION_ID="SAO-Redeemed-Station-1"

usage() {
  cat <<'EOF'
Install InteliPump for one SAO physical pump (one Pi per pump, MQTT sales stream).

Required:
  --pump N                          Physical pump number (1..12)
  --confirm-sao-authorize-install   Acknowledge sole-controller authorize install

Optional:
  --start           enable --now controller + cloud-sync after install
  --no-cloud-sync   skip cloud-sync unit/env install
  --port PATH       Serial device (default /dev/ttyUSB0)
  --price N         Raw BCD unit price (default 1400 PMS / 1875 AGO)
  --product CODE    PMS (default) or AGO
  --addresses LIST  DART addresses on this Pi's bus (default 1,2)
  --user NAME       Service user (default intelipump)
  -h, --help

Layout on this Pi's RS-485 bus (default):
  DART 1 → pump-N / nozzle-1
  DART 2 → pump-N / nozzle-2
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --confirm-sao-authorize-install) CONFIRM=1 ;;
    --start) START=1 ;;
    --no-cloud-sync) INSTALL_SYNC=0 ;;
    --pump)
      PUMP_NUM="${2:?--pump requires a number}"
      shift
      ;;
    --port)
      PORT="${2:?--port requires a path}"
      shift
      ;;
    --price)
      PRICE="${2:?--price requires an integer}"
      shift
      ;;
    --product)
      PRODUCT="${2:?--product requires PMS or AGO}"
      shift
      ;;
    --addresses)
      ADDRESSES="${2:?--addresses requires a list}"
      shift
      ;;
    --user)
      REPO_USER="${2:?--user requires a name}"
      shift
      ;;
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

if [[ "$CONFIRM" -ne 1 ]]; then
  echo "Refused: pass --confirm-sao-authorize-install (sole controller authorize)." >&2
  usage >&2
  exit 2
fi

if [[ -z "$PUMP_NUM" ]]; then
  echo "Refused: pass --pump N (1..12)." >&2
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
  if [[ "$PRODUCT" == "AGO" ]]; then
    PRICE=1875
  else
    PRICE=1400
  fi
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

printf -v DEVICE_ID "InteliPump-SAO-RS1-pi-%03d" "$PUMP_NUM"
PUMP_CODE="pump-${PUMP_NUM}"
MAP_REL="config/channel_map.sao-rs1-pump${PUMP_NUM}.json"
MAP_SRC="${REPO_ROOT}/${MAP_REL}"

# Pump 1 keeps the historical day-1 map (source_identifier pump-1 / pump-2).
if [[ "$PUMP_NUM" -eq 1 && -f "${REPO_ROOT}/config/channel_map.sao-rs1.json" && "$PRODUCT" == "PMS" ]]; then
  MAP_REL="config/channel_map.sao-rs1.json"
  MAP_SRC="${REPO_ROOT}/${MAP_REL}"
else
  echo "==> Writing channel map ${MAP_REL} (product=${PRODUCT})"
  cat >"$MAP_SRC" <<EOF
{
  "1": {
    "pump_id": "${PUMP_CODE}",
    "nozzle_id": "nozzle-1",
    "source_identifier": "${PUMP_CODE}-n1",
    "product": "${PRODUCT}"
  },
  "2": {
    "pump_id": "${PUMP_CODE}",
    "nozzle_id": "nozzle-2",
    "source_identifier": "${PUMP_CODE}-n2",
    "product": "${PRODUCT}"
  }
}
EOF
fi

# Prefer pump-specific unit templates when present; else pump-2 templates; else pump-1.
pick_unit() {
  local name="$1"
  local candidates=(
    "${REPO_ROOT}/deploy/systemd/${name}.sao-rs1-pump${PUMP_NUM}.service"
    "${REPO_ROOT}/deploy/systemd/${name}.sao-rs1-pump2.service"
    "${REPO_ROOT}/deploy/systemd/${name}.sao-rs1.service"
  )
  local f
  for f in "${candidates[@]}"; do
    if [[ -f "$f" ]]; then
      echo "$f"
      return 0
    fi
  done
  return 1
}

pick_env() {
  local name="$1"
  local candidates=(
    "${REPO_ROOT}/deploy/systemd/${name}.sao-rs1-pump${PUMP_NUM}.env.example"
    "${REPO_ROOT}/deploy/systemd/${name}.sao-rs1-pump2.env.example"
    "${REPO_ROOT}/deploy/systemd/${name}.sao-rs1.env.example"
  )
  local f
  for f in "${candidates[@]}"; do
    if [[ -f "$f" ]]; then
      echo "$f"
      return 0
    fi
  done
  return 1
}

UNIT_SRC="$(pick_unit intelipump)"
ENV_SRC="$(pick_env intelipump)"
SYNC_UNIT_SRC="$(pick_unit intelipump-cloud-sync)"
SYNC_ENV_SRC="$(pick_env intelipump-cloud-sync)"

UNIT_DST="/etc/systemd/system/intelipump.service"
ENV_DST="/etc/intelipump/intelipump.env"
SYNC_UNIT_DST="/etc/systemd/system/intelipump-cloud-sync.service"
SYNC_ENV_DST="/etc/intelipump/intelipump-cloud-sync.env"
VENV_BIN="${REPO_ROOT}/.venv/bin/intelipump-controller"
SYNC_BIN="${REPO_ROOT}/.venv/bin/intelipump-cloud-sync"
DB_DIR="/var/lib/intelipump"
ETC_DIR="/etc/intelipump"

for f in "$UNIT_SRC" "$ENV_SRC" "$MAP_SRC"; do
  if [[ ! -f "$f" ]]; then
    echo "Missing required file: ${f}" >&2
    exit 1
  fi
done

echo "==> SAO Redeemed Station 1 — physical ${PUMP_CODE} (one Pi per pump)"
echo "==> Repo: ${REPO_ROOT}"
echo "==> Station: ${STATION_ID}"
echo "==> Device:  ${DEVICE_ID}"
echo "==> Product: ${PRODUCT}  price: ${PRICE}"
echo "==> Port: ${PORT}  addresses: ${ADDRESSES}  user: ${REPO_USER}"
echo "==> Channel map: ${MAP_SRC}"
echo "==> MQTT stream: intelipump-cloud-sync → 157.230.215.93 (prod topics)"
echo "WARN: This Pi is the only RS-485 master on THIS bus. Do not share with another Pi."

if ! id -u "$REPO_USER" >/dev/null 2>&1; then
  echo "==> Creating user ${REPO_USER}"
  sudo useradd -m -s /bin/bash "$REPO_USER"
fi
sudo usermod -aG dialout "$REPO_USER" || true

echo "==> Apt packages"
sudo apt-get update -y
sudo apt-get install -y curl git socat python3-venv

if ! command -v uv >/dev/null 2>&1; then
  echo "==> Installing uv"
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="${HOME}/.local/bin:${PATH}"
if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found on PATH after install; open a new shell or source ~/.local/bin/env" >&2
  exit 1
fi

echo "==> uv sync --dev"
uv sync --dev

if [[ ! -x "$VENV_BIN" ]]; then
  echo "Missing executable: ${VENV_BIN}" >&2
  exit 1
fi

echo "==> Data / config directories"
sudo install -d -o "$REPO_USER" -g "$REPO_USER" -m 0750 "$DB_DIR"
sudo install -d -o root -g root -m 0755 "$ETC_DIR"

rewrite_ids() {
  local src="$1"
  local dst="$2"
  sed \
    -e "s|/home/intelipump/intelipump/intelipump-fdc|${REPO_ROOT}|g" \
    -e "s|config/channel_map\\.sao-rs1[^\" ]*\\.json|${MAP_REL}|g" \
    -e "s|InteliPump-SAO-RS1-pi-00[0-9]|${DEVICE_ID}|g" \
    -e "s|SAO-Redeemed-Station-1|${STATION_ID}|g" \
    "$src" >"$dst"
}

echo "==> Writing ${ENV_DST}"
TMP_ENV="$(mktemp)"
rewrite_ids "$ENV_SRC" "$TMP_ENV"
sed -i \
  -e "s|^INTELIPUMP_DART__SERIAL_PORT=.*|INTELIPUMP_DART__SERIAL_PORT=${PORT}|" \
  -e "s|^INTELIPUMP_CONTROLLER__DEVICE_ID=.*|INTELIPUMP_CONTROLLER__DEVICE_ID=${DEVICE_ID}|" \
  -e "s|^INTELIPUMP_CONTROLLER__STATION_ID=.*|INTELIPUMP_CONTROLLER__STATION_ID=${STATION_ID}|" \
  -e "s|^INTELIPUMP_CHANNEL_MAP_PATH=.*|INTELIPUMP_CHANNEL_MAP_PATH=${REPO_ROOT}/${MAP_REL}|" \
  "$TMP_ENV"
sudo install -o root -g root -m 0644 "$TMP_ENV" "$ENV_DST"
rm -f "$TMP_ENV"

echo "==> Writing ${UNIT_DST}"
TMP_UNIT="$(mktemp)"
rewrite_ids "$UNIT_SRC" "$TMP_UNIT"
sed -i \
  -e "s|User=intelipump|User=${REPO_USER}|g" \
  -e "s|Group=intelipump|Group=${REPO_USER}|g" \
  -e "s|--port /dev/ttyUSB0|--port ${PORT}|g" \
  -e "s|--addresses 1,2|--addresses ${ADDRESSES}|g" \
  -e "s|--price 1400|--price ${PRICE}|g" \
  -e "s|INTELIPUMP_CONTROLLER__DEVICE_ID=.*|INTELIPUMP_CONTROLLER__DEVICE_ID=${DEVICE_ID}|" \
  -e "s|INTELIPUMP_CHANNEL_MAP_PATH=.*|INTELIPUMP_CHANNEL_MAP_PATH=${REPO_ROOT}/${MAP_REL}|" \
  "$TMP_UNIT"

if [[ "$PORT" != "/dev/ttyUSB0" ]]; then
  sed -i \
    -e '/^After=dev-ttyUSB0\.device$/d' \
    -e '/^Wants=dev-ttyUSB0\.device$/d' \
    "$TMP_UNIT"
fi

sudo install -o root -g root -m 0644 "$TMP_UNIT" "$UNIT_DST"
rm -f "$TMP_UNIT"

if [[ "$INSTALL_SYNC" -eq 1 ]]; then
  if [[ ! -f "$SYNC_UNIT_SRC" || ! -f "$SYNC_ENV_SRC" ]]; then
    echo "Missing cloud-sync deploy files under deploy/systemd/" >&2
    exit 1
  fi
  if [[ ! -x "$SYNC_BIN" ]]; then
    echo "Missing ${SYNC_BIN}. uv sync should have built it." >&2
    exit 1
  fi

  echo "==> Writing ${SYNC_ENV_DST} (preserve existing password if present)"
  EXISTING_PW=""
  if [[ -f "$SYNC_ENV_DST" ]]; then
    EXISTING_PW="$(sudo grep -E '^INTELIPUMP_MQTT__PASSWORD=' "$SYNC_ENV_DST" | head -1 | cut -d= -f2- || true)"
  fi
  TMP_SYNC_ENV="$(mktemp)"
  rewrite_ids "$SYNC_ENV_SRC" "$TMP_SYNC_ENV"
  sed -i \
    -e "s|^INTELIPUMP_CONTROLLER__DEVICE_ID=.*|INTELIPUMP_CONTROLLER__DEVICE_ID=${DEVICE_ID}|" \
    -e "s|^INTELIPUMP_CONTROLLER__STATION_ID=.*|INTELIPUMP_CONTROLLER__STATION_ID=${STATION_ID}|" \
    -e "s|^INTELIPUMP_CHANNEL_MAP_PATH=.*|INTELIPUMP_CHANNEL_MAP_PATH=${REPO_ROOT}/${MAP_REL}|" \
    -e "s|^INTELIPUMP_MQTT__CLIENT_ID=.*|INTELIPUMP_MQTT__CLIENT_ID=${DEVICE_ID}-sync|" \
    "$TMP_SYNC_ENV"
  if [[ -n "$EXISTING_PW" ]]; then
    sed -i "s|^INTELIPUMP_MQTT__PASSWORD=.*|INTELIPUMP_MQTT__PASSWORD=${EXISTING_PW}|" "$TMP_SYNC_ENV"
  fi
  sudo install -o root -g root -m 0640 "$TMP_SYNC_ENV" "$SYNC_ENV_DST"
  rm -f "$TMP_SYNC_ENV"

  echo "==> Writing ${SYNC_UNIT_DST}"
  TMP_SYNC_UNIT="$(mktemp)"
  rewrite_ids "$SYNC_UNIT_SRC" "$TMP_SYNC_UNIT"
  sed -i \
    -e "s|User=intelipump|User=${REPO_USER}|g" \
    -e "s|Group=intelipump|Group=${REPO_USER}|g" \
    -e "s|--device-id InteliPump-SAO-RS1-pi-[0-9]*|--device-id ${DEVICE_ID}|g" \
    -e "s|--station-id SAO-Redeemed-Station-1|--station-id ${STATION_ID}|g" \
    "$TMP_SYNC_UNIT"
  sudo install -o root -g root -m 0644 "$TMP_SYNC_UNIT" "$SYNC_UNIT_DST"
  rm -f "$TMP_SYNC_UNIT"
fi

echo "==> systemctl daemon-reload"
sudo systemctl daemon-reload

if [[ ! -e "$PORT" ]]; then
  echo "WARN: serial device ${PORT} not present yet (plug USB-RS485 before start)." >&2
else
  echo "==> Serial present: $(ls -l "$PORT")"
fi

if sudo lsof "$PORT" >/dev/null 2>&1; then
  echo "WARN: ${PORT} is in use (stop other masters before starting):" >&2
  sudo lsof "$PORT" || true
fi

echo "==> Binary check"
"$VENV_BIN" --help >/dev/null
echo "OK: ${VENV_BIN}"
if [[ "$INSTALL_SYNC" -eq 1 ]]; then
  "$SYNC_BIN" --help >/dev/null
  echo "OK: ${SYNC_BIN}"
fi

NEED_PW=0
if [[ "$INSTALL_SYNC" -eq 1 ]]; then
  if sudo grep -qE '^INTELIPUMP_MQTT__PASSWORD=$' "$SYNC_ENV_DST" 2>/dev/null; then
    NEED_PW=1
    echo "WARN: set INTELIPUMP_MQTT__PASSWORD in ${SYNC_ENV_DST} before starting cloud-sync." >&2
  fi
fi

if [[ "$START" -eq 1 ]]; then
  echo "==> enable --now intelipump.service"
  sudo systemctl enable --now intelipump.service
  sleep 1
  sudo systemctl --no-pager --full status intelipump.service || true
  sudo journalctl -u intelipump -n 40 --no-pager || true

  if [[ "$INSTALL_SYNC" -eq 1 ]]; then
    if [[ "$NEED_PW" -eq 1 ]]; then
      echo "Skipping cloud-sync start until MQTT password is set." >&2
    else
      echo "==> enable --now intelipump-cloud-sync.service"
      sudo systemctl enable --now intelipump-cloud-sync.service
      sleep 1
      sudo systemctl --no-pager --full status intelipump-cloud-sync.service || true
      sudo journalctl -u intelipump-cloud-sync -n 40 --no-pager || true
    fi
  fi
else
  echo
  echo "Installed but not started. When ready:"
  echo "  sudo systemctl enable --now intelipump.service"
  if [[ "$INSTALL_SYNC" -eq 1 ]]; then
    echo "  sudo nano /etc/intelipump/intelipump-cloud-sync.env   # set MQTT password"
    echo "  sudo systemctl enable --now intelipump-cloud-sync.service"
  fi
  echo "  journalctl -u intelipump -u intelipump-cloud-sync -f"
fi

echo
echo "Done. ${PUMP_CODE} on ${DEVICE_ID}"
echo "Sales stream: intelipump/prod/stations/${STATION_ID}/transactions"
echo "Twin: DigitalTwin/scripts/provision_sao_rs1_pump.sh --pump ${PUMP_NUM}"
echo "One master per bus only."
