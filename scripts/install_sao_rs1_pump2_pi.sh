#!/usr/bin/env bash
# Install InteliPump on a dedicated Pi for SAO Redeemed Station 1 physical pump 2.
#
# Same station as pump 1, new device id. Requires its own RS-485 bus — do not
# attach this Pi to the same USB/serial bus as InteliPump-SAO-RS1-pi-001.
#
# Usage (from intelipump-fdc on the pump-2 Pi):
#   ./scripts/install_sao_rs1_pump2_pi.sh --confirm-sao-authorize-install
#   ./scripts/install_sao_rs1_pump2_pi.sh --confirm-sao-authorize-install --start
#   ./scripts/install_sao_rs1_pump2_pi.sh --confirm-sao-authorize-install --port /dev/ttyUSB0 --start
#
# After install, set MQTT password then start cloud-sync if not using --start:
#   sudo nano /etc/intelipump/intelipump-cloud-sync.env
#   sudo systemctl enable --now intelipump-cloud-sync.service
#
# Twin catalog (from laptop / droplet):
#   DigitalTwin/scripts/provision_sao_rs1_pump2.sh
#
set -euo pipefail

CONFIRM=0
START=0
INSTALL_SYNC=1
PORT="${INTELIPUMP_SERIAL_PORT:-/dev/ttyUSB0}"
PRICE="${INTELIPUMP_UNIT_PRICE:-1400}"
ADDRESSES="${INTELIPUMP_ADDRESSES:-1,2}"
REPO_USER="${INTELIPUMP_RUN_USER:-intelipump}"
STATION_ID="SAO-Redeemed-Station-1"
DEVICE_ID="InteliPump-SAO-RS1-pi-002"

usage() {
  cat <<'EOF'
Install InteliPump SAO RS1 physical pump 2 (authorize-on-lift, prod MQTT).

Layout on this Pi's RS-485 bus:
  DART address 1 → pump-2 / nozzle-1 (PMS)  source pump-2-n1
  DART address 2 → pump-2 / nozzle-2 (PMS)  source pump-2-n2
  Unit price default: 1400 (raw BCD)
  Device: InteliPump-SAO-RS1-pi-002
  Station: SAO-Redeemed-Station-1 (same as pump 1)

Required:
  --confirm-sao-authorize-install   Acknowledge sole-controller authorize install

Optional:
  --start                       enable --now controller + cloud-sync after install
  --no-cloud-sync               skip cloud-sync unit/env install
  --port PATH                   Serial device (default /dev/ttyUSB0)
  --price N                     Raw BCD unit price (default 1400)
  --addresses LIST              Pump addresses (default 1,2)
  --user NAME                   Service user (default intelipump)
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --confirm-sao-authorize-install) CONFIRM=1 ;;
    --start) START=1 ;;
    --no-cloud-sync) INSTALL_SYNC=0 ;;
    --port)
      PORT="${2:?--port requires a path}"
      shift
      ;;
    --price)
      PRICE="${2:?--price requires an integer}"
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

UNIT_SRC="${REPO_ROOT}/deploy/systemd/intelipump.sao-rs1-pump2.service"
ENV_SRC="${REPO_ROOT}/deploy/systemd/intelipump.sao-rs1-pump2.env.example"
SYNC_UNIT_SRC="${REPO_ROOT}/deploy/systemd/intelipump-cloud-sync.sao-rs1-pump2.service"
SYNC_ENV_SRC="${REPO_ROOT}/deploy/systemd/intelipump-cloud-sync.sao-rs1-pump2.env.example"
MAP_SRC="${REPO_ROOT}/config/channel_map.sao-rs1-pump2.json"

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

echo "==> SAO Redeemed Station 1 — physical pump 2 install"
echo "==> Repo: ${REPO_ROOT}"
echo "==> Station: ${STATION_ID}  device: ${DEVICE_ID}"
echo "==> Port: ${PORT}  addresses: ${ADDRESSES}  price: ${PRICE}  user: ${REPO_USER}"
echo "==> Channel map: ${MAP_SRC}"
echo "WARN: This Pi becomes the only RS-485 master for pump 2 on THIS bus."
echo "WARN: Do not share the serial bus with InteliPump-SAO-RS1-pi-001."

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

echo "==> Writing ${ENV_DST}"
TMP_ENV="$(mktemp)"
sed \
  -e "s|/home/intelipump/intelipump/intelipump-fdc|${REPO_ROOT}|g" \
  -e "s|^INTELIPUMP_DART__SERIAL_PORT=.*|INTELIPUMP_DART__SERIAL_PORT=${PORT}|" \
  -e "s|^INTELIPUMP_CONTROLLER__DEVICE_ID=.*|INTELIPUMP_CONTROLLER__DEVICE_ID=${DEVICE_ID}|" \
  -e "s|^INTELIPUMP_CONTROLLER__STATION_ID=.*|INTELIPUMP_CONTROLLER__STATION_ID=${STATION_ID}|" \
  "$ENV_SRC" >"$TMP_ENV"
sudo install -o root -g root -m 0644 "$TMP_ENV" "$ENV_DST"
rm -f "$TMP_ENV"

echo "==> Writing ${UNIT_DST}"
TMP_UNIT="$(mktemp)"
sed \
  -e "s|/home/intelipump/intelipump/intelipump-fdc|${REPO_ROOT}|g" \
  -e "s|User=intelipump|User=${REPO_USER}|g" \
  -e "s|Group=intelipump|Group=${REPO_USER}|g" \
  -e "s|--port /dev/ttyUSB0|--port ${PORT}|g" \
  -e "s|--addresses 1,2|--addresses ${ADDRESSES}|g" \
  -e "s|--price 1400|--price ${PRICE}|g" \
  -e "s|InteliPump-SAO-RS1-pi-002|${DEVICE_ID}|g" \
  -e "s|SAO-Redeemed-Station-1|${STATION_ID}|g" \
  "$UNIT_SRC" >"$TMP_UNIT"

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
  sed \
    -e "s|/home/intelipump/intelipump/intelipump-fdc|${REPO_ROOT}|g" \
    -e "s|InteliPump-SAO-RS1-pi-002|${DEVICE_ID}|g" \
    -e "s|SAO-Redeemed-Station-1|${STATION_ID}|g" \
    "$SYNC_ENV_SRC" >"$TMP_SYNC_ENV"
  if [[ -n "$EXISTING_PW" ]]; then
    sed -i "s|^INTELIPUMP_MQTT__PASSWORD=.*|INTELIPUMP_MQTT__PASSWORD=${EXISTING_PW}|" "$TMP_SYNC_ENV"
  fi
  sudo install -o root -g root -m 0640 "$TMP_SYNC_ENV" "$SYNC_ENV_DST"
  rm -f "$TMP_SYNC_ENV"

  echo "==> Writing ${SYNC_UNIT_DST}"
  TMP_SYNC_UNIT="$(mktemp)"
  sed \
    -e "s|/home/intelipump/intelipump/intelipump-fdc|${REPO_ROOT}|g" \
    -e "s|User=intelipump|User=${REPO_USER}|g" \
    -e "s|Group=intelipump|Group=${REPO_USER}|g" \
    -e "s|InteliPump-SAO-RS1-pi-002|${DEVICE_ID}|g" \
    -e "s|SAO-Redeemed-Station-1|${STATION_ID}|g" \
    "$SYNC_UNIT_SRC" >"$TMP_SYNC_UNIT"
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
  echo "  journalctl -u intelipump -f"
fi

echo
echo "Done."
echo "Topics: intelipump/prod/stations/${STATION_ID}/transactions"
echo "Dashboard: provision pump-2 catalog (DigitalTwin/scripts/provision_sao_rs1_pump2.sh)"
echo "One master per bus: do not also run uv run intelipump-controller by hand."
