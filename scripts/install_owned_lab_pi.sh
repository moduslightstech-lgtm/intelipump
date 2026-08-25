#!/usr/bin/env bash
# Install InteliPump FDC on an owned-lab Raspberry Pi (after clone).
#
# OWNED LAB ONLY: enables BENCH_CONTROL + authorize-on-lift via systemd.
# Remote authorization / MQTT stay off. One RS-485 master only.
#
# Usage (from repo root or scripts/):
#   ./scripts/install_owned_lab_pi.sh --confirm-owned-lab-install
#   ./scripts/install_owned_lab_pi.sh --confirm-owned-lab-install --start
#   ./scripts/install_owned_lab_pi.sh --confirm-owned-lab-install --port /dev/ttyUSB0 --start
#
set -euo pipefail

CONFIRM=0
START=0
PORT="${INTELIPUMP_SERIAL_PORT:-/dev/ttyUSB0}"
PRICE="${INTELIPUMP_UNIT_PRICE:-1175}"
ADDRESSES="${INTELIPUMP_ADDRESSES:-1,2}"
REPO_USER="${INTELIPUMP_RUN_USER:-intelipump}"
SERVICE_NAME="intelipump.service"

usage() {
  cat <<'EOF'
Install InteliPump owned-lab controller on this Pi.

Required:
  --confirm-owned-lab-install   Acknowledge OWNED LAB active dispense install

Optional:
  --start                       systemctl enable --now after install
  --port PATH                   Serial device (default /dev/ttyUSB0)
  --price N                     Raw BCD unit price (default 1175)
  --addresses LIST              Pump addresses (default 1,2)
  --user NAME                   Service user (default intelipump)
  -h, --help                    Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --confirm-owned-lab-install) CONFIRM=1 ;;
    --start) START=1 ;;
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
  echo "Refused: pass --confirm-owned-lab-install (owned-lab active dispense)." >&2
  usage >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

UNIT_SRC="${REPO_ROOT}/deploy/systemd/intelipump.service"
ENV_SRC="${REPO_ROOT}/deploy/systemd/intelipump.env.example"
UNIT_DST="/etc/systemd/system/${SERVICE_NAME}"
ENV_DST="/etc/intelipump/intelipump.env"
VENV_BIN="${REPO_ROOT}/.venv/bin/intelipump-controller"
DB_DIR="/var/lib/intelipump"
ETC_DIR="/etc/intelipump"

if [[ ! -f "$UNIT_SRC" || ! -f "$ENV_SRC" ]]; then
  echo "Missing deploy files under ${REPO_ROOT}/deploy/systemd/" >&2
  exit 1
fi

echo "==> Repo: ${REPO_ROOT}"
echo "==> Port: ${PORT}  addresses: ${ADDRESSES}  price: ${PRICE}  user: ${REPO_USER}"

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

echo "==> Writing ${ENV_DST} (overwrite)"
TMP_ENV="$(mktemp)"
sed \
  -e "s|^INTELIPUMP_DART__SERIAL_PORT=.*|INTELIPUMP_DART__SERIAL_PORT=${PORT}|" \
  -e "s|^INTELIPUMP_CONTROLLER__DEVICE_ID=.*|INTELIPUMP_CONTROLLER__DEVICE_ID=InteliPump-Lab-$(hostname -s)-001|" \
  "$ENV_SRC" >"$TMP_ENV"
sudo install -o root -g root -m 0644 "$TMP_ENV" "$ENV_DST"
rm -f "$TMP_ENV"

echo "==> Writing ${UNIT_DST}"
TMP_UNIT="$(mktemp)"
# Rewrite paths for this clone; keep owned-lab ExecStart flags.
sed \
  -e "s|/home/intelipump/intelipump/intelipump-fdc|${REPO_ROOT}|g" \
  -e "s|User=intelipump|User=${REPO_USER}|g" \
  -e "s|Group=intelipump|Group=${REPO_USER}|g" \
  -e "s|--port /dev/ttyUSB0|--port ${PORT}|g" \
  -e "s|--addresses 1,2|--addresses ${ADDRESSES}|g" \
  -e "s|--price 1175|--price ${PRICE}|g" \
  "$UNIT_SRC" >"$TMP_UNIT"

# Prefer concrete ttyUSB device unit only when using /dev/ttyUSB0.
if [[ "$PORT" != "/dev/ttyUSB0" ]]; then
  sed -i \
    -e '/^After=dev-ttyUSB0\.device$/d' \
    -e '/^Wants=dev-ttyUSB0\.device$/d' \
    "$TMP_UNIT"
fi

sudo install -o root -g root -m 0644 "$TMP_UNIT" "$UNIT_DST"
rm -f "$TMP_UNIT"

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

if [[ "$START" -eq 1 ]]; then
  echo "==> enable --now ${SERVICE_NAME}"
  sudo systemctl enable --now "$SERVICE_NAME"
  sleep 1
  sudo systemctl --no-pager --full status "$SERVICE_NAME" || true
  echo
  echo "Recent logs:"
  sudo journalctl -u intelipump -n 40 --no-pager || true
else
  echo
  echo "Installed but not started. When ready:"
  echo "  sudo systemctl enable --now ${SERVICE_NAME}"
  echo "  journalctl -u intelipump -f"
fi

echo
echo "Done."
echo "Note: if this shell was opened before dialout membership, re-login or reboot once."
echo "One master only: do not also run uv run intelipump-controller by hand."
