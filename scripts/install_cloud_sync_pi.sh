#!/usr/bin/env bash
# Install intelipump-cloud-sync.service on the owned-lab Pi.
# Does not change intelipump.service (pump control stays MQTT-off).
#
#   ./scripts/install_cloud_sync_pi.sh --confirm-cloud-sync-install
#   # edit /etc/intelipump/intelipump-cloud-sync.env (MQTT password)
#   sudo systemctl enable --now intelipump-cloud-sync.service
set -euo pipefail

CONFIRM=0
START=0
REPO_USER="${INTELIPUMP_RUN_USER:-intelipump}"
SERVICE_NAME="intelipump-cloud-sync.service"

usage() {
  cat <<'EOF'
Install the publish-only cloud sync sidecar.

Required:
  --confirm-cloud-sync-install   Acknowledge publish-only MQTT sidecar install

Optional:
  --start                        systemctl enable --now after install
  --user NAME                    Service user (default intelipump)
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --confirm-cloud-sync-install) CONFIRM=1 ;;
    --start) START=1 ;;
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
  echo "Refused: pass --confirm-cloud-sync-install." >&2
  usage >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

UNIT_SRC="${REPO_ROOT}/deploy/systemd/intelipump-cloud-sync.service"
ENV_SRC="${REPO_ROOT}/deploy/systemd/intelipump-cloud-sync.env.example"
UNIT_DST="/etc/systemd/system/${SERVICE_NAME}"
ENV_DST="/etc/intelipump/intelipump-cloud-sync.env"
VENV_BIN="${REPO_ROOT}/.venv/bin/intelipump-cloud-sync"
ETC_DIR="/etc/intelipump"

if [[ ! -f "$UNIT_SRC" || ! -f "$ENV_SRC" ]]; then
  echo "Missing deploy files under ${REPO_ROOT}/deploy/systemd/" >&2
  exit 1
fi

if [[ ! -x "$VENV_BIN" ]]; then
  echo "Missing ${VENV_BIN}. Run uv sync in ${REPO_ROOT} first." >&2
  exit 1
fi

echo "==> Repo: ${REPO_ROOT}"
echo "==> User: ${REPO_USER}"
echo "==> Controller unit is not modified."

sudo install -d -o root -g root -m 0755 "$ETC_DIR"

MAP_SRC="${REPO_ROOT}/config/channel_map.us-lab.json"
MAP_DST="${REPO_ROOT}/config/channel_map.us-lab.json"
if [[ -f "$MAP_SRC" ]]; then
  echo "==> Channel map present: ${MAP_SRC}"
else
  echo "ERROR: missing ${MAP_SRC}" >&2
  exit 1
fi

if [[ ! -f "$ENV_DST" ]]; then
  echo "==> Writing ${ENV_DST} from example"
  TMP_ENV="$(mktemp)"
  sed -e "s|/home/intelipump/intelipump/intelipump-fdc|${REPO_ROOT}|g" "$ENV_SRC" >"$TMP_ENV"
  sudo install -o root -g root -m 0640 "$TMP_ENV" "$ENV_DST"
  rm -f "$TMP_ENV"
else
  echo "==> Keeping existing ${ENV_DST}"
  if ! sudo grep -q '^INTELIPUMP_CHANNEL_MAP_PATH=' "$ENV_DST" 2>/dev/null; then
    echo "==> Adding INTELIPUMP_CHANNEL_MAP_PATH=${MAP_DST}"
    echo "INTELIPUMP_CHANNEL_MAP_PATH=${MAP_DST}" | sudo tee -a "$ENV_DST" >/dev/null
  fi
fi

TMP_UNIT="$(mktemp)"
sed \
  -e "s|/home/intelipump/intelipump/intelipump-fdc|${REPO_ROOT}|g" \
  -e "s|User=intelipump|User=${REPO_USER}|g" \
  -e "s|Group=intelipump|Group=${REPO_USER}|g" \
  "$UNIT_SRC" >"$TMP_UNIT"
sudo install -o root -g root -m 0644 "$TMP_UNIT" "$UNIT_DST"
rm -f "$TMP_UNIT"

echo "==> systemctl daemon-reload"
sudo systemctl daemon-reload

"$VENV_BIN" --help >/dev/null
echo "OK: ${VENV_BIN}"

if grep -q '^INTELIPUMP_MQTT__PASSWORD=$' "$ENV_DST" 2>/dev/null \
  || grep -q '^INTELIPUMP_MQTT__PASSWORD=$' <(sudo cat "$ENV_DST"); then
  echo "WARN: set INTELIPUMP_MQTT__PASSWORD in ${ENV_DST} before starting." >&2
fi

if [[ "$START" -eq 1 ]]; then
  echo "==> enable --now ${SERVICE_NAME}"
  sudo systemctl enable --now "$SERVICE_NAME"
  sleep 1
  sudo systemctl --no-pager --full status "$SERVICE_NAME" || true
  sudo journalctl -u intelipump-cloud-sync -n 40 --no-pager || true
else
  echo
  echo "Installed but not started. After setting the MQTT password:"
  echo "  sudo nano /etc/intelipump/intelipump-cloud-sync.env"
  echo "  sudo systemctl enable --now ${SERVICE_NAME}"
  echo "  journalctl -u intelipump-cloud-sync -f"
fi

echo
echo "Done. intelipump.service is unchanged (MQTT stays off on the controller)."
