#!/usr/bin/env bash
# Enable dashboard "Read now" (hardware CD101) on one SAO pump Pi.
#
# Run ON the Pi from the intelipump-fdc checkout that systemd uses, e.g.:
#   cd ~/intelipump-fdc/intelipump   # or ~/intelipump/intelipump-fdc
#   ./scripts/setup_sao_meter_reading_pi.sh --pump 5
#   ./scripts/setup_sao_meter_reading_pi.sh --pump 6 --swap-nozzles   # if face check needs swap
#
# What it does:
#   1) Writes/updates config/channel_map.sao-rs1-pumpN.json
#   2) Upserts meter-reading + command-subscription keys into
#        /etc/intelipump/intelipump.env
#        /etc/intelipump/intelipump-cloud-sync.env
#   3) Optionally clears stuck local PENDING_CONTROLLER rows (max_pending=2)
#   4) Restarts intelipump + intelipump-cloud-sync
#
# Does NOT open RS-485 itself. Does NOT invent zeros.
# After setup: re-seat nozzle (fresh NOZIO IN) → Read now within ~30s.
#
set -euo pipefail

PUMP_NUM=""
SWAP_NOZZLES=0
CLEAR_PENDING=1
DO_RESTART=1
DRY_RUN=0
ADDRESSES="${INTELIPUMP_ADDRESSES:-1,2}"
PRODUCT="${INTELIPUMP_PRODUCT:-PMS}"
VOLUME_DECIMALS="${INTELIPUMP_METER_VOLUME_DECIMALS:-3}"
MIN_INTERVAL="${INTELIPUMP_METER_MIN_INTERVAL_SECONDS:-60}"
NOZZLE_MAX_AGE="${INTELIPUMP_METER_NOZZLE_IN_MAX_AGE_SECONDS:-30}"
CONTROLLER_ENV="/etc/intelipump/intelipump.env"
SYNC_ENV="/etc/intelipump/intelipump-cloud-sync.env"
DB_PATH="${INTELIPUMP_DB_PATH:-/var/lib/intelipump/intelipump.db}"

usage() {
  cat <<'EOF'
Enable SAO dashboard meter Read now (HARDWARE_CD101) on this Pi.

Required:
  --pump N                 Physical pump number (1..12)

Optional:
  --swap-nozzles           Map DART 1→nozzle-2, DART 2→nozzle-1
                           (default: 1→nozzle-1, 2→nozzle-2)
  --addresses LIST         DART addresses (default 1,2)
  --product CODE           PMS (default) or AGO
  --no-clear-pending       skip clearing stuck PENDING_CONTROLLER rows
  --no-restart             write config only; do not restart services
  --dry-run                print actions; write nothing
  -h, --help

Prereqs:
  - intelipump.service + intelipump-cloud-sync.service already installed
  - DEVICE_ID already set in /etc/intelipump/*.env (e.g. InteliPump-SAO-RS1-pi-005)
  - tree on disk includes meter-reading file bridge (command_intake + controller)

Operator after setup:
  1) Brief nozzle re-seat (OUT→IN) for a fresh NOZIO
  2) Dashboard Read now within ~30s
  3) journalctl -u intelipump -u intelipump-cloud-sync -f
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pump)
      PUMP_NUM="${2:?--pump requires a number}"
      shift
      ;;
    --swap-nozzles) SWAP_NOZZLES=1 ;;
    --addresses)
      ADDRESSES="${2:?}"
      shift
      ;;
    --product)
      PRODUCT="${2:?}"
      shift
      ;;
    --no-clear-pending) CLEAR_PENDING=0 ;;
    --no-restart) DO_RESTART=0 ;;
    --dry-run) DRY_RUN=1 ;;
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PUMP_ID="pump-${PUMP_NUM}"
MAP_NAME="channel_map.sao-rs1-pump${PUMP_NUM}.json"
MAP_PATH="${REPO_ROOT}/config/${MAP_NAME}"

# Prefer DEVICE_ID already on the Pi (controller env, then sync env).
read_env_val() {
  local file="$1" key="$2"
  [[ -f "$file" ]] || return 0
  # shellcheck disable=SC2002
  grep -E "^${key}=" "$file" 2>/dev/null | tail -1 | cut -d= -f2- | tr -d '\r' || true
}

DEVICE_ID="$(read_env_val "$CONTROLLER_ENV" INTELIPUMP_CONTROLLER__DEVICE_ID)"
if [[ -z "$DEVICE_ID" ]]; then
  DEVICE_ID="$(read_env_val "$SYNC_ENV" INTELIPUMP_CONTROLLER__DEVICE_ID)"
fi
if [[ -z "$DEVICE_ID" ]]; then
  # Fallback: zero-pad pump number (pi-006 for pump 6) — verify before relying on it.
  printf -v DEVICE_ID 'InteliPump-SAO-RS1-pi-%03d' "$PUMP_NUM"
  echo "WARN: DEVICE_ID not found in env files; using fallback ${DEVICE_ID}" >&2
  echo "      Confirm this matches the Pi before Read now." >&2
fi

upsert_env() {
  local file="$1" key="$2" value="$3"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] ${file}: ${key}=${value}"
    return 0
  fi
  sudo mkdir -p "$(dirname "$file")"
  if [[ ! -f "$file" ]]; then
    echo "Refused: missing ${file}. Install controller/cloud-sync first." >&2
    exit 2
  fi
  if grep -qE "^${key}=" "$file"; then
    sudo sed -i "s|^${key}=.*|${key}=${value}|" "$file"
  else
    printf '%s=%s\n' "$key" "$value" | sudo tee -a "$file" >/dev/null
  fi
}

write_channel_map() {
  local n1_addr=1 n2_addr=2
  if [[ "$SWAP_NOZZLES" -eq 1 ]]; then
    n1_addr=2
    n2_addr=1
  fi
  local body
  body="$(cat <<EOF
{
  "${n1_addr}": {
    "pump_id": "${PUMP_ID}",
    "nozzle_id": "nozzle-1",
    "source_identifier": "${PUMP_ID}-n1",
    "product": "${PRODUCT}"
  },
  "${n2_addr}": {
    "pump_id": "${PUMP_ID}",
    "nozzle_id": "nozzle-2",
    "source_identifier": "${PUMP_ID}-n2",
    "product": "${PRODUCT}"
  }
}
EOF
)"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] would write ${MAP_PATH}:"
    echo "$body"
    return 0
  fi
  mkdir -p "$(dirname "$MAP_PATH")"
  printf '%s\n' "$body" >"$MAP_PATH"
  echo "Wrote ${MAP_PATH}"
}

apply_meter_keys() {
  local file="$1"
  upsert_env "$file" "INTELIPUMP_CHANNEL_MAP_PATH" "$MAP_PATH"
  upsert_env "$file" "INTELIPUMP_METER_READING__HARDWARE_CD101" "true"
  upsert_env "$file" "INTELIPUMP_METER_READING__ALLOWED_DEVICE_ID" "$DEVICE_ID"
  upsert_env "$file" "INTELIPUMP_METER_READING__ALLOWED_ADDRESSES" "$ADDRESSES"
  upsert_env "$file" "INTELIPUMP_METER_READING__COUNTER_SELECT" "1"
  upsert_env "$file" "INTELIPUMP_METER_READING__VOLUME_DECIMALS" "$VOLUME_DECIMALS"
  upsert_env "$file" "INTELIPUMP_METER_READING__MIN_INTERVAL_SECONDS" "$MIN_INTERVAL"
  upsert_env "$file" "INTELIPUMP_METER_READING__NOZZLE_IN_MAX_AGE_SECONDS" "$NOZZLE_MAX_AGE"
  upsert_env "$file" "INTELIPUMP_METER_READING__RESPONSE_TIMEOUT_SECONDS" "8"
  # Morning auto OPENING when Pi boots (controller). No re-seat ritual.
  upsert_env "$file" "INTELIPUMP_METER_READING__STARTUP_CAPTURE_ENABLED" "true"
  upsert_env "$file" "INTELIPUMP_METER_READING__STARTUP_CAPTURE_TIMEZONE" "Africa/Lagos"
  upsert_env "$file" "INTELIPUMP_METER_READING__STARTUP_CAPTURE_SETTLE_SECONDS" "20"
  upsert_env "$file" "INTELIPUMP_METER_READING__STARTUP_CAPTURE_WINDOW_SECONDS" "1800"
}

clear_pending() {
  if [[ "$CLEAR_PENDING" -ne 1 ]]; then
    return 0
  fi
  if [[ ! -f "$DB_PATH" ]]; then
    echo "WARN: DB not found at ${DB_PATH}; skip pending clear" >&2
    return 0
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] clear PENDING/PENDING_CONTROLLER for ${PUMP_ID} in ${DB_PATH}"
    return 0
  fi
  python3 - <<PY
import sqlite3
path = ${DB_PATH@Q}
pump = ${PUMP_ID@Q}
c = sqlite3.connect(path)
before = list(c.execute(
    "SELECT status, count(*) FROM meter_readings WHERE pump_id=? GROUP BY status",
    (pump,),
))
print("before:", before)
cur = c.execute(
    "UPDATE meter_readings SET status='SUPERSEDED' "
    "WHERE pump_id=? AND status IN ('PENDING','PENDING_CONTROLLER')",
    (pump,),
)
c.commit()
print("cleared", cur.rowcount)
after = list(c.execute(
    "SELECT status, count(*) FROM meter_readings WHERE pump_id=? GROUP BY status",
    (pump,),
))
print("after:", after)
PY
}

echo "=== SAO meter-reading setup ==="
echo "repo:       ${REPO_ROOT}"
echo "pump:       ${PUMP_ID}"
echo "device_id:  ${DEVICE_ID}"
echo "addresses:  ${ADDRESSES}"
echo "map:        ${MAP_PATH}"
echo "swap:       ${SWAP_NOZZLES}"
echo "decimals:   ${VOLUME_DECIMALS}"
echo

# Sanity: file-bridge symbols present in this tree
if [[ ! -f "${REPO_ROOT}/src/intelipump_fdc/cloud/command_intake.py" ]]; then
  echo "Refused: ${REPO_ROOT} does not look like intelipump-fdc (missing command_intake.py)." >&2
  exit 2
fi
if ! grep -q 'hardware_read_meter' "${REPO_ROOT}/src/intelipump_fdc/cloud/command_intake.py"; then
  echo "WARN: command_intake.py has no hardware_read_meter — pull a meter-reading pin first." >&2
fi

write_channel_map

echo "Updating ${CONTROLLER_ENV}"
apply_meter_keys "$CONTROLLER_ENV"

echo "Updating ${SYNC_ENV}"
apply_meter_keys "$SYNC_ENV"
# Dashboard Read now needs command subscription on cloud-sync only.
upsert_env "$SYNC_ENV" "INTELIPUMP_MQTT__COMMAND_SUBSCRIPTION_ENABLED" "true"

clear_pending

if [[ "$DO_RESTART" -eq 1 ]]; then
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "[dry-run] systemctl restart intelipump.service intelipump-cloud-sync.service"
  else
    echo "Restarting services..."
    sudo systemctl restart intelipump.service intelipump-cloud-sync.service
    sleep 2
    systemctl is-active intelipump.service intelipump-cloud-sync.service
  fi
fi

echo
echo "=== Verify ==="
echo "systemctl cat intelipump-cloud-sync.service | grep -E 'ExecStart|WorkingDirectory'"
echo "grep -E 'HARDWARE_CD101|ALLOWED_DEVICE|COMMAND_SUBSCRIPTION|CHANNEL_MAP' ${SYNC_ENV}"
echo "cat ${MAP_PATH}"
echo "sudo journalctl -u intelipump -u intelipump-cloud-sync -f"
echo
echo "Dashboard: Admin → Meter reading → pump ${PUMP_NUM} → re-seat → Read now (<30s)"
echo "Done."
