#!/usr/bin/env bash
# Deploy meter-reading + morning OPENING capture to one SAO pump Pi (from Mac).
#
# Syncs this tree → Pi, reinstalls the venv package, runs setup_sao_meter_reading_pi.sh
# (channel map, HARDWARE_CD101, command subscription, startup capture), restarts services.
#
# Usage (from intelipump-fdc on your Mac):
#   ./scripts/deploy_sao_meter_reading_to_pi.sh --pump 5 intelipump@100.x.x.x
#   ./scripts/deploy_sao_meter_reading_to_pi.sh --pump 6 intelipump@HOST --swap-nozzles
#   ./scripts/deploy_sao_meter_reading_to_pi.sh --pump 3 intelipump@HOST --dry-run
#
# Optional:
#   INTELIPUMP_REMOTE_DIR=/home/intelipump/intelipump-fdc/intelipump
#   (default: auto-detect from systemd WorkingDirectory, else ~/intelipump-fdc/intelipump)
#
set -euo pipefail

PUMP_NUM=""
HOST=""
SWAP_FLAG=()
DRY_RUN=0
SKIP_SYNC=0
SKIP_UV=0

usage() {
  cat <<'EOF'
Deploy SAO meter reading (Read now + morning OPENING) to one pump Pi.

Required:
  --pump N           Physical pump number (1..12)
  user@pi-host       SSH target

Optional:
  --swap-nozzles     DART 1→nozzle-2, 2→nozzle-1 (pump-6 style)
  --skip-sync        do not rsync; only remote setup + restart
  --skip-uv          do not run uv sync on the Pi
  --dry-run          rsync skipped; remote setup --dry-run
  -h, --help

Examples:
  ./scripts/deploy_sao_meter_reading_to_pi.sh --pump 5 intelipump@100.85.77.22
  ./scripts/deploy_sao_meter_reading_to_pi.sh --pump 6 intelipump@intelipump-6 --swap-nozzles
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pump)
      PUMP_NUM="${2:?}"
      shift
      ;;
    --swap-nozzles) SWAP_FLAG=(--swap-nozzles) ;;
    --skip-sync) SKIP_SYNC=1 ;;
    --skip-uv) SKIP_UV=1 ;;
    --dry-run) DRY_RUN=1 ;;
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
PIN_SHA="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"

detect_remote_dir() {
  if [[ -n "${INTELIPUMP_REMOTE_DIR:-}" ]]; then
    echo "$INTELIPUMP_REMOTE_DIR"
    return 0
  fi
  local wd
  wd="$(ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST" \
    "systemctl show intelipump.service -p WorkingDirectory --value 2>/dev/null || true" \
    | tr -d '\r')"
  if [[ -n "$wd" && "$wd" != "/" && "$wd" != "" ]]; then
    echo "$wd"
    return 0
  fi
  # Common layouts seen in the field
  for candidate in \
    /home/intelipump/intelipump-fdc/intelipump \
    /home/intelipump/intelipump/intelipump-fdc
  do
    if ssh -o BatchMode=yes -o ConnectTimeout=10 "$HOST" "test -d '$candidate/src/intelipump_fdc'" 2>/dev/null; then
      echo "$candidate"
      return 0
    fi
  done
  echo "/home/intelipump/intelipump-fdc/intelipump"
}

REMOTE_DIR="$(detect_remote_dir)"
printf -v DEVICE_ID "InteliPump-SAO-RS1-pi-%03d" "$PUMP_NUM"

echo "==> Deploy meter-reading → ${HOST}"
echo "==> Pump ${PUMP_NUM}  device ${DEVICE_ID}"
echo "==> Remote ${REMOTE_DIR}"
echo "==> Pin    ${PIN_SHA}"
echo "==> Swap   ${SWAP_FLAG[*]:-no}"

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "==> dry-run: skip rsync / uv; remote setup --dry-run only"
  ssh "$HOST" "cd '$REMOTE_DIR' && ./scripts/setup_sao_meter_reading_pi.sh --pump '$PUMP_NUM' ${SWAP_FLAG[*]:-} --dry-run"
  exit 0
fi

if [[ "$SKIP_SYNC" -ne 1 ]]; then
  echo "==> rsync tree (excludes .venv)"
  ssh "$HOST" "mkdir -p '$REMOTE_DIR'"
  rsync -az --delete \
    --exclude '.venv' \
    --exclude '__pycache__' \
    --exclude '.pytest_cache' \
    --exclude '.git' \
    --exclude '*.pyc' \
    --exclude '.mypy_cache' \
    "${REPO_ROOT}/" "${HOST}:${REMOTE_DIR}/"
  ssh "$HOST" "printf '%s\n' '$PIN_SHA' > '${REMOTE_DIR}/.meter-reading-pin'"
fi

if [[ "$SKIP_UV" -ne 1 ]]; then
  echo "==> uv sync on Pi"
  ssh "$HOST" "bash -lc '
    set -euo pipefail
    cd \"$REMOTE_DIR\"
    export PATH=\"\$HOME/.local/bin:\$PATH\"
    if ! command -v uv >/dev/null 2>&1; then
      curl -LsSf https://astral.sh/uv/install.sh | sh
      export PATH=\"\$HOME/.local/bin:\$PATH\"
    fi
    uv sync
    test -x .venv/bin/intelipump-controller
    test -x .venv/bin/intelipump-cloud-sync
    .venv/bin/python -c \"from intelipump_fdc.controller.meter_startup_capture import business_date_today; print(\"startup_capture_ok\", business_date_today())\"
  '"
fi

echo "==> setup_sao_meter_reading_pi.sh --pump ${PUMP_NUM}"
SETUP_ARGS=(--pump "$PUMP_NUM")
if [[ ${#SWAP_FLAG[@]} -gt 0 ]]; then
  SETUP_ARGS+=("${SWAP_FLAG[@]}")
fi
ssh "$HOST" "cd '$REMOTE_DIR' && chmod +x scripts/setup_sao_meter_reading_pi.sh && ./scripts/setup_sao_meter_reading_pi.sh ${SETUP_ARGS[*]}"

echo
echo "Done. Verify on Pi:"
echo "  ssh ${HOST}"
echo "  systemctl is-active intelipump intelipump-cloud-sync"
echo "  grep -E 'HARDWARE_CD101|STARTUP_CAPTURE|COMMAND_SUBSCRIPTION|CHANNEL_MAP' /etc/intelipump/intelipump.env /etc/intelipump/intelipump-cloud-sync.env"
echo "  sudo journalctl -u intelipump -u intelipump-cloud-sync -f"
echo
echo "Morning: power Pi → expect meter_startup_capture_queued / CAPTURED OPENING on dashboard."
echo "Ad-hoc: re-seat → Admin Meter reading → Read now (<30s)."
