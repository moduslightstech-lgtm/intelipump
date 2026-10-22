#!/usr/bin/env bash
# Local CD101 meter read for troubleshooting — does NOT publish to the cloud
# dashboard. Uses the running intelipump controller (same safety gates).
#
# On the pump Pi (idle nozzles IN):
#   cd "$(systemctl show intelipump -p WorkingDirectory --value)"
#   ./scripts/pi_meter_read_local.sh              # both allowlisted addresses
#   ./scripts/pi_meter_read_local.sh 1            # address 1 only
#   ./scripts/pi_meter_read_local.sh 1 2 --with-recent-sales
#
# Loads the same EnvironmentFile(s) as intelipump.service so HARDWARE_CD101
# and allowlists match the running controller.
#
# Morning STARTUP_OPENING / dashboard Read now are unchanged.
set -euo pipefail

WD="$(systemctl show intelipump.service -p WorkingDirectory --value 2>/dev/null || true)"
if [[ -z "$WD" || ! -d "$WD" ]]; then
  WD="${INTELIPUMP_REMOTE_DIR:-$HOME/intelipump-fdc/intelipump}"
fi
cd "$WD"

# --- load controller env (CLI otherwise defaults HARDWARE_CD101=false) ---
_load_env_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  echo "==> sourcing $f"
  set -a
  # shellcheck disable=SC1090
  source "$f"
  set +a
}

# Prefer unit EnvironmentFiles (handles drop-ins).
ENV_FILES="$(systemctl show intelipump.service -p EnvironmentFiles --value 2>/dev/null || true)"
if [[ -n "$ENV_FILES" ]]; then
  # Format: /path/to/file (ignore_errors=yes)  — one per line or space-ish
  while read -r token; do
    [[ -z "$token" || "$token" == "(null)" ]] && continue
    # strip optional (ignore_errors=...)
    f="${token%% *}"
    f="${f#(}"
    _load_env_file "$f"
  done <<<"$(echo "$ENV_FILES" | tr ' ' '\n')"
fi
# Fallbacks used on SAO Pis
_load_env_file "${INTELIPUMP_ENV:-/etc/intelipump/intelipump.env}"
_load_env_file /etc/intelipump/intelipump-cloud-sync.env

if [[ "${INTELIPUMP_METER_READING__HARDWARE_CD101:-}" != "true" \
   && "${INTELIPUMP_METER_READING__HARDWARE_CD101:-}" != "True" \
   && "${INTELIPUMP_METER_READING__HARDWARE_CD101:-}" != "1" ]]; then
  echo "REFUSED: INTELIPUMP_METER_READING__HARDWARE_CD101 is not true in shell env." >&2
  echo "  Check: grep HARDWARE_CD101 /etc/intelipump/intelipump.env" >&2
  echo "  Unit:  systemctl show intelipump -p EnvironmentFiles --value" >&2
  exit 2
fi

EXTRA_LOCAL=()
if [[ -x "$WD/.venv/bin/intelipump-meter-read-local" ]]; then
  BIN="$WD/.venv/bin/intelipump-meter-read-local"
elif [[ -x "$WD/.venv/bin/intelipump-meter-read-once" ]]; then
  BIN="$WD/.venv/bin/intelipump-meter-read-once"
  EXTRA_LOCAL=(--local-only)
else
  echo "Missing venv meter-read binary under $WD/.venv/bin" >&2
  echo "Install/sync package first, or: pip install -e . in the repo." >&2
  exit 1
fi

ADDRS=()
EXTRA=()
for arg in "$@"; do
  case "$arg" in
    --with-recent-sales|--sales-limit=*) EXTRA+=("$arg") ;;
    --*) EXTRA+=("$arg") ;;
    *)
      if [[ "$arg" =~ ^[0-9]+$ ]]; then
        ADDRS+=("$arg")
      else
        EXTRA+=("$arg")
      fi
      ;;
  esac
done

COMMON=(--wait-seconds "${WAIT_SECONDS:-25}" --gap-seconds "${GAP_SECONDS:-2}")
if [[ ${#ADDRS[@]} -eq 0 ]]; then
  set -- --all-allowed "${COMMON[@]}" "${EXTRA_LOCAL[@]}" "${EXTRA[@]}"
else
  joined=$(IFS=,; echo "${ADDRS[*]}")
  set -- --addresses "$joined" "${COMMON[@]}" "${EXTRA_LOCAL[@]}" "${EXTRA[@]}"
fi

echo "==> local meter read (no cloud) cwd=$WD"
echo "    device=${INTELIPUMP_CONTROLLER__DEVICE_ID:-?} HARDWARE_CD101=${INTELIPUMP_METER_READING__HARDWARE_CD101}"
echo "    $BIN $*"
exec "$BIN" "$@"
