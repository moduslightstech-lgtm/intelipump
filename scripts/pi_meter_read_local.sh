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
# Morning STARTUP_OPENING / dashboard Read now are unchanged.
set -euo pipefail

WD="$(systemctl show intelipump.service -p WorkingDirectory --value 2>/dev/null || true)"
if [[ -z "$WD" || ! -d "$WD" ]]; then
  WD="${INTELIPUMP_REMOTE_DIR:-$HOME/intelipump-fdc/intelipump}"
fi
cd "$WD"

PY=""
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
  set -- --all-allowed "${COMMON[@]}" "${EXTRA_LOCAL[@]:-}" "${EXTRA[@]}"
else
  joined=$(IFS=,; echo "${ADDRS[*]}")
  set -- --addresses "$joined" "${COMMON[@]}" "${EXTRA_LOCAL[@]:-}" "${EXTRA[@]}"
fi

echo "==> local meter read (no cloud) cwd=$WD"
echo "    $BIN $*"
exec "$BIN" "$@"
