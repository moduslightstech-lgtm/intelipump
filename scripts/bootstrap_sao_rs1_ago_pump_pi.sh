#!/usr/bin/env bash
# One-shot SAO AGO pump Pi setup (single AGO dispenser at the station).
#
# Defaults for SAO Redeemed Station 1 AGO island:
#   --pump 8 --product AGO --price 1875
#
# Usage (on the Pi):
#   cd ~/intelipump-fdc/intelipump
#   ./scripts/bootstrap_sao_rs1_ago_pump_pi.sh --mqtt-password 'SECRET'
#   ./scripts/bootstrap_sao_rs1_ago_pump_pi.sh --pump 8 --mqtt-password 'SECRET'
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOTSTRAP="${SCRIPT_DIR}/bootstrap_sao_rs1_pump_pi.sh"

if [[ ! -x "$BOOTSTRAP" ]]; then
  echo "ERROR: missing ${BOOTSTRAP}" >&2
  exit 1
fi

PUMP_NUM=8
EXTRA=()
HAS_PUMP=0
HAS_PRICE=0
HAS_PRODUCT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pump)
      PUMP_NUM="${2:?}"
      HAS_PUMP=1
      EXTRA+=(--pump "$2")
      shift
      ;;
    --price)
      HAS_PRICE=1
      EXTRA+=(--price "$2")
      shift
      ;;
    --product)
      HAS_PRODUCT=1
      EXTRA+=(--product "$2")
      shift
      ;;
    *)
      EXTRA+=("$1")
      ;;
  esac
  shift
done

ARGS=()
if [[ "$HAS_PUMP" -eq 0 ]]; then
  ARGS+=(--pump "$PUMP_NUM")
fi
if [[ "$HAS_PRODUCT" -eq 0 ]]; then
  ARGS+=(--product AGO)
fi
if [[ "$HAS_PRICE" -eq 0 ]]; then
  ARGS+=(--price 1875)
fi
ARGS+=("${EXTRA[@]}")

echo "==> SAO AGO pump Pi bootstrap (defaults: pump ${PUMP_NUM}, AGO @ 1875)"
exec "$BOOTSTRAP" "${ARGS[@]}"
