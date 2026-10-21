#!/usr/bin/env bash
# After DB recover: re-touch meter-read-result*.json so cloud-sync republishes
# OPENING / meter readings without restarting the hose controller.
#
#   sudo ./scripts/pi_sqlite_republish_meter_results.sh
set -euo pipefail

DIR="${INTELIPUMP_METER_READ_REQUEST_DIR:-/var/lib/intelipump}"
UNIT="${INTELIPUMP_CLOUD_SYNC_UNIT:-intelipump-cloud-sync.service}"

shopt -s nullglob
files=("${DIR}"/meter-read-result*.json)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "No meter-read-result*.json under ${DIR}" >&2
  exit 1
fi

echo "=== touching ${#files[@]} result file(s) ==="
for f in "${files[@]}"; do
  touch "$f"
  echo "  $f"
done

echo "=== restart ${UNIT} (controller untouched) ==="
systemctl restart "$UNIT"
sleep 1
systemctl --no-pager --full status "$UNIT" || true
echo "Done. Watch: journalctl -u ${UNIT} -f | grep -i meter"
