#!/usr/bin/env bash
# Dig Pi journals for pump-3 mid-fill multi-UUID settles (Oct 8 morning).
# Run on intelipump-3 (or ssh and pipe). No DB writes.
set -euo pipefail

UNIT="${INTELIPUMP_UNIT:-intelipump}"
GREP_CORE='sidecar|SALE|FILLING|mint|begin|settle|COMPLETED|uuid|UUID|nozzle|DC2|volume'

echo "===== cluster A 07:41–07:43 (8.75→8.86) ====="
journalctl -u "$UNIT" --since "2026-10-08 07:41:00" --until "2026-10-08 07:43:30" --no-pager \
  | grep -E "29b66603|5dc0b361|354940a7|7258571a|${GREP_CORE}" || true

echo
echo "===== cluster B 08:22–08:25 (43→54) ====="
journalctl -u "$UNIT" --since "2026-10-08 08:22:00" --until "2026-10-08 08:25:30" --no-pager \
  | grep -E "8c8b1a66|fae6df1c|1e10b85b|${GREP_CORE}" || true

echo
echo "===== cluster C 09:00–09:03 (small ticks) ====="
journalctl -u "$UNIT" --since "2026-10-08 09:00:00" --until "2026-10-08 09:03:30" --no-pager \
  | grep -E "481bd425|b66a87ac|9e80d55b|9069b185|2dda3256|${GREP_CORE}" || true
