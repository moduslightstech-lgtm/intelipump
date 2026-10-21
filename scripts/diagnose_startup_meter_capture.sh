#!/usr/bin/env bash
# Diagnose once-per-morning OPENING meter capture on this Pi.
# Run on the pump Pi (e.g. intelipump-3):
#   ./scripts/diagnose_startup_meter_capture.sh
set -euo pipefail

ENV_FILE="${INTELIPUMP_ENV:-/etc/intelipump/intelipump.env}"
MARKER="${INTELIPUMP_METER_READ_REQUEST_DIR:-/var/lib/intelipump}/meter-startup-capture.json"
UNIT="${INTELIPUMP_UNIT:-intelipump.service}"

echo "===== hostname / uptime ====="
hostname
uptime
systemctl show "$UNIT" -p ActiveEnterTimestamp -p WorkingDirectory --no-pager 2>/dev/null || true

echo
echo "===== meter / startup env ====="
if [[ -f "$ENV_FILE" ]]; then
  grep -E 'DEVICE_ID|HARDWARE_CD101|ALLOWED_ADDRESSES|STARTUP_CAPTURE|CHANNEL_MAP|VOLUME_DECIMALS|COUNTER_SELECT' \
    "$ENV_FILE" || echo "(no meter keys found)"
else
  echo "MISSING $ENV_FILE"
fi

echo
echo "===== startup marker ====="
if [[ -f "$MARKER" ]]; then
  cat "$MARKER"
else
  echo "MISSING $MARKER (never captured / never attempted)"
fi

echo
echo "===== local meter_readings OPENING (last 15) ====="
PY="${INTELIPUMP_PYTHON:-}"
if [[ -z "$PY" ]]; then
  WD="$(systemctl show "$UNIT" -p WorkingDirectory --value 2>/dev/null || true)"
  if [[ -n "$WD" && -x "$WD/.venv/bin/python" ]]; then
    PY="$WD/.venv/bin/python"
  elif [[ -x "$HOME/intelipump-fdc/intelipump/.venv/bin/python" ]]; then
    PY="$HOME/intelipump-fdc/intelipump/.venv/bin/python"
  else
    PY="python3"
  fi
fi
DB="${INTELIPUMP_DB_PATH:-/var/lib/intelipump/intelipump.db}"
"$PY" - <<PY
from sqlalchemy import create_engine, text, inspect
e = create_engine("sqlite:///${DB}")
insp = inspect(e)
if "meter_readings" not in insp.get_table_names():
    print("no meter_readings table")
else:
    cols = {c["name"] for c in insp.get_columns("meter_readings")}
    # tolerate schema variants
    time_col = "captured_at" if "captured_at" in cols else "created_at"
    q = text(f"""
      SELECT nozzle_id, {time_col} AS captured_at, volume_liters, slot, source, status
      FROM meter_readings
      WHERE UPPER(COALESCE(slot,'')) = 'OPENING'
      ORDER BY {time_col} DESC
      LIMIT 15
    """)
    with e.connect() as c:
        rows = list(c.execute(q).mappings())
    for r in rows:
        print(dict(r))
    if not rows:
        print("(no OPENING rows)")
PY

echo
echo "===== recent startup capture journal (today) ====="
journalctl -u "$UNIT" --since today --no-pager 2>/dev/null \
  | grep -E 'meter_startup_capture_|\[METER-READ\] startup OPENING' \
  | tail -80 || echo "(no matches / empty journal)"

echo
echo "===== boot window hint ====="
echo "If you see meter_startup_capture_window_closed and NO meter_startup_capture_new_business_day"
echo "after midnight, deploy the business-day rollover fix and restart $UNIT."
