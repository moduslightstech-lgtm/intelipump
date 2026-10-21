#!/usr/bin/env bash
# Compare code pin + config on this Pi (run on pump-2 AND pump-3).
# Usage:
#   bash scripts/compare_pump_pi_code.sh
#   # or from Mac:
#   ssh intelipump@HOST 'bash -s' < scripts/compare_pump_pi_code.sh
set -euo pipefail

UNIT="${INTELIPUMP_UNIT:-intelipump.service}"
ENV_FILE="${INTELIPUMP_ENV:-/etc/intelipump/intelipump.env}"
WD="$(systemctl show "$UNIT" -p WorkingDirectory --value 2>/dev/null || true)"
PY=""
if [[ -n "$WD" && -x "$WD/.venv/bin/python" ]]; then
  PY="$WD/.venv/bin/python"
fi

echo "===== identity ====="
hostname
uptime
date
echo "unit=$UNIT"
echo "WorkingDirectory=$WD"
systemctl show "$UNIT" -p FragmentPath -p ActiveEnterTimestamp -p ExecStart --no-pager 2>/dev/null || true

echo
echo "===== git pin (checkout) ====="
if [[ -n "$WD" && -d "$WD/.git" ]]; then
  git -C "$WD" rev-parse HEAD 2>/dev/null || true
  git -C "$WD" log -1 --oneline 2>/dev/null || true
  git -C "$WD" status -sb 2>/dev/null || true
  git -C "$WD" describe --tags --always --dirty 2>/dev/null || true
elif [[ -n "$WD" ]]; then
  # nested intelipump-fdc layouts
  for cand in "$WD" "$WD/.." "$HOME/intelipump-fdc/intelipump" "$HOME/intelipump-fdc"; do
    if [[ -d "$cand/.git" ]]; then
      echo "git_dir=$cand"
      git -C "$cand" rev-parse HEAD
      git -C "$cand" log -1 --oneline
      git -C "$cand" status -sb
      break
    fi
  done
else
  echo "no WorkingDirectory"
fi

echo
echo "===== installed package ====="
if [[ -n "$PY" ]]; then
  "$PY" - <<'PY'
import importlib.metadata as m
import intelipump_fdc, inspect, pathlib
print("module_file:", pathlib.Path(intelipump_fdc.__file__).resolve())
try:
    print("version:", m.version("intelipump-fdc"))
except Exception as e:
    print("version_err:", e)
# fingerprint a few settle-related symbols if present
for modname in (
    "intelipump_fdc.services.persistence_bridge",
    "intelipump_fdc.controller.controller_loop",
):
    try:
        mod = __import__(modname, fromlist=["*"])
        p = pathlib.Path(inspect.getfile(mod)).resolve()
        print(f"{modname}: {p} mtime={p.stat().st_mtime}")
    except Exception as e:
        print(modname, e)
PY
else
  echo "no venv python"
fi

echo
echo "===== key env (sale/meter/sim) ====="
if [[ -f "$ENV_FILE" ]]; then
  grep -E 'DEVICE_ID|STATION|ENVIRONMENT|SIMULATED|SALE_DISPLAY|AUTHORIZE|EXPERIMENT|CHANNEL_MAP|HARDWARE_CD101|STARTUP_CAPTURE|LOGICAL_NOZZLE|HOLD' \
    "$ENV_FILE" | sort || true
else
  echo "missing $ENV_FILE"
fi

echo
echo "===== flash completes last 48h (STARTED→COMPLETED, no fill, <2s, vol<0.5L) ====="
if [[ -n "$PY" ]]; then
  "$PY" - <<'PY'
from sqlalchemy import create_engine, text, inspect
from datetime import datetime, timedelta

e = create_engine("sqlite:////var/lib/intelipump/intelipump.db")
since = (datetime.utcnow() - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
with e.connect() as c:
    txs = list(c.execute(text("""
      SELECT id, transaction_uuid, started_at, completed_at,
             raw_volume, raw_amount, source_completion_key, source_identifier, nozzle_id
      FROM transactions
      WHERE status IN ('COMPLETED','COMPLETE')
        AND completed_at >= :since
        AND COALESCE(raw_volume,0) > 0
        AND COALESCE(raw_volume,0) < 50
      ORDER BY completed_at DESC
      LIMIT 200
    """), {"since": since}).mappings())

    flash = []
    for tx in txs:
        # duration
        try:
            a = datetime.fromisoformat(str(tx["started_at"]).replace("Z",""))
            b = datetime.fromisoformat(str(tx["completed_at"]).replace("Z","").replace("+00:00",""))
            dur = (b - a).total_seconds()
        except Exception:
            dur = None
        n_fill = c.execute(text("""
          SELECT COUNT(*) FROM transaction_events
          WHERE transaction_id=:id AND event_type='FILLING_UPDATE'
        """), {"id": tx["id"]}).scalar()
        if (dur is not None and dur < 2.0) and int(n_fill or 0) == 0:
            flash.append({**dict(tx), "dur_s": dur, "n_fill": n_fill})

    print(f"completed_small_last_48h={len(txs)} flash_no_fill_lt2s={len(flash)}")
    for r in flash[:30]:
        print({
            "uuid": r["transaction_uuid"],
            "started": r["started_at"],
            "completed": r["completed_at"],
            "dur_s": r["dur_s"],
            "raw_volume": r["raw_volume"],
            "raw_amount": r["raw_amount"],
            "src": r["source_identifier"],
            "key": r["source_completion_key"],
            "n_fill": r["n_fill"],
        })
PY
else
  echo "skip flash scan (no python)"
fi

echo
echo "Done — paste this whole block from pump-2 and pump-3."
