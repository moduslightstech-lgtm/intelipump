#!/usr/bin/env bash
# Dig pump-3 weird / micro COMPLETED sales — Oct 20 15:35–15:46 WAT.
# Run ON intelipump-3 (or: ssh intelipump@HOST 'bash -s' < this-script).
set -euo pipefail

UNIT="${INTELIPUMP_UNIT:-intelipump.service}"
DB="${INTELIPUMP_DB_PATH:-/var/lib/intelipump/intelipump.db}"
SINCE="${SINCE:-2026-10-20 15:35:00}"
UNTIL="${UNTIL:-2026-10-20 15:46:00}"

echo "===== host ====="
hostname
uptime
date

echo
echo "===== journal ${SINCE} → ${UNTIL} (settle / sale / uuid) ====="
journalctl -u "$UNIT" --since "$SINCE" --until "$UNTIL" --no-pager 2>/dev/null \
  | grep -iE 'sidecar-settle|FILLING_COMPLETED|FILLING_COMPLETE|premature|mint|begin_sale|SALE addr=|COMPLETED|uuid|UUID|hang.?up|NOZZLE|DC2|volume_raw|amount_raw|settle' \
  | tail -400 || echo "(no journal matches — logs may be volatile/rotated)"

echo
echo "===== sqlite completed txs in window ====="
WD="$(systemctl show "$UNIT" -p WorkingDirectory --value 2>/dev/null || true)"
if [[ -n "$WD" && -x "$WD/.venv/bin/python" ]]; then
  PY="$WD/.venv/bin/python"
elif [[ -x "${HOME}/intelipump-fdc/intelipump/.venv/bin/python" ]]; then
  PY="${HOME}/intelipump-fdc/intelipump/.venv/bin/python"
else
  PY="${INTELIPUMP_PYTHON:-python3}"
fi

"$PY" - <<PY
from sqlalchemy import create_engine, text, inspect

e = create_engine("sqlite:///${DB}")
insp = inspect(e)
tables = set(insp.get_table_names())
print("tables_with_tx:", sorted(t for t in tables if "trans" in t.lower()))

# Prefer local transactions table used by FDC
name = "transactions" if "transactions" in tables else None
if name is None:
    for t in tables:
        if "transaction" in t.lower():
            name = t
            break
if not name:
    print("NO transactions table")
    raise SystemExit(0)

cols = {c["name"] for c in insp.get_columns(name)}
print("cols:", sorted(cols))

def pick(*cands):
    for c in cands:
        if c in cols:
            return c
    return None

tcol = pick("completed_at", "transaction_completed_at", "device_timestamp", "updated_at", "created_at")
vol = pick("volume_liters", "volume")
amt = pick("amount", "amount_naira")
price = pick("unit_price", "price_per_liter", "raw_price")
uuid = pick("transaction_uuid", "id")
key = pick("source_completion_key", "completion_key", "deduplication_key")
status = pick("status",)
pump = pick("pump_id",)
nozzle = pick("nozzle_id",)
raw_vol = pick("raw_volume", "volume_raw", "filled_volume_raw")
raw_amt = pick("raw_amount", "amount_raw")

selects = [f"{uuid} AS tx", f"{tcol} AS t"]
for label, col in [
    ("nozzle", nozzle), ("vol", vol), ("amt", amt), ("price", price),
    ("status", status), ("key", key), ("raw_vol", raw_vol), ("raw_amt", raw_amt),
    ("pump", pump),
]:
    if col:
        selects.append(f"{col} AS {label}")

where = [f"{tcol} >= :since", f"{tcol} < :until"]
if status:
    where.append(f"UPPER(COALESCE({status},'')) IN ('COMPLETED','COMPLETE')")
if pump:
    where.append(f"{pump}='pump-3'")

sql = f"SELECT {', '.join(selects)} FROM {name} WHERE " + " AND ".join(where) + f" ORDER BY {tcol}, {vol or tcol}"
print("SQL:", sql)
with e.connect() as c:
    rows = list(c.execute(text(sql), {"since": "${SINCE}", "until": "${UNTIL}"}).mappings())
print(f"n={len(rows)}")
for r in rows:
    d = dict(r)
    print(d)
    # flag amount vs vol*price when both present
    try:
        if d.get("vol") is not None and d.get("amt") is not None and d.get("price") is not None:
            gap = float(d["amt"]) - float(d["vol"]) * float(d["price"])
            if abs(gap) > 1.0:
                print(f"  !! amt_gap={gap:.2f}")
        if d.get("vol") is not None and float(d["vol"]) < 0.35:
            print("  !! MICRO")
    except Exception:
        pass

# same-minute clusters
from datetime import datetime
def parse(ts):
    if ts is None:
        return None
    s = str(ts).replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:26], fmt)
        except ValueError:
            continue
    return None

print("\\n===== pairs within 90s (same nozzle if known) =====")
parsed = [(parse(r.get("t")), r) for r in rows]
parsed = [(t, r) for t, r in parsed if t]
for i, (ta, a) in enumerate(parsed):
    for tb, b in parsed[i+1:]:
        dt = (tb - ta).total_seconds()
        if dt > 90:
            break
        if a.get("nozzle") and b.get("nozzle") and a["nozzle"] != b["nozzle"]:
            continue
        print({
            "delta_sec": dt,
            "tx_a": a.get("tx"), "vol_a": a.get("vol"), "amt_a": a.get("amt"), "key_a": a.get("key"),
            "tx_b": b.get("tx"), "vol_b": b.get("vol"), "amt_b": b.get("amt"), "key_b": b.get("key"),
            "t_a": str(a.get("t")), "t_b": str(b.get("t")),
        })
PY

echo
echo "Done. Paste this full output back."
