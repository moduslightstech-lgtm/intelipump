#!/usr/bin/env python3
"""Pump-3 Pi recon — Oct 8 2026 05:00–09:20 Africa/Lagos vs cloud.

Run on intelipump-3:
  python3 scripts/recon_oct8_pump3_pi.py
  python3 scripts/recon_oct8_pump3_pi.py --db /var/lib/intelipump/intelipump.db

Compares local COMPLETED sales + sync_queue publishes for the same window
the dashboard used. Paste full stdout back for Pi↔droplet matching.
"""

from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

DB = Path("/var/lib/intelipump/intelipump.db")
LAGOS = ZoneInfo("Africa/Lagos")
# Half-open [05:00, 09:20) WAT — same as droplet recon.
START = datetime(2026, 10, 8, 5, 0, 0, tzinfo=LAGOS)
END = datetime(2026, 10, 8, 9, 20, 0, tzinfo=LAGOS)
MGR_LITRES = 599.144
CLOUD_LITRES = 695.900  # from droplet SQL totals


def _parse_ts(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        # sqlite often stores naive UTC
        try:
            dt = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=ZoneInfo("UTC")
            )
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(LAGOS)


def _litres(raw_volume: int | None, volume_decimals: int | None) -> float:
    if raw_volume is None:
        return 0.0
    dec = 2 if volume_decimals is None else int(volume_decimals)
    return raw_volume / (10**dec)


def _amount(raw_amount: int | None, amount_decimals: int | None) -> float:
    if raw_amount is None:
        return 0.0
    dec = 2 if amount_decimals is None else int(amount_decimals)
    return raw_amount / (10**dec)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DB))
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    print("===== pump-3 Pi recon =====")
    print(f"db={args.db}")
    print(f"window=[{START.isoformat()}, {END.isoformat()})")
    print(f"manager_target_L={MGR_LITRES}")
    print(f"cloud_sql_L={CLOUD_LITRES}")
    print()

    cols = {r[1] for r in cur.execute("PRAGMA table_info(transactions)")}
    need = {
        "transaction_uuid",
        "status",
        "raw_volume",
        "raw_amount",
        "completed_at",
        "source_completion_key",
    }
    missing = sorted(need - cols)
    if missing:
        print("MISSING columns:", missing)
        return

    has_vol_dec = "volume_decimals" in cols
    has_amt_dec = "amount_decimals" in cols
    has_price = "raw_price" in cols
    has_pump = "pump_id" in cols
    has_noz = "nozzle_id" in cols
    has_can_pump = "canonical_pump_id" in cols

    select_cols = [
        "transaction_uuid",
        "status",
        "raw_volume",
        "raw_amount",
        "source_completion_key",
        "completed_at",
        "started_at" if "started_at" in cols else "NULL AS started_at",
        "volume_decimals" if has_vol_dec else "2 AS volume_decimals",
        "amount_decimals" if has_amt_dec else "2 AS amount_decimals",
        "raw_price" if has_price else "NULL AS raw_price",
        "pump_id" if has_pump else "NULL AS pump_id",
        "nozzle_id" if has_noz else "NULL AS nozzle_id",
        "canonical_pump_id" if has_can_pump else "NULL AS canonical_pump_id",
    ]
    rows = cur.execute(
        f"""
        SELECT {", ".join(select_cols)}
        FROM transactions
        WHERE status IN ('COMPLETED', 'COMPLETE')
          AND completed_at IS NOT NULL
        ORDER BY completed_at ASC
        """
    ).fetchall()

    in_win: list[sqlite3.Row] = []
    for r in rows:
        ts = _parse_ts(r["completed_at"])
        if ts is None:
            continue
        if START <= ts < END:
            in_win.append(r)

    total_l = 0.0
    total_a = 0.0
    by_key: dict[str, list[tuple[float, float, str, str]]] = defaultdict(list)
    print("===== 01 completed_in_window =====")
    print(
        "uuid,completed_lagos,litres,amount,raw_price,key,pump,nozzle,canonical_pump"
    )
    for r in in_win:
        lit = _litres(r["raw_volume"], r["volume_decimals"])
        amt = _amount(r["raw_amount"], r["amount_decimals"])
        total_l += lit
        total_a += amt
        key = r["source_completion_key"] or ""
        klass = "null"
        if key.startswith("fill:") or key.startswith("sidecar-settle"):
            klass = key.split(":")[0] if key.startswith("fill:") else "sidecar-settle"
        elif "sidecar-settle" in key:
            klass = "sidecar-settle"
        elif key.startswith("complete:") or ":complete:" in key:
            klass = "complete"
        elif key.startswith("abandoned"):
            klass = "abandoned"
        else:
            klass = "other"
        by_key[klass].append((lit, amt, r["transaction_uuid"], key[:80]))
        ts = _parse_ts(r["completed_at"])
        print(
            f"{r['transaction_uuid']},{ts.isoformat() if ts else ''},"
            f"{lit:.3f},{amt:.2f},{r['raw_price']},{key[:90]},"
            f"{r['pump_id']},{r['nozzle_id']},{r['canonical_pump_id']}"
        )

    print()
    print("===== 02 totals =====")
    print(f"n={len(in_win)}")
    print(f"litres={total_l:.3f}")
    print(f"amount={total_a:.2f}")
    print(f"pi_minus_manager_L={total_l - MGR_LITRES:.3f}")
    print(f"pi_minus_cloud_L={total_l - CLOUD_LITRES:.3f}")
    print(f"cloud_minus_manager_L={CLOUD_LITRES - MGR_LITRES:.3f}")

    print()
    print("===== 03 by_completion_key_class =====")
    for klass in sorted(by_key):
        items = by_key[klass]
        print(
            f"{klass}: n={len(items)} litres={sum(x[0] for x in items):.3f} "
            f"amount={sum(x[1] for x in items):.2f}"
        )

    # Rising-volume tick chains (same nozzle, <45s, litres up by <5L)
    print()
    print("===== 04 rising_tick_chains =====")
    parsed = []
    for r in in_win:
        ts = _parse_ts(r["completed_at"])
        if ts is None:
            continue
        parsed.append(
            (
                ts,
                r["nozzle_id"],
                _litres(r["raw_volume"], r["volume_decimals"]),
                r["transaction_uuid"],
                r["source_completion_key"] or "",
            )
        )
    parsed.sort(key=lambda x: (str(x[1]), x[0]))
    excess = 0.0
    for i in range(1, len(parsed)):
        ts, noz, lit, uuid, key = parsed[i]
        pts, pnoz, plit, puuid, pkey = parsed[i - 1]
        if noz != pnoz or noz is None:
            continue
        dt = (ts - pts).total_seconds()
        if 0 < dt <= 45 and lit > plit and (lit - plit) < 5.0:
            excess += plit  # prior tick likely phantom if we keep latest
            print(
                f"tick {puuid[:8]}… {plit:.3f}L -> {uuid[:8]}… {lit:.3f}L "
                f"delta_s={dt:.1f} nozzle={noz} keys={pkey[:40]!r}->{key[:40]!r}"
            )
    print(f"approx_prior_tick_excess_L={excess:.3f}")

    # sync_queue: what was published
    print()
    print("===== 05 sync_queue_TRANSACTION_COMPLETED =====")
    sq_cols = {r[1] for r in cur.execute("PRAGMA table_info(sync_queue)")}
    if not sq_cols:
        print("no sync_queue table")
    else:
        # Pull COMPLETED events; filter by payload time / created_at in python
        qrows = cur.execute(
            """
            SELECT id, entity_id, event_type, status, deduplication_key,
                   payload, created_at, updated_at
            FROM sync_queue
            WHERE event_type = 'TRANSACTION_COMPLETED'
            ORDER BY created_at ASC
            """
        ).fetchall()
        import json

        pub_l = 0.0
        pub_n = 0
        print("entity_id,status,dedupe,created_at,litres,amount,payload_status")
        for r in qrows:
            ts = _parse_ts(r["created_at"])
            if ts is None or not (START <= ts < END):
                # also try payload completed_at
                pass
            payload = {}
            try:
                payload = json.loads(r["payload"] or "{}")
            except json.JSONDecodeError:
                payload = {}
            completed = (
                payload.get("completedAt")
                or payload.get("completed_at")
                or payload.get("timestamp")
            )
            cts = _parse_ts(completed) or ts
            if cts is None or not (START <= cts < END):
                continue
            # volumeLitres may be string
            try:
                lit = float(payload.get("volumeLitres") or payload.get("volume_liters") or 0)
            except (TypeError, ValueError):
                lit = 0.0
            try:
                amt = float(payload.get("amount") or 0)
            except (TypeError, ValueError):
                amt = 0.0
            pub_l += lit
            pub_n += 1
            print(
                f"{r['entity_id']},{r['status']},{(r['deduplication_key'] or '')[:70]},"
                f"{cts.isoformat()},{lit:.3f},{amt:.2f},"
                f"{payload.get('status') or payload.get('final_status')}"
            )
        print(f"sync_queue_published_n={pub_n} litres={pub_l:.3f}")

    print()
    print("===== 06 uuid_list_for_droplet_match =====")
    print(",".join(r["transaction_uuid"] for r in in_win))


if __name__ == "__main__":
    main()
