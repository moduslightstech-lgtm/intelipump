#!/usr/bin/env python3
"""Pi recon: unit-price Unknown vs dual COMPLETED (paste on the Pi).

  python3 recon_pi_unit_price_unknown.py
  python3 recon_pi_unit_price_unknown.py --hours 3
  python3 recon_pi_unit_price_unknown.py --vol 4262 --amt 5775010
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

DB = Path("/var/lib/intelipump/intelipump.db")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DB))
    ap.add_argument("--hours", type=float, default=3.0)
    ap.add_argument("--vol", type=int, default=None, help="raw_volume twin filter")
    ap.add_argument("--amt", type=int, default=None, help="raw_amount twin filter")
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    print("--- schema price cols ---")
    cols = [r[1] for r in cur.execute("PRAGMA table_info(transactions)")]
    for c in ("raw_price", "raw_volume", "raw_amount", "source_completion_key", "status"):
        print(f"  {c}: {'yes' if c in cols else 'MISSING'}")

    print(f"\n--- completed last {args.hours}h (price presence) ---")
    rows = cur.execute(
        """
        SELECT
          transaction_uuid,
          pump_id,
          status,
          raw_volume,
          raw_amount,
          raw_price,
          source_completion_key,
          completed_at,
          CASE
            WHEN raw_price IS NULL OR raw_price <= 0 THEN 'UNKNOWN'
            ELSE 'HAS_PRICE'
          END AS price_bucket
        FROM transactions
        WHERE status IN ('COMPLETED', 'COMPLETE')
          AND completed_at IS NOT NULL
          AND completed_at >= datetime('now', ?)
        ORDER BY completed_at DESC
        LIMIT 80
        """,
        (f"-{args.hours} hours",),
    ).fetchall()
    print(
        "uuid,pump_id,vol,amt,raw_price,bucket,key,completed_at"
    )
    for r in rows:
        key = (r["source_completion_key"] or "")[:72]
        print(
            f"{r['transaction_uuid']},{r['pump_id']},{r['raw_volume']},"
            f"{r['raw_amount']},{r['raw_price']},{r['price_bucket']},"
            f"{key},{r['completed_at']}"
        )

    print("\n--- price_bucket counts last window ---")
    for r in cur.execute(
        """
        SELECT
          CASE
            WHEN raw_price IS NULL OR raw_price <= 0 THEN 'UNKNOWN'
            ELSE 'HAS_PRICE'
          END AS price_bucket,
          COUNT(*) AS n
        FROM transactions
        WHERE status IN ('COMPLETED', 'COMPLETE')
          AND completed_at >= datetime('now', ?)
        GROUP BY 1
        ORDER BY 1
        """,
        (f"-{args.hours} hours",),
    ):
        print(dict(r))

    print("\n--- same-pump same-totals twins ≤15s (last window) ---")
    twin_sql = """
        SELECT
          a.transaction_uuid AS uuid_a,
          b.transaction_uuid AS uuid_b,
          a.pump_id,
          a.raw_volume,
          a.raw_amount,
          a.raw_price AS price_a,
          b.raw_price AS price_b,
          a.source_completion_key AS key_a,
          b.source_completion_key AS key_b,
          a.completed_at AS at_a,
          b.completed_at AS at_b,
          ROUND(
            (julianday(b.completed_at) - julianday(a.completed_at)) * 86400.0, 3
          ) AS delta_s
        FROM transactions a
        JOIN transactions b
          ON a.transaction_uuid < b.transaction_uuid
         AND a.pump_id = b.pump_id
         AND a.status IN ('COMPLETED', 'COMPLETE')
         AND b.status IN ('COMPLETED', 'COMPLETE')
         AND a.raw_volume = b.raw_volume
         AND a.raw_amount = b.raw_amount
         AND a.completed_at IS NOT NULL
         AND b.completed_at IS NOT NULL
         AND ABS(
               (julianday(b.completed_at) - julianday(a.completed_at)) * 86400.0
             ) <= 15
         AND a.completed_at >= datetime('now', ?)
        ORDER BY a.completed_at DESC
        LIMIT 40
    """
    twins = cur.execute(twin_sql, (f"-{args.hours} hours",)).fetchall()
    print(f"twin_count={len(twins)} (showing up to 40)")
    for r in twins:
        print(
            f"{r['pump_id']} vol={r['raw_volume']} amt={r['raw_amount']} "
            f"price_a={r['price_a']} price_b={r['price_b']} delta_s={r['delta_s']}"
        )
        print(f"  a={r['uuid_a']} key={r['key_a']}")
        print(f"  b={r['uuid_b']} key={r['key_b']}")

    if args.vol is not None and args.amt is not None:
        print(f"\n--- exact twin vol={args.vol} amt={args.amt} ---")
        for r in cur.execute(
            """
            SELECT
              transaction_uuid, pump_id, raw_volume, raw_amount, raw_price,
              source_completion_key, status, completed_at, started_at
            FROM transactions
            WHERE raw_volume = ? AND raw_amount = ?
              AND status IN ('COMPLETED', 'COMPLETE', 'CANCELLED', 'ACTIVE')
            ORDER BY completed_at DESC NULLS LAST, started_at DESC
            LIMIT 20
            """,
            (args.vol, args.amt),
        ):
            print(dict(r))

    print("\n--- sync_queue TRANSACTION_COMPLETED near-dups ≤15s ---")
    try:
        for r in cur.execute(
            """
            SELECT
              a.entity_id AS uuid_a,
              b.entity_id AS uuid_b,
              a.deduplication_key AS key_a,
              b.deduplication_key AS key_b,
              json_extract(a.payload, '$.raw_volume') AS vol,
              json_extract(a.payload, '$.raw_amount') AS amt,
              json_extract(a.payload, '$.raw_unit_price') AS price_a,
              json_extract(b.payload, '$.raw_unit_price') AS price_b,
              json_extract(a.payload, '$.priceUncertain') AS unc_a,
              json_extract(b.payload, '$.priceUncertain') AS unc_b,
              a.created_at AS created_a,
              b.created_at AS created_b,
              ROUND(
                (julianday(b.created_at) - julianday(a.created_at)) * 86400.0, 3
              ) AS delta_s
            FROM sync_queue a
            JOIN sync_queue b
              ON a.id < b.id
             AND a.event_type = 'TRANSACTION_COMPLETED'
             AND b.event_type = 'TRANSACTION_COMPLETED'
             AND json_extract(a.payload, '$.raw_volume')
               = json_extract(b.payload, '$.raw_volume')
             AND json_extract(a.payload, '$.raw_amount')
               = json_extract(b.payload, '$.raw_amount')
             AND ABS(
                   (julianday(b.created_at) - julianday(a.created_at)) * 86400.0
                 ) <= 15
             AND a.created_at >= datetime('now', ?)
            ORDER BY a.created_at DESC
            LIMIT 20
            """,
            (f"-{args.hours} hours",),
        ):
            print(dict(r))
    except sqlite3.Error as exc:
        print(f"(sync_queue query skipped: {exc})")

    con.close()


if __name__ == "__main__":
    main()
