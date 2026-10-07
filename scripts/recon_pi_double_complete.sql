-- Run on the Pi against shared controller SQLite:
--   sqlite3 /var/lib/intelipump/intelipump.db < recon_pi_double_complete.sql
-- Or: sqlite3 /var/lib/intelipump/intelipump.db
.headers on
.mode csv

-- 1) Same-pump same-totals COMPLETED pairs within 15 seconds (the bug we fixed)
SELECT '--- pairs_15s ---' AS section;
SELECT
  a.transaction_uuid AS uuid_a,
  b.transaction_uuid AS uuid_b,
  a.pump_id,
  a.raw_volume,
  a.raw_amount,
  a.source_completion_key AS key_a,
  b.source_completion_key AS key_b,
  ROUND((julianday(b.completed_at) - julianday(a.completed_at)) * 86400.0, 3) AS delta_s
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
 AND ABS((julianday(b.completed_at) - julianday(a.completed_at)) * 86400.0) <= 15
ORDER BY a.completed_at;

-- 2) Completion key families for one Lagos calendar day (adjust dates)
SELECT '--- key_family_day ---' AS section;
SELECT
  CASE
    WHEN source_completion_key LIKE 'sidecar-settle:%' THEN 'sidecar-settle'
    WHEN source_completion_key LIKE 'complete-fp:%' THEN 'complete-fp'
    WHEN source_completion_key LIKE 'complete:%' THEN 'complete'
    WHEN source_completion_key LIKE 'startup-baseline:%' THEN 'startup-baseline'
    WHEN source_completion_key IS NULL OR source_completion_key = '' THEN 'null'
    ELSE 'other'
  END AS key_family,
  COUNT(*) AS n,
  SUM(raw_volume) AS sum_raw_volume,
  SUM(raw_amount) AS sum_raw_amount
FROM transactions
WHERE status IN ('COMPLETED', 'COMPLETE')
  AND completed_at >= '2026-10-06 00:00:00'
  AND completed_at <  '2026-10-07 00:00:00'
GROUP BY 1
ORDER BY 1;

-- 3) Sync-queue TRANSACTION_COMPLETED twins (same pump totals, ≤15s)
SELECT '--- sync_queue_near_dup ---' AS section;
SELECT
  a.deduplication_key AS key_a,
  b.deduplication_key AS key_b,
  a.entity_id AS uuid_a,
  b.entity_id AS uuid_b,
  json_extract(a.payload, '$.raw_volume') AS vol,
  json_extract(a.payload, '$.raw_amount') AS amt,
  a.created_at AS created_a,
  b.created_at AS created_b,
  ROUND((julianday(b.created_at) - julianday(a.created_at)) * 86400.0, 3) AS delta_s
FROM sync_queue a
JOIN sync_queue b
  ON a.id < b.id
 AND a.event_type = 'TRANSACTION_COMPLETED'
 AND b.event_type = 'TRANSACTION_COMPLETED'
 AND json_extract(a.payload, '$.raw_volume') = json_extract(b.payload, '$.raw_volume')
 AND json_extract(a.payload, '$.raw_amount') = json_extract(b.payload, '$.raw_amount')
 AND COALESCE(json_extract(a.payload, '$.pump_db_id'), json_extract(a.payload, '$.pump_id'))
   = COALESCE(json_extract(b.payload, '$.pump_db_id'), json_extract(b.payload, '$.pump_id'))
 AND ABS((julianday(b.created_at) - julianday(a.created_at)) * 86400.0) <= 15
ORDER BY a.created_at
LIMIT 50;
