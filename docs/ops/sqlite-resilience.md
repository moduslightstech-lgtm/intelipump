# SQLite resilience (pump Pi)

Goal: **hose / RS-485 controller keeps working** if the shared SQLite file or MQTT outbox is sick. Cloud lag is acceptable; stopping fills is not.

## Services

| Unit | Role |
|------|------|
| `intelipump.service` | Controller — do not stop for outbox issues |
| `intelipump-cloud-sync.service` | Outbox prune, integrity degrade, MQTT heartbeats |
| `intelipump-sqlite-auto-recover.timer` | Periodic corrupt detect → recover |

## Automatic (after deploy)

- Cloud-sync deletes old **DELIVERED** and terminal **FAILED** `sync_queue` rows in small batches.
- Cloud-sync runs `PRAGMA quick_check` periodically; on failure it **degrades outbox only** (no controller restart) and keeps heartbeating with `databaseStatus=CORRUPT` so the twin can raise **EDGE_SQLITE_CORRUPT** (+ email) instead of only Offline.
- Daily cron: backup + full `integrity_check` (`install_pi_sqlite_cron.sh`).
- Timer (every 5 min): if DB is corrupt or cloud-sync is down with a healthy DB, auto-recover / restart sync (`install_pi_sqlite_auto_recover.sh`).

## Recover a corrupt DB (manual)

```bash
sudo apt-get install -y sqlite3   # once
sudo INTELIPUMP_RECOVER_STOP_CONTROLLER=1 ./scripts/pi_sqlite_recover.sh
# clear poison CRITICAL spill if queue storms after recover:
#   sudo mv /var/lib/intelipump/intelipump.persist_recovery.jsonl \
#     /var/lib/intelipump/intelipump.persist_recovery.jsonl.poison.$(date +%Y%m%d-%H%M%S)
sudo ./scripts/pi_sqlite_republish_meter_results.sh
```

Prefer stopping **cloud-sync only**. Set `INTELIPUMP_RECOVER_STOP_CONTROLLER=1` when exclusive access is required (auto-recover always uses this).

## Install cron + auto-recover on an existing Pi

```bash
sudo ./scripts/install_pi_sqlite_cron.sh
sudo ./scripts/install_pi_sqlite_auto_recover.sh
# dry-run watchdog:
sudo INTELIPUMP_AUTO_RECOVER_DRY_RUN=1 /usr/local/sbin/pi_sqlite_auto_recover.sh
```

## Env knobs (cloud-sync)

See comments in `deploy/systemd/intelipump-cloud-sync.env.example`
(`INTELIPUMP_DATABASE__DELIVERED_SYNC_QUEUE_RETENTION_DAYS`, prune batch/interval, alert thresholds, integrity interval).

Recommend `INTELIPUMP_DATABASE__DELIVERED_SYNC_QUEUE_RETENTION_DAYS=2` on busy SAO pumps to keep the DB small (fewer corruptions on brownouts without a UPS).
