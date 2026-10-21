# SQLite resilience (pump Pi)

Goal: **hose / RS-485 controller keeps working** if the shared SQLite file or MQTT outbox is sick. Cloud lag is acceptable; stopping fills is not.

## Services

| Unit | Role |
|------|------|
| `intelipump.service` | Controller — do not stop for outbox issues |
| `intelipump-cloud-sync.service` | Outbox prune, integrity degrade, MQTT |

## Automatic (after deploy)

- Cloud-sync deletes old **DELIVERED** and terminal **FAILED** `sync_queue` rows in small batches.
- Cloud-sync runs `PRAGMA quick_check` periodically; on failure it **degrades outbox only** (no controller restart).
- Daily cron: backup + full `integrity_check` (`install_pi_sqlite_cron.sh`).

## Recover a corrupt DB

```bash
sudo apt-get install -y sqlite3   # once
sudo ./scripts/pi_sqlite_recover.sh
# optional empty broken outbox:
#   sudo INTELIPUMP_RECOVER_EMPTY_SYNC_QUEUE=1 ./scripts/pi_sqlite_recover.sh
sudo ./scripts/pi_sqlite_republish_meter_results.sh
```

Prefer stopping **cloud-sync only**. Set `INTELIPUMP_RECOVER_STOP_CONTROLLER=1` only if exclusive access is required.

## Install cron on an existing Pi

```bash
sudo ./scripts/install_pi_sqlite_cron.sh
```

## Env knobs (cloud-sync)

See comments in `deploy/systemd/intelipump-cloud-sync.env.example`
(`INTELIPUMP_DATABASE__DELIVERED_SYNC_QUEUE_RETENTION_DAYS`, prune batch/interval, alert thresholds, integrity interval).
