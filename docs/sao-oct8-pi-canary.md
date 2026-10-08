# SAO pump-5 canary — session identity + application ACK

**Manual operator runbook only.** Agents do not SSH, deploy, restart, or dispense.

| Pin | Full SHA | Role |
| --- | --- | --- |
| Pi `intelipump-fdc` | `95e5d5edc46fb73c4393a288c8d8ddfaa46c9d0c` | Controller + cloud-sync (session boundary, ACK intake) |
| Cloud `DigitalTwin` | `62e9a8e05552ca5872a996ffa3d2f2585a05331b` | Live telemetry table, COMPLETED freeze, `SALE_COMMITTED` digest |

**Image tag (build/push before droplet pull):** `prod_feature-62e9a8e05552`  
**Hub namespace:** `kacytunde`  
**Droplet:** `root@157.230.215.93` · cloud root `/opt/intelipump-cloud`  
**Pump-5 Pi repo:** `/home/intelipump/intelipump-fdc/intelipump`  
**Pi SQLite:** `/var/lib/intelipump/intelipump.db`  
**Pi env (preserve — do not rewrite via installer):**  
`/etc/intelipump/intelipump.env` · `/etc/intelipump/intelipump-cloud-sync.env`

**Expected SAO scope (confirm on Pi before changes):**

| Key | Expected |
| --- | --- |
| Station | `SAO-Redeemed-Station-1` |
| Device | `InteliPump-SAO-RS1-pi-005` |
| Topic env | `PROD` → `intelipump/prod/...` |
| Sale ACK topic | `intelipump/prod/devices/InteliPump-SAO-RS1-pi-005/sale-acks` |
| Transactions topic | `intelipump/prod/stations/SAO-Redeemed-Station-1/transactions` |

Preserve SAO mode, prices, channel maps, serial port, and SQLite. Do **not** run `install_sao_rs1_pump_pi.sh` / `bootstrap_sao_rs1_pump_pi.sh`. No forced outage or crash tests on the trading pump.

---

## Migration graph (029 / 030)

**Committed at `62e9a8e` (sales canary):** single head

```text
… → 028_sale_identity_decisions → 030_live_dispensing_telemetry
```

`029_pump_meter_readings` is **not** in that commit. It is local uncommitted meter work and must **not** be copied to the droplet for this canary.

**Local laptop (keep meter out of release):** if `db/alembic/versions/029_pump_meter_readings.py` exists in the working tree, its `down_revision` must be `030_live_dispensing_telemetry` (not `028`) so a dirty tree cannot create two heads off `028`. Still **exclude** that file from droplet sync.

**Committed cloud dependencies already required before `030`:** Alembic through `028_sale_identity_decisions` (sale identity / decisions tables). No meter models, routers, or consumer meter ingest.

**Exclude from build/sync (uncommitted meter):**

- `db/alembic/versions/029_pump_meter_readings.py`
- `backend/app/models/__init__.py` (local `PumpMeterReading*` edits)
- `backend/app/routers/pump_meter_readings.py`
- `backend/app/schemas/pump_meter_readings.py`
- `backend/app/services/pump_meter_readings.py`
- `backend/tests/test_pump_meter_readings.py`
- `consumer/app/services/meter_reading_ingest.py`
- `consumer/tests/test_meter_reading_ingest.py`
- `docs/pump-meter-readings.md`
- `ui/src/pages/PumpMeterReadingsPage.tsx`

**Tested migration command (repository runtime):** on the droplet, after syncing **committed-only** `db/` from `62e9a8e`, with Hub api image for the same tag:

```bash
cd /opt/intelipump-cloud
set -a && source .env && set +a
# IMAGE_TAG must match the api image that has alembic deps; host mounts ./db
IMAGE_TAG=prod_feature-62e9a8e05552 ./scripts/migrate.sh
```

That runs `alembic -c alembic.ini upgrade head` inside  
`kacytunde/intelipump-api:${IMAGE_TAG}` with `-v /opt/intelipump-cloud/db:/db` on the postgres Docker network. Equivalent one-liner (same as `migrate.sh` / cutover):

```bash
cd /opt/intelipump-cloud
set -a && source .env && set +a
PG_NETWORK="$(docker inspect intelipump-postgres --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
docker run --rm --network "$PG_NETWORK" \
  -e POSTGRES_HOST=intelipump-postgres -e POSTGRES_PORT=5432 \
  -e POSTGRES_DB="$POSTGRES_DB" -e POSTGRES_USER="$POSTGRES_USER" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -v /opt/intelipump-cloud/db:/db -w /db \
  kacytunde/intelipump-api:prod_feature-62e9a8e05552 \
  alembic -c alembic.ini upgrade head
```

---

## Testing order (mandatory)

1. Cloud deploy + schema verification  
2. Pump-5 Pi at `95e5d5e` with application ACK **off**; one attended sale per nozzle  
3. Enable application ACK on **that Pi only** (systemd env)  
4. Another attended sale: local pending → matching `SALE_COMMITTED` → durable `DELIVERED`; one cloud row  
5. Read-only reconciliation of every sale in the canary window  

---

## 0. Laptop — build/push cloud images from clean pin

```bash
cd /path/to/DigitalTwin
git fetch --prune origin
git checkout --detach 62e9a8e05552ca5872a996ffa3d2f2585a05331b
git rev-parse HEAD   # must equal 62e9a8e05552ca5872a996ffa3d2f2585a05331b
git status --porcelain=v1
# Must be empty. If meter files appear, you are on a dirty tree — abort and clean.

export IMAGE_TAG=prod_feature-62e9a8e05552
./scripts/build-push-images.sh --push

# Capture digests after push (fill into the table below):
for img in api consumer dashboard; do
  echo "=== intelipump-${img}:${IMAGE_TAG} ==="
  docker buildx imagetools inspect "kacytunde/intelipump-${img}:${IMAGE_TAG}" | sed -n '1,20p'
done
```

| Image | Tag | Digest (fill after push) |
| --- | --- | --- |
| `kacytunde/intelipump-api` | `prod_feature-62e9a8e05552` | `sha256:…` |
| `kacytunde/intelipump-consumer` | `prod_feature-62e9a8e05552` | `sha256:…` |
| `kacytunde/intelipump-dashboard` | `prod_feature-62e9a8e05552` | `sha256:…` |

**Reference (currently published, pre-canary):** `prod_feature-3023dd0e69bc`  
api `sha256:396be2a2dfde839995693fe722fe9a9ebf55b28c2f453a64ef3fc15c324eea97` ·  
consumer `sha256:0c95ab019e746dda9939b8a8e90f5dacb178cf7e1e1d9c98554e61afc8781b23` ·  
dashboard `sha256:2ad35bbfa37162a2c43132a0fbdecd377ca7f248d5cf10ea6f78337077c5e038`

Sync **committed-only** compose/db/scripts (never a dirty working tree with `029`):

```bash
cd /path/to/DigitalTwin
# Still detached at 62e9a8e with clean tree
./scripts/sync-cloud-to-droplet.sh root@157.230.215.93 /opt/intelipump-cloud

# Prove meter migration was not copied:
ssh root@157.230.215.93 'test ! -f /opt/intelipump-cloud/db/alembic/versions/029_pump_meter_readings.py \
  && ls /opt/intelipump-cloud/db/alembic/versions/028_sale_identity_decisions.py \
  && ls /opt/intelipump-cloud/db/alembic/versions/030_live_dispensing_telemetry.py'
```

---

## 1. Cloud deployment and schema verification

SSH: `ssh root@157.230.215.93`

```bash
cd /opt/intelipump-cloud
set -a && source .env && set +a

# Record prior pin / tag
grep -E '^IMAGE_TAG=' .env | tee /tmp/cloud-image-tag.pre-canary.txt
docker inspect --format '{{.Config.Image}} {{.Image}}' intelipump-api intelipump-consumer intelipump-dashboard \
  | tee /tmp/cloud-images.pre-canary.txt

# Point compose at the new tag (edit .env; do not invent secrets)
# IMAGE_TAG=prod_feature-62e9a8e05552
grep -E '^IMAGE_TAG=' .env

# Ensure cloud publishes application ACKs (default true in consumer code)
# Prefer explicit in /opt/intelipump-cloud/.env:
grep -E '^MQTT_PUBLISH_SALE_ACKS=|^MQTT_TOPIC_ENVIRONMENT=|^MQTT_TOPIC=' .env || true
# Expected for SAO prod:
#   MQTT_PUBLISH_SALE_ACKS=true
#   MQTT_TOPIC_ENVIRONMENT=prod   (or inferred from MQTT_TOPIC containing /prod/)
#   MQTT_TOPIC=intelipump/#   (or intelipump/prod/#)

# Backup Postgres (read consistency; trading continues)
ts="$(date -u +%Y%m%dT%H%M%SZ)"
docker exec intelipump-postgres pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  --format=custom --file="/tmp/intelipump-${ts}.dump"
docker cp "intelipump-postgres:/tmp/intelipump-${ts}.dump" \
  "/opt/intelipump-cloud/backups/intelipump-${ts}.dump"
ls -lh "/opt/intelipump-cloud/backups/intelipump-${ts}.dump"

# Preflight (additive; no deletes) — from laptop copy if scripts present on droplet
# Or run from laptop against published PG if you use that path.
test -f scripts/migration_028_preflight.sql && \
  docker exec -i intelipump-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    < scripts/migration_028_preflight.sql

# Pull + migrate + recreate app containers (keeps mqtt/postgres volumes)
IMAGE_TAG=prod_feature-62e9a8e05552 ./scripts/droplet-cutover.sh
# Or stepwise:
#   docker compose pull consumer api dashboard
#   IMAGE_TAG=prod_feature-62e9a8e05552 ./scripts/migrate.sh
#   docker compose up -d --no-build --remove-orphans consumer api dashboard nginx
```

### Schema verification (read-only)

```bash
cd /opt/intelipump-cloud
set -a && source .env && set +a

docker exec -i intelipump-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
SELECT version_num FROM alembic_version;
-- expect: 030_live_dispensing_telemetry

SELECT to_regclass('public.live_dispensing_telemetry') AS live_telemetry,
       to_regclass('public.sale_ingestion_decisions') AS sale_decisions,
       to_regclass('public.pump_meter_readings') AS meter_must_be_null;
-- meter_must_be_null → NULL for this canary

\d live_dispensing_telemetry
SQL
```

### Cloud ACK publish verification

```bash
# Effective consumer settings
docker exec intelipump-consumer printenv MQTT_PUBLISH_SALE_ACKS MQTT_TOPIC_ENVIRONMENT MQTT_TOPIC MQTT_HOST MQTT_PORT

# Expect MQTT_PUBLISH_SALE_ACKS=true (or unset → code default true)
# Expect MQTT_TOPIC_ENVIRONMENT=prod (SAO)

# After first Stage-2 sale (ACK still off on Pi), confirm publish in logs:
docker logs intelipump-consumer --since 30m 2>&1 | grep -E 'SALE_COMMITTED|Published SALE_COMMITTED' | tail -20

# Optional broker sniff (read-only subscribe; do not publish):
# mosquitto_sub -h 127.0.0.1 -p <mqtt_port> -u ... -P ... \
#   -t 'intelipump/prod/devices/InteliPump-SAO-RS1-pi-005/sale-acks' -v
```

Rollback cloud (images only; **do not** downgrade-migrate):

```bash
cd /opt/intelipump-cloud
# Restore IMAGE_TAG=prod_feature-3023dd0e69bc (or prior from /tmp/cloud-image-tag.pre-canary.txt)
docker compose pull consumer api dashboard
docker compose up -d --no-build --remove-orphans consumer api dashboard nginx
# Leave alembic at 030 (additive table is safe to keep)
```

---

## 2. Pump-5 Pi — deploy `95e5d5e`, ACK off; one sale per nozzle

On pump 5 as `intelipump` (sudo where shown). Nozzles idle; no active dispense.

```bash
export PI_SHA=95e5d5edc46fb73c4393a288c8d8ddfaa46c9d0c
export PREV_SHA="$(git -C /home/intelipump/intelipump-fdc/intelipump rev-parse HEAD)"
echo "PREV_SHA=$PREV_SHA" | tee /tmp/intelipump-pump5-prev-sha.txt
echo "CANARY_WINDOW_START_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee /tmp/intelipump-pump5-canary-window.txt

cd /home/intelipump/intelipump-fdc/intelipump

git fetch --prune origin
git status --porcelain=v1
# Expect empty. Do NOT git clean -fdx (would wipe .venv).

# Backup configs + DB (read-only copy)
sudo cp -a /etc/intelipump/intelipump.env \
  /tmp/intelipump.env.pre-${PI_SHA:0:12}
sudo cp -a /etc/intelipump/intelipump-cloud-sync.env \
  /tmp/intelipump-cloud-sync.env.pre-${PI_SHA:0:12}
sudo cp -a /var/lib/intelipump/intelipump.db \
  /tmp/intelipump.db.pre-${PI_SHA:0:12}

# Confirm scope + SAO mappings (do not change)
grep -E 'STATION_ID|DEVICE_ID|TOPIC_ENVIRONMENT|CHANNEL_MAP|LOGICAL_PUMP|PRODUCT|SERIAL_PORT|REQUIRE_APPLICATION_SALE_ACK|MODE' \
  /etc/intelipump/intelipump.env /etc/intelipump/intelipump-cloud-sync.env || true

# ACK must stay off for Stage 2
if grep -E '^[[:space:]]*INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=[[:space:]]*true' \
     /etc/intelipump/intelipump-cloud-sync.env /etc/intelipump/intelipump.env 2>/dev/null; then
  echo 'REFUSE: application sale ACK is already on' >&2; exit 1
fi
echo 'ACK off or unset (OK for Stage 2)'

# Pinned checkout
git checkout "$PI_SHA"
test "$(git rev-parse HEAD)" = "95e5d5edc46fb73c4393a288c8d8ddfaa46c9d0c"

# Package install into existing venv
export PATH="${HOME}/.local/bin:${PATH}"
command -v uv
uv sync --dev
test -x /home/intelipump/intelipump-fdc/intelipump/.venv/bin/intelipump-controller
test -x /home/intelipump/intelipump-fdc/intelipump/.venv/bin/intelipump-cloud-sync

# Restore env if anything touched it
sudo cp -a /tmp/intelipump.env.pre-${PI_SHA:0:12} /etc/intelipump/intelipump.env
sudo cp -a /tmp/intelipump-cloud-sync.env.pre-${PI_SHA:0:12} /etc/intelipump/intelipump-cloud-sync.env

# systemd must still use this venv + EnvironmentFiles
systemctl cat intelipump.service | grep -E 'ExecStart=|EnvironmentFile='
systemctl cat intelipump-cloud-sync.service | grep -E 'ExecStart=|EnvironmentFile='
# Expect EnvironmentFile=-/etc/intelipump/intelipump.env
# Expect EnvironmentFile=-/etc/intelipump/intelipump-cloud-sync.env

# Attended idle restart (no pump commands)
systemctl is-active intelipump intelipump-cloud-sync
sudo systemctl restart intelipump.service
sudo systemctl restart intelipump-cloud-sync.service
systemctl is-active intelipump intelipump-cloud-sync
journalctl -u intelipump -u intelipump-cloud-sync -n 80 --no-pager

# Effective ACK setting (file + unit)
systemctl show intelipump-cloud-sync -p EnvironmentFiles --no-pager
grep -E 'REQUIRE_APPLICATION_SALE_ACK' /etc/intelipump/intelipump-cloud-sync.env \
  || echo 'ACK unset (defaults false)'
# Process environ should not force true:
tr '\0' '\n' < /proc/"$(systemctl show -p MainPID --value intelipump-cloud-sync)"/environ \
  | grep -E 'REQUIRE_APPLICATION_SALE_ACK|TOPIC_ENVIRONMENT|DEVICE_ID|STATION_ID' || true
```

### Stage 2 attended sales (ACK off)

1. One attended hang-up sale on **each nozzle** of pump 5.  
2. Record face litres/amount/price, Lagos time, totalizers.  
3. No crash/forced-outage tests.

**Pi read-only after each sale:**

```bash
sqlite3 /var/lib/intelipump/intelipump.db <<'SQL'
.headers on
.mode column
SELECT transaction_uuid, status, raw_volume, raw_amount, source_completion_key, completed_at, updated_at
FROM transactions
ORDER BY updated_at DESC LIMIT 10;

SELECT id, event_type, status, entity_id, deduplication_key, created_at, updated_at
FROM sync_queue
ORDER BY created_at DESC LIMIT 15;
SQL
```

With ACK **off**, `sync_queue` may reach `DELIVERED` on broker PUBACK (not PG proof). Still require **one** COMPLETED UUID per physical sale.

**Cloud read-only (same UUID):**

```bash
docker exec -i intelipump-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
-- replace <uuid>
SELECT id, station_id, device_id, status, volume_liters, amount, currency, created_at, updated_at
FROM pump_transactions WHERE id = '<uuid>';

SELECT * FROM live_dispensing_telemetry WHERE transaction_id = '<uuid>'
ORDER BY updated_at DESC;

SELECT * FROM sale_ingestion_decisions
WHERE transaction_id = '<uuid>' OR decision_key LIKE '%<uuid>%'
ORDER BY created_at DESC
LIMIT 20;
SQL
```

**Pass Stage 2:** one physical dispense → one Pi COMPLETED → one cloud `pump_transactions` COMPLETED → dashboard row; twin may show DISPENSING in `live_dispensing_telemetry` without a second financial row. Cloud logs show `Published SALE_COMMITTED` for that device (Pi may ignore while ACK off).

---

## 3. Enable application ACK on pump-5 only (systemd)

Do **not** `export` in a shell. Edit the unit EnvironmentFile and restart the sync service.

```bash
# Backup current env
sudo cp -a /etc/intelipump/intelipump-cloud-sync.env \
  /tmp/intelipump-cloud-sync.env.pre-ack-on

# Set on cloud-sync only (ACK intake runs in intelipump-cloud-sync)
# Keep intelipump.service MQTT-off / prices / maps untouched.
if grep -q '^INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=' \
     /etc/intelipump/intelipump-cloud-sync.env; then
  sudo sed -i \
    's/^INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=.*/INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=true/' \
    /etc/intelipump/intelipump-cloud-sync.env
else
  echo 'INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=true' \
    | sudo tee -a /etc/intelipump/intelipump-cloud-sync.env >/dev/null
fi

grep -E '^INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=|^INTELIPUMP_MQTT__TOPIC_ENVIRONMENT=|^INTELIPUMP_CONTROLLER__DEVICE_ID=|^INTELIPUMP_CONTROLLER__STATION_ID=' \
  /etc/intelipump/intelipump-cloud-sync.env

# Reload + restart sync (and controller if it also loads the same flag — SAO: sync owns MQTT)
sudo systemctl daemon-reload
sudo systemctl restart intelipump-cloud-sync.service
# Controller does not require MQTT ACK flag for RS-485; restart only if its EnvironmentFile was changed (it was not).
systemctl is-active intelipump intelipump-cloud-sync

# Verify effective process environment
SYNC_PID="$(systemctl show -p MainPID --value intelipump-cloud-sync)"
tr '\0' '\n' < /proc/"$SYNC_PID"/environ \
  | grep -E 'REQUIRE_APPLICATION_SALE_ACK|TOPIC_ENVIRONMENT|DEVICE_ID|STATION_ID|CLIENT_ID'
# Expect:
#   INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=true
#   DEVICE_ID=InteliPump-SAO-RS1-pi-005
#   STATION_ID=SAO-Redeemed-Station-1
#   TOPIC_ENVIRONMENT=PROD (or PRODUCTION → prod segment)

journalctl -u intelipump-cloud-sync -n 50 --no-pager \
  | grep -E 'sale_ack_subscription_active|sale-acks|REQUIRE_APPLICATION' || true
# Expect subscription to:
#   intelipump/prod/devices/InteliPump-SAO-RS1-pi-005/sale-acks
```

Disable ACK later (rollback flag only):

```bash
sudo sed -i \
  's/^INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=.*/INTELIPUMP_MQTT__REQUIRE_APPLICATION_SALE_ACK=false/' \
  /etc/intelipump/intelipump-cloud-sync.env
sudo systemctl restart intelipump-cloud-sync.service
```

---

## 4. Attended sale with ACK on — pending → SALE_COMMITTED → DELIVERED

One more attended hang-up on pump 5 (either nozzle). Record face values.

```bash
# Immediately after hang-up / finalize (watch progression):
sqlite3 /var/lib/intelipump/intelipump.db <<'SQL'
.headers on
.mode column
SELECT id, event_type, status, entity_id, deduplication_key, created_at, updated_at
FROM sync_queue
ORDER BY created_at DESC LIMIT 10;

SELECT transaction_uuid, status, raw_volume, raw_amount, completed_at
FROM transactions
ORDER BY updated_at DESC LIMIT 5;
SQL
```

**Expected progression (ACK on):**

1. Outbox row `PENDING` (then briefly `CLAIMED`) for the sale event  
2. After MQTT publish of the sale: `AWAITING_APP_ACK`  
3. Cloud commits PG → publishes `SALE_COMMITTED` on  
   `intelipump/prod/devices/InteliPump-SAO-RS1-pi-005/sale-acks`  
4. Pi validates device/station/transaction → status `DELIVERED`  

```bash
# Cloud: exactly one financial row for that UUID
docker exec -i intelipump-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
SELECT id, status, volume_liters, amount, device_id, station_id
FROM pump_transactions WHERE id = '<uuid>';
-- one row, COMPLETED

SELECT COUNT(*) AS financial_rows FROM pump_transactions WHERE id = '<uuid>';
SELECT COUNT(*) AS twin_rows FROM live_dispensing_telemetry WHERE transaction_id = '<uuid>';
SQL

docker logs intelipump-consumer --since 15m 2>&1 \
  | grep -E "SALE_COMMITTED.*<uuid>|Published SALE_COMMITTED.*<uuid>" | tail -5

journalctl -u intelipump-cloud-sync --since "15 min ago" --no-pager \
  | grep -E 'sale_ack|SALE_COMMITTED|AWAITING_APP_ACK|DELIVERED' | tail -40
```

**Pass Stage 4:** face match · one Pi COMPLETED · one cloud COMPLETED · outbox ends `DELIVERED` only after matching `SALE_COMMITTED` · no second financial cloud row.

---

## 5. Read-only reconciliation (entire canary window)

Set window from `/tmp/intelipump-pump5-canary-window.txt` through now (UTC). Convert Lagos day bounds if reconciling a calendar day.

```bash
# Pi — every COMPLETED in window
sqlite3 /var/lib/intelipump/intelipump.db <<'SQL'
.headers on
.mode column
SELECT transaction_uuid, status,
       raw_volume/100.0 AS litres, raw_amount/100.0 AS amount,
       source_completion_key, completed_at
FROM transactions
WHERE status IN ('COMPLETED','COMPLETE')
  AND completed_at >= '<CANARY_START_UTC>'
ORDER BY completed_at;

SELECT status, COUNT(*), MIN(created_at), MAX(created_at)
FROM sync_queue
GROUP BY status;

SELECT entity_id, status, event_type, updated_at
FROM sync_queue
WHERE created_at >= '<CANARY_START_UTC>'
ORDER BY created_at;
SQL

# Cloud — same station / device / window
docker exec -i intelipump-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
SELECT id, status, volume_liters, amount, device_id, nozzle_id, created_at, updated_at
FROM pump_transactions
WHERE station_id = 'SAO-Redeemed-Station-1'
  AND device_id = 'InteliPump-SAO-RS1-pi-005'
  AND created_at >= '<CANARY_START_UTC>'
ORDER BY created_at;

-- twin vs financial: telemetry must not invent extra COMPLETED financial rows
SELECT t.id AS financial_id, t.status AS financial_status,
       l.status AS twin_status, l.updated_at AS twin_updated
FROM pump_transactions t
LEFT JOIN live_dispensing_telemetry l ON l.transaction_id = t.id
WHERE t.device_id = 'InteliPump-SAO-RS1-pi-005'
  AND t.created_at >= '<CANARY_START_UTC>'
ORDER BY t.created_at;
SQL
```

Trace each canary sale: face → Pi UUID → `sync_queue` → MQTT → `sale_ingestion_decisions` → `pump_transactions` → (`SALE_COMMITTED` if ACK-on) → dashboard completed filter.

---

## Rollback pump 5

```bash
export PREV_SHA="$(sed -n 's/^PREV_SHA=//p' /tmp/intelipump-pump5-prev-sha.txt)"
export PI_SHA=95e5d5edc46fb73c4393a288c8d8ddfaa46c9d0c
cd /home/intelipump/intelipump-fdc/intelipump

# Turn ACK off first if enabled
sudo cp -a /tmp/intelipump-cloud-sync.env.pre-ack-on \
  /etc/intelipump/intelipump-cloud-sync.env 2>/dev/null || \
sudo cp -a /tmp/intelipump-cloud-sync.env.pre-${PI_SHA:0:12} \
  /etc/intelipump/intelipump-cloud-sync.env
sudo cp -a /tmp/intelipump.env.pre-${PI_SHA:0:12} /etc/intelipump/intelipump.env

git checkout "$PREV_SHA"
export PATH="${HOME}/.local/bin:${PATH}"
uv sync --dev

sudo systemctl restart intelipump.service
sudo systemctl restart intelipump-cloud-sync.service
systemctl is-active intelipump intelipump-cloud-sync
# Do not wipe /var/lib/intelipump/intelipump.db — keep captured sales
```

---

## Out of scope

- Fleet enable of `REQUIRE_APPLICATION_SALE_ACK`  
- Meter / CD101 / uncommitted `029`  
- Destructive DB resets, reverse-migrate of `030`, forced power loss on trading pump  
- Rewriting prices, channel maps, or SAO authorize mode
