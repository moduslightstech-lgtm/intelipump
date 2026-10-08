# SAO attended CD101/DC101 totalizer canary (one pump)

**Status of software results:** **UNVERIFIED** until you supply face/totalizer photos and the exported result JSON.

Do **not** enable scheduled production meter reads after this test until results pass.  
This agent does **not** SSH or run live pump commands — you run every step.

---

## Protocol (from code + Pump Interface Rev 2.11 refs — not guessed)

| Item | Documented value |
|------|------------------|
| Request | **CD101** — `TRANS=0x65`, `LNG=1`, `DATA=counter_select` |
| Lab/ePump default COUN | **`1`** → application payload **`65 01 01`** |
| Spec | `protocol/cd101.py` → “Pump Interface Rev 2.11, page 19, CD101” |
| Nature | **Read-only** — “does not authorize delivery” (same module docstring) |
| Response | **DC101** — `TRANS=0x65`, `LNG=16` (`protocol/.../decoder.py`, page 25) |
| DC101 fields | `counter_select` (COUN), `total_value` (5 BCD), `total_meter1_or_nofill` (5 BCD), `total_meter2` (5 BCD) |
| Address | DART **logical address** on the wire (controller `--addresses`); not invented |
| Units / precision | Raw = packed BCD **`raw_scaled`**. Litres Decimal only if COUN ∈ `0x01..0x09` **and** volume decimals known (DC7 DPVOL or explicit config). If decimals unknown, **store raw only — never invent 0.00 L** |
| Nozzle mapping | Channel map tags address→pump/nozzle; which DC101 field matches the **face** cumulative is **unverified** until attended compare |

Print the request bytes locally (no bus TX):

```bash
cd /home/intelipump/intelipump-fdc/intelipump
.venv/bin/intelipump-meter-read-once --address 1 --print-protocol
```

---

## What this path does / does not

| Does | Does not |
|------|----------|
| One-shot CD101 via **existing** controller outbound / poll serial | Open a second serial / bench `cd101_session` |
| File bridge under `/var/lib/intelipump/` | Change SAO mode, enable auto-auth, or ACK |
| Timeout + rate limit; `UNSUPPORTED`/`ERROR` without fake zero | RESET, SET_PRICE, AUTHORIZE |
| Record request/response hex, decoded counters, timestamps | Scheduled production meter reads |

Gate defaults **off**.

---

## Confirm from live config (please paste back)

Run on the Pi and send the output (needed values cannot be assumed from repo examples):

```bash
grep -E 'DEVICE_ID|STATION_ID|CHANNEL_MAP|SERIAL_PORT|ENVIRONMENT|MODE' /etc/intelipump/intelipump.env
systemctl cat intelipump.service | grep -E 'ExecStart=|--addresses'
# Expect something like --addresses 1,2 for a dual-hose head.
```

**Ask only if missing from that output:**

1. Exact `INTELIPUMP_CONTROLLER__DEVICE_ID`
2. Which DART logical address(es) you will allowlist for this pump (usually `1` and/or `2`)
3. Optional: face display decimal places (if known) for `VOLUME_DECIMALS` — leave unset to keep raw-only

---

## A. Setup (attended idle — enable gate for one device/address)

```bash
# Backup
sudo cp -a /etc/intelipump/intelipump.env /tmp/intelipump.env.pre-meter-canary
sudo mkdir -p /var/lib/intelipump
sudo chown intelipump:intelipump /var/lib/intelipump

# Install code that contains intelipump-meter-read-once + controller gate
# (use your chosen prod_feature SHA after pull/uv sync — you choose pin)
cd /home/intelipump/intelipump-fdc/intelipump
git rev-parse HEAD
.venv/bin/intelipump-meter-read-once --help >/dev/null

# Append gate — REPLACE DEVICE_ID and ADDRESSES from your grep above
sudo tee -a /etc/intelipump/intelipump.env >/dev/null <<'EOF'
# --- attended CD101 canary (remove after test) ---
INTELIPUMP_METER_READING__HARDWARE_CD101=true
INTELIPUMP_METER_READING__ALLOWED_DEVICE_ID=InteliPump-SAO-RS1-pi-00N
INTELIPUMP_METER_READING__ALLOWED_ADDRESSES=1
INTELIPUMP_METER_READING__COUNTER_SELECT=1
INTELIPUMP_METER_READING__MIN_INTERVAL_SECONDS=60
INTELIPUMP_METER_READING__RESPONSE_TIMEOUT_SECONDS=8
# Optional only after face scale is known; omit to keep raw_scaled only:
# INTELIPUMP_METER_READING__VOLUME_DECIMALS=2
EOF

# Edit the placeholder device id / addresses to match live config:
sudo nano /etc/intelipump/intelipump.env

# Restart controller only (no pump commands; cloud-sync unchanged)
systemctl is-active intelipump
sudo systemctl restart intelipump.service
systemctl is-active intelipump
journalctl -u intelipump -n 40 --no-pager
```

Confirm gate loaded (process env):

```bash
# systemd EnvironmentFile is sourced by the unit — verify file content:
grep METER_READING /etc/intelipump/intelipump.env
```

---

## B. Attended validation procedure

### 1) Photo face cumulative litres (each nozzle)

Record nozzle-1 and nozzle-2 face totals + time (Lagos). Keep photos.

### 2) Idle — one read per address

```bash
export ADDR=1   # then repeat with ADDR=2 after allowlisting both if needed
.venv/bin/intelipump-meter-read-once \
  --address "$ADDR" \
  --nozzle-hint "nozzle-$ADDR" \
  --notes "idle-baseline photo-$(date -u +%Y%m%dT%H%M%SZ)" \
  --wait-seconds 25 | tee "/tmp/meter-read-idle-addr${ADDR}.json"
```

Controller journal should show `[METER-READ] queued CD101` then `finished`.  
Compare JSON `rawScaled` / optional `volumeLiters` to the photo — mapping may need both meter1 and meter2 fields.

### 3) Independent dispense

Dispense a known volume on one nozzle; record face litres dispensed and both totalizers after hang-up. Confirm normal sale appears in dashboard (sales path unchanged).

### 4) Second read — delta vs dispense

```bash
.venv/bin/intelipump-meter-read-once \
  --address "$ADDR" \
  --notes "post-dispense" \
  --wait-seconds 25 | tee "/tmp/meter-read-post-addr${ADDR}.json"
```

Compute delta of the matching raw/litres field vs observed dispense.  
If rate-limited, wait `MIN_INTERVAL_SECONDS` (default 60).

### 5) Other nozzle

Allowlist `1,2` if both sides are on this Pi; repeat steps 1–4 for the other address. Confirm channel-map annotation in the result JSON.

### 6) Health

```bash
systemctl is-active intelipump intelipump-cloud-sync
journalctl -u intelipump -u intelipump-cloud-sync --since "10 min ago" --no-pager | \
  grep -E 'METER-READ|sale|ERROR|timeout|set_price' | tail -80
# Confirm sales still completing; no unexpected RESET/SET_PRICE from this canary.
```

---

## C. Log export

```bash
sudo cp -a /var/lib/intelipump/meter-read-result.json "/tmp/meter-read-result-$(date -u +%Y%m%dT%H%M%SZ).json"
cp -a /tmp/meter-read-*.json /tmp/ 2>/dev/null || true
journalctl -u intelipump --since "30 min ago" --no-pager | grep METER-READ | tee /tmp/meter-read-journal.txt
ls -la /tmp/meter-read*
```

Return: face photos, dispense notes, exported JSON, journal snippet.  
Until then, treat electronic totals as **UNVERIFIED**.

---

## D. Disable / rollback (required after session)

```bash
sudo cp -a /tmp/intelipump.env.pre-meter-canary /etc/intelipump/intelipump.env
# Or manually delete the INTELIPUMP_METER_READING__* block so HARDWARE_CD101 is unset/false
grep METER_READING /etc/intelipump/intelipump.env || echo 'gate lines removed'
sudo rm -f /var/lib/intelipump/meter-read-request.json
sudo systemctl restart intelipump.service
systemctl is-active intelipump
grep METER_READING /etc/intelipump/intelipump.env || echo 'OK: hardware meter gate off'
```

Do **not** leave `HARDWARE_CD101=true` on after the attended session.

---

## Pass criteria (for a later enablement decision)

1. Idle CD101→DC101 observed (or explicit unsupported/timeout — never fake zero).  
2. Face cumulative matches a documented DC101 field (+ scale) for **each** nozzle/address.  
3. Counter delta matches an attended dispense within agreed tolerance.  
4. Polling + sale capture remain healthy; no auth/price/reset side effects.  

Only after that may scheduled production reads be considered in a separate change.
