# Dashboard Read now → hardware CD101 (SAO)

## Why we suggested disabling the canary gate

The attended test used a **temporary** allowlist so CD101 was only possible while you were present. Clearing `/etc/intelipump/intelipump.env` was hygiene after a one-off canary — **not** a permanent product requirement.

For dashboard use you **keep the gate on** as normal Pi config (still allowlisted to that device/addresses).

## Architecture

```
Dashboard "Read now"
  → API MQTT READ_METER
  → intelipump-cloud-sync (command intake)
  → /var/lib/intelipump/meter-read-request.json
  → intelipump-controller (sole serial outbound CD101)
  → meter-read-result.json
  → cloud-sync publishes METER_READING
  → DigitalTwin consumer → Pump Meter Readings UI
```

No second serial connection. No RESET / SET_PRICE / AUTHORIZE.

## Permanent Pi config (controller + cloud-sync)

Add to **both** `/etc/intelipump/intelipump.env` and `/etc/intelipump/intelipump-cloud-sync.env`:

```bash
INTELIPUMP_METER_READING__HARDWARE_CD101=true
INTELIPUMP_METER_READING__ALLOWED_DEVICE_ID=InteliPump-SAO-RS1-pi-00N
INTELIPUMP_METER_READING__ALLOWED_ADDRESSES=1,2
INTELIPUMP_METER_READING__COUNTER_SELECT=1
INTELIPUMP_METER_READING__VOLUME_DECIMALS=3
INTELIPUMP_METER_READING__MIN_INTERVAL_SECONDS=60
INTELIPUMP_METER_READING__RESPONSE_TIMEOUT_SECONDS=8
```

Cloud-sync must also have command subscription (for Read now):

```bash
INTELIPUMP_MQTT__COMMAND_SUBSCRIPTION_ENABLED=true
```

Then:

```bash
sudo systemctl restart intelipump.service intelipump-cloud-sync.service
```

Code pin: include meter file-bridge + result publisher (`6bb2566` or later with dashboard wiring).

## Dashboard

Admin/Executive → **Pump Meter Readings** → station/pump → **Read now** per nozzle.  
Manual entry still works. Scheduled production reads remain off until dispense-delta soak.

## SAO scale (operator-verified pump-3)

`litres = raw_scaled / 1000` (3 decimal places).  
Addr 1 → nozzle-1, addr 2 → nozzle-2.
