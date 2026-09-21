# SAO Redeemed Station 1 — one Pi per physical pump

Each physical pump has its own Raspberry Pi and its own RS-485 USB adapter.
All Pis publish to the **same** station id on the cloud Mosquitto broker.

## Data path

```
Wayne pump  --RS-485-->  intelipump.service (controller, MQTT off)
                              |
                              v
                         SQLite /var/lib/intelipump/intelipump.db
                              |
                              v
                    intelipump-cloud-sync.service
                              |
                         MQTT publish
                              v
              157.230.215.93  intelipump/prod/stations/SAO-Redeemed-Station-1/...
                              |
                              v
                    Digital Twin consumer → dashboard
```

## Identity

| Field | Value |
|-------|--------|
| Station (all pumps) | `SAO-Redeemed-Station-1` |
| Device (pump N) | `InteliPump-SAO-RS1-pi-00N` |
| Logical pump | `pump-N` |
| DART on that Pi | addresses `1,2` → nozzle-1 / nozzle-2 |
| MQTT client | `InteliPump-SAO-RS1-pi-00N-sync` |

Do **not** put two Pis on the same RS-485 bus (two masters will fight).

## Deploy pump N (on the Pi — preferred for remaining pumps)

```bash
cd ~/intelipump-fdc/intelipump   # adjust if your clone path differs
git checkout v4-cloud-deploy && git pull

./scripts/bootstrap_sao_rs1_pump_pi.sh --pump 3 --mqtt-password 'SECRET'
```

That one command: pulls the branch, installs controller + cloud-sync for pump N,
writes the MQTT password, and starts both services.

## Deploy pump N (from Mac)

```bash
cd intelipump-fdc

MQTT_PASSWORD='<same as pump 1>' \
  ./scripts/deploy_sao_rs1_pump_to_pi.sh --pump 2 intelipump@<pi-ip> --start
```

Twin catalog (once per pump) — from Mac:

```bash
cd DigitalTwin
export TWIN_ADMIN_EMAIL=...
export TWIN_ADMIN_PASSWORD=...
./scripts/deploy_sao_rs1_pump_cloud_to_droplet.sh --pump 3
```

Or on the droplet:

```bash
cd /opt/intelipump-cloud
export TWIN_API_BASE=http://127.0.0.1
export TWIN_ADMIN_EMAIL=...
export TWIN_ADMIN_PASSWORD=...
./scripts/bootstrap_sao_rs1_pump_cloud.sh --pump 3
```

That creates the device (`InteliPump-SAO-RS1-pi-00N`), pump-N nozzles, and
PMS Tank 1 → pump-N connections (clears Unconnected).

## Verify sales stream

On the Pi:

```bash
systemctl is-active intelipump intelipump-cloud-sync
journalctl -u intelipump-cloud-sync -f
```

Look for `TRANSACTION_COMPLETED` / MQTT publish lines after a dispense.

On the droplet / dashboard: Transactions for **SAO redeemed station 1** should show `pump-2`.

## Related scripts

| Script | Role |
|--------|------|
| `scripts/deploy_sao_rs1_pump_to_pi.sh` | rsync + install on remote Pi |
| `scripts/install_sao_rs1_pump_pi.sh` | on-Pi install (also usable directly) |
| `DigitalTwin/scripts/provision_sao_rs1_pump.sh` | Twin device + pump catalog |
| `scripts/install_sao_rs1_pi.sh` | legacy pump-1-only installer (still valid) |
