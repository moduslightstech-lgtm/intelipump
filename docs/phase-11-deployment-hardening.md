# Phase 11 — Deployment Hardening (11A / 11B / 11C)

Status: **11A, 11B, and 11C implemented**. 11D–11H are not started.

## Scope completed

### 11A — Internal liveness

- `src/intelipump_fdc/core/liveness.py` — `LivenessTracker` / `LivenessSnapshot`
- Monotonic marks: last loop iteration, last successful poll
- Snapshot fields: process start/uptime, ages, polls, valid responses, timeouts,
  mode, database health, serial status
- No per-loop database writes

### 11B — systemd watchdog

- `src/intelipump_fdc/core/systemd_notify.py` — `SystemdNotifier` (Unix datagram)
- Config: `INTELIPUMP_WATCHDOG__ENABLED` (default **false** for dev/tests)
- `READY=1` after config, safety, DB recovery, serial open attempt
- `WATCHDOG=1` only from controller-loop progress (never a free-running timer)
- `STATUS=` throttled (`INTELIPUMP_WATCHDOG__STATUS_INTERVAL_S`, default 15s)
- `STOPPING=1` on graceful shutdown (SIGTERM / finally)
- Missing `NOTIFY_SOCKET` → safe no-op

### 11C — Serial and communication health

- `src/intelipump_fdc/controller/comm_health.py` — serial monitor + backoff
- Per-pump states: HEALTHY / DEGRADED / DISCONNECTED / FAULTED
- Defaults: DEGRADED after 3 consecutive timeouts; DISCONNECTED after 10;
  FAULTED after 3 consecutive persistent protocol faults
- Reconnect backoff: 0.5s → 1 → 2 → 4 → 8 → max 15s (jitter default 0)
- Temporary serial loss does not terminate the process
- Transition logs only (no per-timeout spam)
- Serial health: `OPEN` / `DEGRADED` / `DISCONNECTED` / `FAULTED`
  (STATUS token: `open|missing|disconnected|degraded|faulted`)
- STATUS example:
  `mode=LISTEN_ONLY serial=open pumps=2/2 healthy reconnects=0 timeouts=0 crc=0 loop_age=0.0s`
- Read-only `ControllerLoop.health_diagnostic_snapshot()` for a future health CLI
- Persistent faults retained after valid EOT/DATA; transient `response_timeout` clears
- Passive Wayne lab checklist: `docs/phase-11c-wayne-passive-lab-test.md`

## Unit file

See `deploy/systemd/intelipump.service` and `deploy/systemd/intelipump.env.example`.

Conservative settings: `Type=notify`, `WatchdogSec=30s`, `Restart=on-failure`,
`StartLimitIntervalSec=300`, `StartLimitBurst=5`, `TimeoutStopSec=30`,
`KillSignal=SIGTERM`.

## Pi deployment (reviewable)

Owned-lab one-shot installer (after clone):

```bash
cd ~/intelipump/intelipump-fdc
./scripts/install_owned_lab_pi.sh --confirm-owned-lab-install --start
# optional: --port /dev/ttyUSB0 --price 1175 --addresses 1,2
journalctl -u intelipump -n 100 --no-pager
```

Manual equivalent:

```bash
# From the deployed tree (adjust paths as needed):
sudo install -d -o intelipump -g intelipump -m 0750 /var/lib/intelipump
sudo install -d -o root -g root -m 0755 /etc/intelipump
sudo cp deploy/systemd/intelipump.service /etc/systemd/system/intelipump.service
sudo cp -n deploy/systemd/intelipump.env.example /etc/intelipump/intelipump.env
# Owned lab: ExecStart in the unit uses BENCH_CONTROL + confirm flags.
sudo systemctl daemon-reload
sudo systemctl enable intelipump.service
sudo systemctl start intelipump.service
sudo systemctl status intelipump.service
journalctl -u intelipump -e
```

Confirm watchdog:

```bash
systemctl show intelipump -p WatchdogUSec,WatchdogTimestamp,StatusText
```

## Pi validation (11C)

```bash
sudo systemctl restart intelipump.service
systemctl show intelipump -p StatusText,NRestarts,ActiveState
# Unplug USB-RS485 (controller side) briefly; confirm process stays up,
# StatusText shows serial=missing|disconnected and reconnects increases, then recovers.
journalctl -u intelipump -e | grep -E 'serial_|pump_communication_|persistent_protocol'
# Confirm LISTEN_ONLY and no authorize / command replay lines.
```

## Safety impact

- LISTEN_ONLY default unchanged; active commands remain disabled
- No protocol framing/CRC/ACK/timing/retry changes
- Serial loss uses reconnect backoff; does not authorize or replay commands
- Watchdog disable keeps all existing tests socket-free

## Rollback

```bash
sudo systemctl stop intelipump.service
# Redeploy previous package/unit, or:
sudo systemctl disable intelipump.service
sudo rm -f /etc/systemd/system/intelipump.service
sudo systemctl daemon-reload
# Optional: INTELIPUMP_WATCHDOG__ENABLED=false with Type=simple unit
```

## Not in this delivery

11D SQLite backup, 11E host health, 11F recovery limits, 11G hardware watchdog
docs enablement, 11H health CLI.
