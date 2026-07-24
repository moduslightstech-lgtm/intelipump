# Phase 11 — Deployment Hardening (11A / 11B)

Status: **11A and 11B implemented**. 11C–11H are not started.

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

## Unit file

See `deploy/systemd/intelipump.service` and `deploy/systemd/intelipump.env.example`.

Conservative settings: `Type=notify`, `WatchdogSec=30s`, `Restart=on-failure`,
`StartLimitIntervalSec=300`, `StartLimitBurst=5`, `TimeoutStopSec=30`,
`KillSignal=SIGTERM`.

## Pi deployment (reviewable)

```bash
# From the deployed tree (adjust paths as needed):
sudo install -d -o intelipump -g intelipump -m 0750 /var/lib/intelipump
sudo install -d -o root -g root -m 0755 /etc/intelipump
sudo cp -n deploy/systemd/intelipump.env.example /etc/intelipump/intelipump.env
sudo cp deploy/systemd/intelipump.service /etc/systemd/system/intelipump.service
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

## Rollback

```bash
sudo systemctl stop intelipump.service
sudo systemctl disable intelipump.service
# Restore previous unit if any, or remove:
sudo rm -f /etc/systemd/system/intelipump.service
sudo systemctl daemon-reload
# Optional: set INTELIPUMP_WATCHDOG__ENABLED=false and run Type=simple unit
```

## Safety impact

- LISTEN_ONLY default unchanged; active commands remain disabled
- No protocol, retry, state-machine, or command-path changes
- Watchdog disable keeps all existing tests socket-free

## Not in this delivery

11C serial health classification, 11D SQLite backup, 11E host health,
11F recovery limits, 11G hardware watchdog docs enablement, 11H health CLI.
