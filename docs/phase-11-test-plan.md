# Phase 11A/11B test plan

## Automated (this delivery)

| # | Case | Coverage |
|---|------|----------|
| 1 | READY only after init | CLI / notifier ready sequencing unit tests |
| 2 | Watchdog fed by loop progress | ControllerLoop + recording notifier |
| 3 | Watchdog not fed when progress stops | Stalled transport / no progress marks |
| 4 | Missing NOTIFY_SOCKET handled | SystemdNotifier no-op |
| 16 | SIGTERM → STOPPING | CLI finally / notifier.stopping |
| 17 | LISTEN_ONLY retained | Existing safety assertions in loop tests |

## Manual on Pi (after deploy)

1. Start `intelipump.service` with `WatchdogSec=30`
2. Confirm `StatusText` updates and service stays active
3. Pause process (`kill -STOP`) and confirm systemd restarts after watchdog
4. `systemctl stop` and confirm clean journal `STOPPING` / exit

## Deferred to 11C–11H

Serial reconnect, DB backup, host health, health CLI exit codes, hardware watchdog.
