# Phase 11A/11B/11C test plan

## Automated (this delivery)

| # | Case | Coverage |
|---|------|----------|
| 1 | READY only after init | CLI / notifier ready sequencing unit tests |
| 2 | Watchdog fed by loop progress | ControllerLoop + recording notifier |
| 3 | Watchdog not fed when progress stops | Stalled transport / no progress marks |
| 4 | Missing NOTIFY_SOCKET handled | SystemdNotifier no-op |
| 5 | Missing serial does not crash | FlakySerialTransport fail_opens |
| 6 | Reconnect backoff increases / capped | SerialReconnectBackoff + loop test |
| 7 | Successful reconnect resets backoff | Flaky then open + simulator |
| 8 | 3 timeouts → DEGRADED | PumpSession |
| 9 | 10 timeouts → DISCONNECTED | PumpSession |
| 10 | Valid EOT/DATA restores HEALTHY | PumpSession |
| 11 | Cumulative timeouts preserved | PumpSession |
| 12 | Transient clears / persistent remains | PumpSession |
| 13 | Serial loss → pumps DISCONNECTED | ControllerLoop |
| 14 | Transition logs once | HealthTransitionLog |
| 15 | Watchdog during reconnect progress | RecordingNotifier |
| 16 | SIGTERM → STOPPING | CLI finally / notifier.stopping |
| 17 | LISTEN_ONLY retained | Safety assertions in loop tests |
| 18 | No command replay on reconnect | Outbound empty during reconnect |

## Manual on Pi (after deploy)

1. Start `intelipump.service` with `WatchdogSec=30`
2. Confirm `StatusText` includes `serial=` / `pumps=N/M healthy` / `reconnects=`
3. Unplug controller adapter briefly; process stays up; reconnect recovers
4. Pause process (`kill -STOP`) and confirm systemd restarts after watchdog
5. `systemctl stop` and confirm clean journal `STOPPING` / exit

## Deferred to 11D–11H

DB backup, host health, health CLI exit codes, hardware watchdog.
