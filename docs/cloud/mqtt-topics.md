# MQTT Topics (LAB)

Environment segment is derived from controller environment (`LAB` → `lab`, `PROD` → `prod`).

## Device topics

| Purpose | Topic |
| --- | --- |
| Heartbeat | `intelipump/{env}/devices/{deviceId}/heartbeat` |
| Status (ONLINE / LWT OFFLINE) | `intelipump/{env}/devices/{deviceId}/status` |

## Station topics

| Purpose | Topic |
| --- | --- |
| Pump events | `intelipump/{env}/stations/{stationId}/pumps/{pumpId}/events` |
| Transactions | `intelipump/{env}/stations/{stationId}/transactions` |
| Alarms | `intelipump/{env}/stations/{stationId}/alarms` |
| Audit | `intelipump/{env}/stations/{stationId}/audit` |
| Commands (inbound) | `intelipump/{env}/stations/{stationId}/commands` |
| Command result | `intelipump/{env}/stations/{stationId}/commands/{correlationId}/result` |

## Identifier rules

Rejected: empty IDs, wildcards (`+`, `#`), path traversal (`..`, `/`), environment mismatch.

## QoS policy

| Event | QoS |
| --- | --- |
| Heartbeat | 0 |
| Pump state changes | 1 |
| Filling updates | 0 (throttled) |
| Transaction completed | 1 |
| Alarm / audit | 1 |
| Command intake / result | 1 |

Exactly-once business delivery is application-level via `deduplicationKey`, not MQTT QoS.
