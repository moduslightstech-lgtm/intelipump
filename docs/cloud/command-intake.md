# Cloud Command Intake (Phase 9)

## Subscription

Enabled only when `INTELIPUMP_MQTT__COMMAND_SUBSCRIPTION_ENABLED=true`.

Topic: `intelipump/lab/stations/{stationId}/commands`

## Inbound schema

```json
{
  "commandId": "...",
  "correlationId": "...",
  "stationId": "InteliPump-US-Lab",
  "pumpId": "fp-1",
  "commandType": "READ_STATUS",
  "payload": {},
  "simulatorOnly": true,
  "createdAt": "...",
  "expiresAt": "...",
  "requestedBy": "...",
  "schemaVersion": "1.0",
  "environment": "LAB"
}
```

## Phase 9 behavior

1. Validate schema, station, environment, expiration, correlation ID.
2. Reject duplicates.
3. Persist command request + audit.
4. Evaluate via Phase 4 guards.
5. Publish result to `.../commands/{correlationId}/result`.
6. **Do not execute production active commands** (`executed=false`).

## Allowed execution

- LAB + `simulatorOnly=true`
- Phase 8 simulator restrictions pass
- Memory / virtual transport only
- Explicitly configured (`allow_lab_simulator_commands`)

## Result payload

Includes command/correlation IDs, `accepted`, `evaluated`, `executed`, `executionStatus`, blocking reasons, warnings, current/resulting pump state, environment, simulated, timestamp.
