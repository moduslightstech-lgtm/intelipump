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
6. **Do not execute production AUTHORIZE / RESET / STOP** (`executed=false`).

### Production remote SET_PRICE (sole controller)

Enabled on the SAO cloud-sync sidecar when both are set:

- `--commands-enabled`
- `--confirm-production-remote-set-price`

Inbound `SET_PRICE` with `simulatorOnly=false` writes
`/var/lib/intelipump/set-price-request.json`. The RS-485 controller applies CD5
on the next idle poll. Remote AUTHORIZE stays off.

### READ_METER (additive reconciliation, read-only)

Dashboard publishes `commandType: "READ_METER"` on the station commands topic.
Default outcome: `executionStatus=UNSUPPORTED` plus durable
`METER_READING_UNSUPPORTED` on `…/meter-readings` (never invents a zero totalizer).
Does not authorize, price, reset, or open a second serial connection.
Optional `INTELIPUMP_METER_READING__AUTO_CD101=true` may enqueue CD101 only in
LAB on the existing outbound/virtual path — not a production SAO enablement.

## Allowed execution

- LAB + `simulatorOnly=true`
- Phase 8 simulator restrictions pass
- Memory / virtual transport only
- Explicitly configured (`allow_lab_simulator_commands`)
- **or** production SET_PRICE bridge (file → controller CD5) as above

## Result payload

Includes command/correlation IDs, `accepted`, `evaluated`, `executed`, `executionStatus`, blocking reasons, warnings, current/resulting pump state, environment, simulated, timestamp.
