# Cloud Message Schemas

## Envelope (`schemaVersion: "1.0"`)

```json
{
  "messageId": "uuid",
  "eventType": "TRANSACTION_COMPLETED",
  "schemaVersion": "1.0",
  "environment": "LAB",
  "deviceId": "InteliPump-Lab-pi-001",
  "stationId": "InteliPump-US-Lab",
  "pumpId": "fp-1",
  "transactionId": "...",
  "correlationId": "...",
  "simulated": true,
  "sequence": 123,
  "occurredAt": "2026-07-21T15:00:00+00:00",
  "publishedAt": "2026-07-21T15:00:01+00:00",
  "payload": {},
  "deduplicationKey": "tx-completed:..."
}
```

Rules:

- Money/volume values are raw scaled **integers** plus decimal metadata.
- `occurredAt` is the domain event time; `publishedAt` is publish time.
- `deduplicationKey` is stable across restarts for the same business event.

## Heartbeat payload

Includes device/station identity, mode, status, uptime, software version, database/MQTT status, pump health counts, pending sync / unresolved transaction counts, and `simulated`. No secrets or raw DART frames.

## Transaction completed payload

Includes transaction UUID, station/pump/nozzle, product when known, raw unit price / volume / amount with decimals, start/completion times, final status, source completion key, environment, simulated.
