# Event streaming

## Endpoints

- `GET /api/v1/events/stream` — Server-Sent Events
- `WS /api/v1/events/ws` — WebSocket JSON messages

## Filters

`station_id`, `pump_id`, `event_type` (comma-separated), `severity`, `simulated`

## Event envelope

```json
{
  "event_id": "...",
  "event_type": "TRANSACTION_COMPLETED",
  "timestamp": "...",
  "station_id": "InteliPump-US-Lab",
  "pump_id": "pump-1",
  "transaction_id": "...",
  "correlation_id": null,
  "environment": "LAB",
  "simulated": true,
  "payload": {},
  "sequence": 12,
  "state_version": 4
}
```

## Keepalive

SSE and WS emit `HEARTBEAT` when idle longer than `stream_keepalive_seconds`.

## Slow subscribers

Bounded per-subscriber queues. Non-critical updates may drop (counted).
Critical events (`TRANSACTION_COMPLETED`, `ALARM_RAISED`, `FILLING_COMPLETED`,
`CONTROLLER_RECOVERED`) attempt to displace the oldest queued item.
