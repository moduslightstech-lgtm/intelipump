# API error format

```json
{
  "error": {
    "code": "PUMP_NOT_FOUND",
    "message": "Pump was not found",
    "correlation_id": "...",
    "details": {}
  }
}
```

## Codes

| Code | Typical HTTP |
|------|----------------|
| `VALIDATION_ERROR` | 422 |
| `INVALID_CORRELATION_ID` | 400 |
| `PUMP_NOT_FOUND` | 404 |
| `TRANSACTION_NOT_FOUND` | 404 |
| `ALARM_NOT_FOUND` | 404 |
| `DATABASE_UNAVAILABLE` | 503 |
| `CONTROLLER_UNAVAILABLE` | 503 |
| `COMMAND_BLOCKED` | 403 |
| `SIMULATOR_ONLY_RESTRICTION` | 403 |
| `INVALID_ENVIRONMENT` | 403 |
| `STREAM_SUBSCRIBER_LIMIT` | 429 |
| `HTTP_ERROR` | varies |
