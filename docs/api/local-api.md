# Local API (Phase 8)

Trusted-network / LAB FastAPI surface under `/api/v1`.

**Not** for unauthenticated public internet. No Keycloak yet. No MQTT.

## Endpoints

| Method | Path | Notes |
|--------|------|-------|
| GET | `/controller/health` | Shared DB engine; recovery summary |
| GET | `/controller/metrics` | Internal metrics snapshot |
| GET | `/controller/status` | Lightweight loop status |
| GET | `/pumps` | List configured pumps |
| GET | `/pumps/{id}` | Detail (db id, logical id, or address) |
| GET | `/pumps/{id}/status` | Same as detail |
| GET | `/pumps/{id}/totals` | Raw scaled totals when known |
| GET | `/pumps/{id}/events` | Recent historical events |
| GET | `/transactions` | Filtered + paginated |
| GET | `/transactions/{id}` | By row id or uuid |
| GET | `/transactions/{id}/events` | Lifecycle events |
| GET | `/events/stream` | SSE |
| WS | `/events/ws` | WebSocket |
| POST | `/pumps/{id}/commands/evaluate` | Eligibility only |
| POST | `/lab/pumps/{id}/commands` | LAB + virtual transport only |
| GET | `/alarms` | Filtered list |
| GET | `/alarms/{id}` | Detail |
| GET | `/audit` | Filtered list |
| GET | `/audit/verify` | Hash-chain check |

## Pagination

Query `page` (1-based) and `page_size` (bounded by `ApiSettings.max_page_size`).

## Raw scaled values

Responses always include `raw_*` integers. `*_formatted` is set **only** when
decimal metadata is known — never invented.

## Lifespan

Startup: settings → shared engine → schema → recovery → broker → worker.  
Shutdown: stop commands → stop loop → flush worker → close streams → dispose engine.

## Example

```bash
curl -H 'X-Correlation-ID: lab-1' http://127.0.0.1:8000/api/v1/controller/health
```
