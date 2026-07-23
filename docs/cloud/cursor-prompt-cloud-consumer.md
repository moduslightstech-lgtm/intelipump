# Cursor prompt — DigitalOcean MQTT consumer updates

Use this prompt in the **cloud repository** (not this device repo):

```
Update the InteliPump DigitalOcean MQTT consumer and API to accept Phase 9
device envelopes from the Raspberry Pi forecourt controller.

Contract sources (device repo):
- docs/cloud/digitalocean-integration.md
- docs/cloud/mqtt-topics.md
- docs/cloud/message-schemas.md
- docs/cloud/command-intake.md
- docs/cloud/cloud-deduplication.md

Required changes:
1. Parse envelope fields messageId, eventType, schemaVersion, environment,
   deviceId, stationId, pumpId, transactionId, correlationId, simulated,
   sequence, occurredAt, publishedAt, payload, deduplicationKey.
2. Upsert transactions/events by deduplicationKey; ignore duplicates safely.
3. Store raw scaled integers + decimal metadata; do not coerce to float money.
4. Route LAB topics (intelipump/lab/...) separately from PROD.
5. Do not insert simulated=true into production reporting tables.
6. Consume heartbeat and device status for liveness (heartbeat age), not only LWT.
7. Optionally publish LAB commands to intelipump/lab/stations/{stationId}/commands
   and consume .../commands/{correlationId}/result.
8. Keep credentials in env vars; prefer TLS.

Do not implement device-side SQLite or DART protocol in the cloud repo.
```
