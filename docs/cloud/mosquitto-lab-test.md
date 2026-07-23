# Local Mosquitto LAB test

## Quick broker

```bash
docker run --rm -p 1883:1883 \
  -v "$PWD/tests/integration/mosquitto-lab.conf:/mosquitto/config/mosquitto.conf:ro" \
  eclipse-mosquitto:2
```

Anonymous local LAB testing only — not for production. Config: `tests/integration/mosquitto-lab.conf`.

## Manual lifecycle

1. Start Mosquitto.
2. Subscribe: `mosquitto_sub -h 127.0.0.1 -t 'intelipump/lab/#' -v`
3. Enable MQTT and run cloud sync:

```bash
INTELIPUMP_MQTT__ENABLED=true \
INTELIPUMP_MQTT__HOST=127.0.0.1 \
INTELIPUMP_MQTT__COMMAND_SUBSCRIPTION_ENABLED=true \
INTELIPUMP_SAFETY__ALLOW_LAB_SIMULATOR_COMMANDS=true \
uv run intelipump-cloud-sync --duration 120 --commands-enabled --log-messages
```

4. Confirm ONLINE status + heartbeats.
5. Complete a simulated sale (controller/simulator) and confirm transaction topic traffic.
6. Stop Mosquitto; complete another sale; confirm SQLite `sync_queue` remains PENDING.
7. Restart Mosquitto; confirm queued delivery resumes once.
8. Publish an expired command; confirm rejection result.
9. Publish a valid LAB `READ_STATUS` with `simulatorOnly=true`; confirm result message.

## Pytest integration mark

```bash
uv run pytest -m mqtt_integration
```

Skipped automatically when Mosquitto is not reachable on `127.0.0.1:1883`.
