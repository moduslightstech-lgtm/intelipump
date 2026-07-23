"""Cloud message envelope and serialization."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

MESSAGE_SCHEMA_VERSION = "1.0"


@dataclass(frozen=True, slots=True)
class CloudMessageEnvelope:
    message_id: str
    event_type: str
    schema_version: str
    environment: str
    device_id: str
    station_id: str
    sequence: int
    occurred_at: str
    published_at: str
    simulated: bool
    deduplication_key: str
    payload: dict[str, Any] = field(default_factory=dict)
    pump_id: str | None = None
    transaction_id: str | None = None
    correlation_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "messageId": self.message_id,
            "eventType": self.event_type,
            "schemaVersion": self.schema_version,
            "environment": self.environment,
            "deviceId": self.device_id,
            "stationId": self.station_id,
            "pumpId": self.pump_id,
            "transactionId": self.transaction_id,
            "correlationId": self.correlation_id,
            "simulated": self.simulated,
            "sequence": self.sequence,
            "occurredAt": self.occurred_at,
            "publishedAt": self.published_at,
            "payload": self.payload,
            "deduplicationKey": self.deduplication_key,
        }


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


_MONEY_VOLUME_KEYS = frozenset(
    {
        "raw_volume",
        "raw_amount",
        "raw_price",
        "raw_unit_price",
        "volume",
        "amount",
        "price",
        "unitPrice",
        "rawVolume",
        "rawAmount",
        "rawPrice",
        "rawUnitPrice",
    }
)


def build_envelope(
    *,
    event_type: str,
    environment: str,
    device_id: str,
    station_id: str,
    sequence: int,
    simulated: bool,
    deduplication_key: str,
    payload: dict[str, Any],
    occurred_at: str | None = None,
    pump_id: str | None = None,
    transaction_id: str | None = None,
    correlation_id: str | None = None,
    message_id: str | None = None,
) -> CloudMessageEnvelope:
    # Money/volume must remain integer scaled values (no floats).
    for key, value in payload.items():
        if key in _MONEY_VOLUME_KEYS and isinstance(value, float):
            raise ValueError(f"payload must not contain float money/volume: {key}")
        if isinstance(value, float) and key.lower() in {
            "volume",
            "amount",
            "price",
            "money",
        }:
            raise ValueError(f"payload must not contain float money/volume: {key}")
    now = utc_now_iso()
    return CloudMessageEnvelope(
        message_id=message_id or str(uuid4()),
        event_type=event_type,
        schema_version=MESSAGE_SCHEMA_VERSION,
        environment=environment.upper(),
        device_id=device_id,
        station_id=station_id,
        sequence=sequence,
        occurred_at=occurred_at or now,
        published_at=now,
        simulated=simulated,
        deduplication_key=deduplication_key,
        payload=payload,
        pump_id=pump_id,
        transaction_id=transaction_id,
        correlation_id=correlation_id,
    )


# Keep asdict available for debugging helpers.
_ = asdict
