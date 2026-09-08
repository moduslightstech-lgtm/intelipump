"""Convert sync_queue records into MQTT topic + envelope."""

from __future__ import annotations

from typing import Any

from intelipump_fdc.cloud.channel_map import enrich_transaction_payload
from intelipump_fdc.cloud.messages import CloudMessageEnvelope, build_envelope
from intelipump_fdc.cloud.qos import qos_for_event
from intelipump_fdc.cloud.topics import TopicBuilder
from intelipump_fdc.persistence.dto import SyncQueueRecord

# Completed sales are durable. In-progress fills are also published so the
# dashboard can stream volume/amount during a dispense (sidecar also polls
# ACTIVE rows if the controller has not queued FILLING_UPDATED yet).
PUBLISHABLE_QUEUE_EVENTS = frozenset({"TRANSACTION_COMPLETED", "FILLING_UPDATED"})


class DeliveryMapper:
    def __init__(
        self,
        *,
        topics: TopicBuilder,
        device_id: str,
        station_id: str,
        environment: str,
        simulated: bool,
        channel_mappings: dict | None = None,
    ) -> None:
        self._topics = topics
        self._device_id = device_id
        self._station_id = station_id
        self._environment = environment
        self._simulated = simulated
        self._channel_mappings = channel_mappings or {}
        self._seq = 0

    def next_sequence(self) -> int:
        self._seq += 1
        return self._seq

    def should_publish(self, record: SyncQueueRecord) -> bool:
        return record.event_type in PUBLISHABLE_QUEUE_EVENTS

    def map_record(
        self, record: SyncQueueRecord
    ) -> tuple[str, CloudMessageEnvelope, int]:
        if not self.should_publish(record):
            raise ValueError(f"queue event is not publishable: {record.event_type}")
        payload = dict(record.payload)
        if self._channel_mappings:
            payload = enrich_transaction_payload(payload, self._channel_mappings)
        event_type = record.event_type
        pump_id = _as_str(
            payload.get("pumpId") or payload.get("pump_id") or payload.get("logical_pump_id")
        )
        transaction_id = _as_str(
            payload.get("transaction_uuid") or payload.get("transaction_id")
        )
        topic = self._topic_for(event_type, pump_id=pump_id)
        envelope = build_envelope(
            event_type=event_type,
            environment=self._environment,
            device_id=self._device_id,
            station_id=self._station_id,
            sequence=self.next_sequence(),
            simulated=bool(payload.get("simulated", self._simulated)),
            deduplication_key=record.deduplication_key,
            payload=payload,
            pump_id=pump_id,
            transaction_id=transaction_id,
            correlation_id=_as_str(payload.get("correlation_id")),
            occurred_at=_as_str(payload.get("occurred_at")),
        )
        return topic, envelope, qos_for_event(event_type)

    def _topic_for(self, event_type: str, *, pump_id: str | None) -> str:
        if event_type.startswith("TRANSACTION") or event_type == "FILLING_UPDATED":
            return self._topics.transactions(self._station_id)
        if event_type.startswith("ALARM"):
            return self._topics.alarms(self._station_id)
        if event_type.startswith("AUDIT"):
            return self._topics.audit(self._station_id)
        if pump_id:
            return self._topics.pump_events(self._station_id, pump_id)
        return self._topics.transactions(self._station_id)


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
