"""Device heartbeat publisher (ephemeral; not durable-queued)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import platform
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from intelipump_fdc.cloud.messages import build_envelope
from intelipump_fdc.cloud.mqtt.base import MqttClient
from intelipump_fdc.cloud.mqtt.errors import MqttError
from intelipump_fdc.cloud.qos import qos_for_event
from intelipump_fdc.cloud.schemas import HeartbeatPayload
from intelipump_fdc.cloud.topics import TopicBuilder


@dataclass
class HeartbeatService:
    mqtt: MqttClient
    topics: TopicBuilder
    device_id: str
    station_id: str
    environment: str
    simulated: bool
    interval_seconds: float = 30.0
    software_version: str = "0.1.0"
    payload_provider: Any = None  # callable returning dict extras
    last_published_at: datetime | None = None
    _task: asyncio.Task[None] | None = None
    _stop: asyncio.Event = field(default_factory=asyncio.Event)
    _seq: int = 0
    # Only keep intent for latest offline heartbeat (not unbounded queue).
    _pending_latest: dict[str, Any] | None = None

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="mqtt-heartbeat")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            await self.publish_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval_seconds)
            except TimeoutError:
                continue

    async def publish_once(self) -> bool:
        extras: dict[str, Any] = {}
        if callable(self.payload_provider):
            result = self.payload_provider()
            if asyncio.iscoroutine(result):
                result = await result
            extras = dict(result)
        body = HeartbeatPayload(
            deviceId=self.device_id,
            stationId=self.station_id,
            hostname=platform.node() or "unknown",
            environment=self.environment,
            controllerMode=str(extras.get("controllerMode", "LISTEN_ONLY")),
            status=str(extras.get("status", "ONLINE")),
            timestamp=datetime.now(UTC).isoformat(),
            uptimeSeconds=float(extras.get("uptimeSeconds", 0.0)),
            softwareVersion=self.software_version,
            databaseStatus=str(extras.get("databaseStatus", "UNKNOWN")),
            mqttConnectionStatus=(
                "CONNECTED" if self.mqtt.is_connected else "DISCONNECTED"
            ),
            controllerLoopRunning=bool(extras.get("controllerLoopRunning", False)),
            transportKind=extras.get("transportKind"),
            transportOpen=extras.get("transportOpen"),
            configuredPumpCount=int(extras.get("configuredPumpCount", 0)),
            healthyPumpCount=int(extras.get("healthyPumpCount", 0)),
            degradedPumpCount=int(extras.get("degradedPumpCount", 0)),
            disconnectedPumpCount=int(extras.get("disconnectedPumpCount", 0)),
            pendingSyncCount=int(extras.get("pendingSyncCount", 0)),
            unresolvedTransactionCount=int(
                extras.get("unresolvedTransactionCount", 0)
            ),
            simulated=self.simulated,
        ).model_dump()
        self._seq += 1
        envelope = build_envelope(
            event_type="HEARTBEAT",
            environment=self.environment,
            device_id=self.device_id,
            station_id=self.station_id,
            sequence=self._seq,
            simulated=self.simulated,
            deduplication_key=f"heartbeat:{self.device_id}:latest",
            payload=body,
        )
        topic = self.topics.heartbeat(self.device_id)
        if not self.mqtt.is_connected:
            # Replace previous pending heartbeat; do not grow unbounded.
            self._pending_latest = envelope.to_dict()
            return False
        try:
            # Flush pending replacement if any.
            to_send = self._pending_latest or envelope.to_dict()
            self._pending_latest = None
            await self.mqtt.publish(
                topic,
                json.dumps(to_send, separators=(",", ":")),
                qos=qos_for_event("HEARTBEAT"),
                retain=False,
            )
            self.last_published_at = datetime.now(UTC)
            return True
        except MqttError:
            self._pending_latest = envelope.to_dict()
            return False
