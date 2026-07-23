"""Optional Mosquitto integration tests."""

from __future__ import annotations

import asyncio
import json
import socket
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from intelipump_fdc.cloud.mqtt.client import PahoMqttClient
from intelipump_fdc.cloud.mqtt.config import MqttClientConfig
from intelipump_fdc.cloud.topics import TopicBuilder


def _mosquitto_available(host: str = "127.0.0.1", port: int = 1883) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.mqtt_integration,
    pytest.mark.skipif(
        not _mosquitto_available(),
        reason="Mosquitto not available on 127.0.0.1:1883",
    ),
]


@pytest.mark.asyncio
async def test_paho_connect_publish_subscribe() -> None:
    topics = TopicBuilder(environment="LAB")
    cfg = MqttClientConfig(
        host="127.0.0.1",
        port=1883,
        client_id=f"lab-itest-{datetime.now(UTC).timestamp()}",
        username=None,
        password=None,
        tls_enabled=False,
        ca_file=None,
        client_cert=None,
        client_key=None,
        keepalive_seconds=30,
        connect_timeout_seconds=5.0,
        reconnect_min_delay_seconds=1.0,
        reconnect_max_delay_seconds=5.0,
        max_inflight=10,
        clean_session=True,
        default_qos=1,
    )
    received: list[str] = []

    async def on_msg(message: object) -> None:
        received.append(getattr(message, "topic", ""))

    pub = PahoMqttClient(cfg)
    sub = PahoMqttClient(replace(cfg, client_id=cfg.client_id + "-sub"))
    sub.set_message_handler(on_msg)
    async with sub:
        await sub.subscribe(topics.lab_wildcard(), qos=0)
        async with pub:
            topic = topics.heartbeat("InteliPump-Lab-pi-001")
            await pub.publish(topic, json.dumps({"ping": True}), qos=0)
            for _ in range(20):
                if received:
                    break
                await asyncio.sleep(0.1)
    assert any("heartbeat" in t for t in received)
