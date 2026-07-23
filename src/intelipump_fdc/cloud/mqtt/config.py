"""MQTT configuration helpers (no secrets in repr/logs)."""

from __future__ import annotations

from dataclasses import dataclass

from intelipump_fdc.core.config import MqttSettings


@dataclass(frozen=True, slots=True)
class MqttClientConfig:
    host: str
    port: int
    client_id: str
    username: str | None
    password: str | None
    tls_enabled: bool
    ca_file: str | None
    client_cert: str | None
    client_key: str | None
    keepalive_seconds: int
    connect_timeout_seconds: float
    reconnect_min_delay_seconds: float
    reconnect_max_delay_seconds: float
    max_inflight: int
    clean_session: bool
    default_qos: int

    def sanitized_host(self) -> str:
        return f"{self.host}:{self.port}"

    def __repr__(self) -> str:
        return (
            f"MqttClientConfig(host={self.host!r}, port={self.port}, "
            f"client_id={self.client_id!r}, tls={self.tls_enabled}, "
            f"username={'***' if self.username else None})"
        )


def mqtt_config_from_settings(
    settings: MqttSettings, *, device_id: str
) -> MqttClientConfig:
    client_id = settings.client_id or f"{device_id}-mqtt"
    return MqttClientConfig(
        host=settings.host,
        port=settings.port,
        client_id=client_id,
        username=settings.username,
        password=settings.password,
        tls_enabled=settings.tls_enabled,
        ca_file=settings.ca_file,
        client_cert=settings.client_cert,
        client_key=settings.client_key,
        keepalive_seconds=settings.keepalive_seconds,
        connect_timeout_seconds=settings.connect_timeout_seconds,
        reconnect_min_delay_seconds=settings.reconnect_min_delay_seconds,
        reconnect_max_delay_seconds=settings.reconnect_max_delay_seconds,
        max_inflight=settings.max_inflight,
        clean_session=settings.clean_session,
        default_qos=settings.default_qos,
    )
