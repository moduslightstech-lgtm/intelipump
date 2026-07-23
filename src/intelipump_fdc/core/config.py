from __future__ import annotations

from enum import StrEnum
from functools import lru_cache

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ControllerMode(StrEnum):
    LISTEN_ONLY = "LISTEN_ONLY"
    BENCH_CONTROL = "BENCH_CONTROL"
    FIELD_CONTROL = "FIELD_CONTROL"
    LOCKED_OUT = "LOCKED_OUT"


class ControllerSettings(BaseModel):
    mode: ControllerMode = ControllerMode.LISTEN_ONLY
    device_id: str = "InteliPump-Lab-pi-001"
    station_id: str = "InteliPump-US-Lab"


class DartSettings(BaseModel):
    serial_port: str = "/tmp/dart-controller"
    baud_rate: int = 9600
    data_bits: int = 8
    parity: str = "ODD"
    stop_bits: int = 1
    response_timeout_ms: int = Field(default=25, ge=1, le=1000)


class SafetySettings(BaseModel):
    active_commands_enabled: bool = False
    remote_authorization_enabled: bool = False
    require_physical_control_enable: bool = True
    # Phase 8: LAB simulator command submission via API (virtual transport only).
    allow_lab_simulator_commands: bool = False


class DatabaseSettings(BaseModel):
    url: str = "sqlite+aiosqlite:///./data/intelipump.db"
    # Retention knobs only — deletion not implemented in Phase 7.
    raw_frames_retention_days: int = Field(default=30, ge=1)
    state_snapshot_retention_days: int = Field(default=14, ge=1)
    transaction_retention_days: int = Field(default=365, ge=1)
    audit_retention_days: int = Field(default=730, ge=1)
    delivered_sync_queue_retention_days: int = Field(default=30, ge=1)


class MqttSettings(BaseModel):
    """MQTT / cloud sync settings. Disabled by default (Phase 9)."""

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    client_id: str | None = None
    tls_enabled: bool = False
    ca_file: str | None = None
    client_cert: str | None = None
    client_key: str | None = None
    keepalive_seconds: int = Field(default=30, ge=5)
    connect_timeout_seconds: float = Field(default=10.0, ge=1.0)
    reconnect_min_delay_seconds: float = Field(default=1.0, ge=0.1)
    reconnect_max_delay_seconds: float = Field(default=60.0, ge=1.0)
    max_inflight: int = Field(default=20, ge=1)
    clean_session: bool = True
    session_expiry_seconds: int = Field(default=0, ge=0)
    default_qos: int = Field(default=1, ge=0, le=2)
    heartbeat_interval_seconds: float = Field(default=30.0, ge=5.0)
    command_subscription_enabled: bool = False
    topic_environment: str | None = None  # defaults to settings.environment
    # Fill publish throttling
    fill_min_interval_seconds: float = Field(default=2.0, ge=0.0)
    fill_min_volume_delta: int = Field(default=100, ge=0)
    fill_min_amount_delta: int = Field(default=100, ge=0)
    # Sync worker
    sync_batch_size: int = Field(default=10, ge=1, le=100)
    sync_poll_interval_seconds: float = Field(default=1.0, ge=0.1)
    sync_stale_lock_seconds: int = Field(default=60, ge=5)
    sync_max_attempts: int = Field(default=20, ge=1)


class ApiSettings(BaseModel):
    """Local LAB/trusted-network API settings (Phase 8)."""

    host: str = "127.0.0.1"
    port: int = 8000
    max_page_size: int = Field(default=100, ge=1, le=1000)
    default_page_size: int = Field(default=25, ge=1, le=1000)
    max_sse_subscribers: int = Field(default=32, ge=1)
    max_ws_subscribers: int = Field(default=32, ge=1)
    event_queue_size: int = Field(default=128, ge=8)
    stream_keepalive_seconds: float = Field(default=15.0, ge=1.0)
    max_request_body_bytes: int = Field(default=1_048_576, ge=1024)
    # When true, lifespan starts the controller poll loop (needs transport).
    start_controller_loop: bool = False
    controller_addresses: str = "1,2"
    simulated: bool = True


class BenchSettings(BaseModel):
    """Phase 10 RS-485 office bench (two USB adapters, LAB only)."""

    name: str = "us-office-rs485-01"
    controller_port: str = ""
    simulator_port: str = ""
    controller_adapter_stable_id: str | None = None
    simulator_adapter_stable_id: str | None = None
    baud_rate: int = 9600
    parity: str = "ODD"
    stop_bits: int = 1
    response_timeout_ms: int = Field(default=100, ge=1, le=2000)
    protocol_target_ms: int = Field(default=25, ge=1, le=1000)
    turnaround_delay_ms: float = Field(default=2.0, ge=0.0)
    inter_frame_delay_ms: float = Field(default=5.0, ge=0.0)
    adapter_type: str = "usb-rs485-auto"
    automatic_direction_control: bool = True
    termination_enabled: bool = False
    bias_enabled: bool = False
    ground_reference_connected: bool = True
    duration_s: float = Field(default=30.0, ge=1.0)
    exclusive_open: bool = True


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="INTELIPUMP_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )
    environment: str = "LAB"
    controller: ControllerSettings = ControllerSettings()
    dart: DartSettings = DartSettings()
    safety: SafetySettings = SafetySettings()
    database: DatabaseSettings = DatabaseSettings()
    mqtt: MqttSettings = MqttSettings()
    api: ApiSettings = ApiSettings()
    bench: BenchSettings = BenchSettings()


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    if (
        settings.controller.mode == ControllerMode.LISTEN_ONLY
        and settings.safety.active_commands_enabled
    ):
        raise ValueError("Active commands must be disabled in LISTEN_ONLY mode")
    if (
        settings.environment.upper() == "LAB"
        and not settings.controller.station_id.endswith("-Lab")
    ):
        raise ValueError("LAB station_id must end with '-Lab'")
    return settings
