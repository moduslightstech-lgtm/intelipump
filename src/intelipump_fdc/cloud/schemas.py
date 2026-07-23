"""Cloud payload schemas and validators."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class CloudCommandInbound(BaseModel):
    commandId: str = Field(min_length=1, max_length=128)
    correlationId: str = Field(min_length=1, max_length=128)
    stationId: str = Field(min_length=1, max_length=128)
    pumpId: str = Field(min_length=1, max_length=128)
    commandType: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    simulatorOnly: bool = True
    createdAt: datetime
    expiresAt: datetime | None = None
    requestedBy: str | None = None
    schemaVersion: str = "1.0"
    environment: str = "LAB"

    @field_validator("payload")
    @classmethod
    def no_float_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        for k, v in value.items():
            if isinstance(v, float):
                raise ValueError(f"float not allowed in payload: {k}")
        return value


class HeartbeatPayload(BaseModel):
    deviceId: str
    stationId: str
    hostname: str
    environment: str
    controllerMode: str
    status: str
    timestamp: str
    uptimeSeconds: float
    softwareVersion: str
    databaseStatus: str
    mqttConnectionStatus: str
    controllerLoopRunning: bool
    transportKind: str | None
    transportOpen: bool | None
    configuredPumpCount: int
    healthyPumpCount: int
    degradedPumpCount: int
    disconnectedPumpCount: int
    pendingSyncCount: int
    unresolvedTransactionCount: int
    simulated: bool
