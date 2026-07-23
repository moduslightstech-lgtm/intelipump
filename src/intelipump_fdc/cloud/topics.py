"""MQTT topic builder for LAB/PROD namespaces."""

from __future__ import annotations

import re

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_FORBIDDEN = ("+", "#", "..", "/", "\\", "\0")


class TopicError(ValueError):
    pass


def _validate_id(name: str, value: str) -> str:
    if not value or not value.strip():
        raise TopicError(f"{name} must not be empty")
    value = value.strip()
    for bad in _FORBIDDEN:
        if bad in value:
            raise TopicError(f"{name} contains forbidden character sequence: {bad!r}")
    if not _ID_RE.fullmatch(value):
        raise TopicError(f"{name} has invalid characters: {value!r}")
    return value


def _env_segment(environment: str) -> str:
    env = environment.strip().upper()
    if env == "LAB":
        return "lab"
    if env in {"PROD", "PRODUCTION"}:
        return "prod"
    raise TopicError(f"unsupported environment for topics: {environment!r}")


class TopicBuilder:
    """Build intelipump/{lab|prod}/... topics with identifier validation."""

    def __init__(self, *, environment: str) -> None:
        self.environment = environment.strip().upper()
        self._seg = _env_segment(self.environment)

    def _root(self) -> str:
        return f"intelipump/{self._seg}"

    def heartbeat(self, device_id: str) -> str:
        d = _validate_id("device_id", device_id)
        return f"{self._root()}/devices/{d}/heartbeat"

    def device_status(self, device_id: str) -> str:
        d = _validate_id("device_id", device_id)
        return f"{self._root()}/devices/{d}/status"

    def pump_events(self, station_id: str, pump_id: str) -> str:
        s = _validate_id("station_id", station_id)
        p = _validate_id("pump_id", pump_id)
        return f"{self._root()}/stations/{s}/pumps/{p}/events"

    def transactions(self, station_id: str) -> str:
        s = _validate_id("station_id", station_id)
        return f"{self._root()}/stations/{s}/transactions"

    def alarms(self, station_id: str) -> str:
        s = _validate_id("station_id", station_id)
        return f"{self._root()}/stations/{s}/alarms"

    def audit(self, station_id: str) -> str:
        s = _validate_id("station_id", station_id)
        return f"{self._root()}/stations/{s}/audit"

    def commands(self, station_id: str) -> str:
        s = _validate_id("station_id", station_id)
        return f"{self._root()}/stations/{s}/commands"

    def command_result(self, station_id: str, correlation_id: str) -> str:
        s = _validate_id("station_id", station_id)
        c = _validate_id("correlation_id", correlation_id)
        return f"{self._root()}/stations/{s}/commands/{c}/result"

    def lab_wildcard(self) -> str:
        if self._seg != "lab":
            raise TopicError("lab_wildcard only valid for LAB environment")
        return "intelipump/lab/#"
