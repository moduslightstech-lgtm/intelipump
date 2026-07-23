"""MQTT errors."""

from __future__ import annotations


class MqttError(Exception):
    pass


class MqttNotConnectedError(MqttError):
    pass


class MqttPublishError(MqttError):
    pass
