"""Transport-layer errors."""

from __future__ import annotations


class TransportError(Exception):
    """Base transport failure."""


class TransportNotOpenError(TransportError):
    """Operation attempted while transport is closed."""


class TransportConfigError(TransportError, ValueError):
    """Invalid serial or transport configuration."""


class TransportClosedError(TransportError):
    """Transport closed unexpectedly during I/O."""
