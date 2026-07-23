"""Persistence-layer errors."""

from __future__ import annotations


class PersistenceError(Exception):
    """Base persistence failure."""


class SchemaError(PersistenceError):
    """Schema init / migration failure."""


class DuplicateEntityError(PersistenceError):
    """Unique constraint / deduplication conflict."""


class PersistenceQueueFullError(PersistenceError):
    """Bounded persistence worker queue is full."""


class AuditChainError(PersistenceError):
    """Audit hash-chain verification failure."""
