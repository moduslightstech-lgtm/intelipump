"""Retention configuration models only (no deletion implemented)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RetentionSettings(BaseModel):
    """Future retention knobs. Unresolved txs and undelivered queue items
    must never be deleted by automated retention."""

    raw_frames_days: int = Field(default=30, ge=1)
    state_snapshots_days: int = Field(default=14, ge=1)
    transactions_days: int = Field(default=365, ge=1)
    audit_days: int = Field(default=730, ge=1)
    delivered_sync_queue_days: int = Field(default=30, ge=1)
    # Hard rule documented for implementers:
    protect_unresolved_transactions: bool = True
    protect_undelivered_sync_items: bool = True
