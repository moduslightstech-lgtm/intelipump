"""FastAPI dependencies."""

from __future__ import annotations

from fastapi import Request

from intelipump_fdc.api.errors import ApiError
from intelipump_fdc.api.state import AppState
from intelipump_fdc.core.config import Settings


def get_app_state(request: Request) -> AppState:
    state = getattr(request.app.state, "app_state", None)
    if not isinstance(state, AppState):
        raise ApiError(
            code="CONTROLLER_UNAVAILABLE",
            message="Application state is not initialized",
            status_code=503,
        )
    return state


def get_settings_dep(request: Request) -> Settings:
    return get_app_state(request).settings
