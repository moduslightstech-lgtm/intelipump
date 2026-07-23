from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from intelipump_fdc.api.errors import (
    ApiError,
    api_error_handler,
    http_exception_handler,
    validation_exception_handler,
)
from intelipump_fdc.api.lifespan import app_lifespan
from intelipump_fdc.api.middleware import CorrelationIdMiddleware
from intelipump_fdc.api.routes import alarms as alarms_routes
from intelipump_fdc.api.routes import commands as commands_routes
from intelipump_fdc.api.routes import controller as controller_routes
from intelipump_fdc.api.routes import events as events_routes
from intelipump_fdc.api.routes import health as health_routes
from intelipump_fdc.api.routes import pumps as pumps_routes
from intelipump_fdc.api.routes import transactions as transactions_routes
from intelipump_fdc.core.config import get_settings
from intelipump_fdc.core.logging import configure_logging


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.environment)
    app = FastAPI(
        title="InteliPump FDC",
        version="0.1.0",
        description=(
            "Raspberry Pi Wayne DART forecourt controller — "
            "Phase 8 local LAB/trusted-network API"
        ),
        lifespan=app_lifespan,
    )
    app.add_middleware(CorrelationIdMiddleware)
    app.add_exception_handler(ApiError, api_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, validation_exception_handler)  # type: ignore[arg-type]

    prefix = "/api/v1"
    app.include_router(health_routes.router, prefix=prefix)
    app.include_router(controller_routes.router, prefix=prefix)
    app.include_router(pumps_routes.router, prefix=prefix)
    app.include_router(transactions_routes.router, prefix=prefix)
    app.include_router(events_routes.router, prefix=prefix)
    app.include_router(commands_routes.router, prefix=prefix)
    app.include_router(alarms_routes.alarms_router, prefix=prefix)
    app.include_router(alarms_routes.audit_router, prefix=prefix)
    return app
