from __future__ import annotations

import uvicorn

from intelipump_fdc.api.app import create_app
from intelipump_fdc.core.config import get_settings

app = create_app()


def run() -> None:
    settings = get_settings()
    uvicorn.run(
        "intelipump_fdc.main:app",
        host=settings.api.host,
        port=settings.api.port,
        reload=False,
    )


if __name__ == "__main__":
    run()
