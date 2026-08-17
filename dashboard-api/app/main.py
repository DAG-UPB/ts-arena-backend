import logging

from app.core.logging_setup import configure_logging, install_access_log_filter

# Deliberately before the imports below. This service had no logging configuration at
# all -- every line was a print() and nothing carried a timestamp -- and importing the
# routers emits a block of pydantic protected-namespace warnings straight to stderr.
# Configuring logging first is what puts those on the timestamped stream instead.
configure_logging("dashboard-api")
logger = logging.getLogger("dashboard-api")

from fastapi import FastAPI  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402

from app.core.config import settings  # noqa: E402
from app.database.connection import db_connection  # noqa: E402
from app.api.v1 import models, health, definitions, rounds  # noqa: E402

app = FastAPI(
    title=settings.API_TITLE,
    description="TS-Arena Dashboard API - Provides access to challenge and forecast data",
    version=settings.API_VERSION,
    debug=settings.DEBUG
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(definitions.router)
app.include_router(rounds.router)

app.include_router(models.router)

# After the routers above: the filter reads the served surface off app.routes.
install_access_log_filter(app)


@app.on_event("startup")
async def startup_event():
    logger.info("%s v%s started", settings.API_TITLE, settings.API_VERSION)
    logger.info(
        "Database: %s",
        settings.DATABASE_URL.split('@')[-1] if '@' in settings.DATABASE_URL else 'configured',
    )


@app.on_event("shutdown")
async def shutdown_event():
    db_connection.close()
    logger.info("%s shutting down", settings.API_TITLE)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
