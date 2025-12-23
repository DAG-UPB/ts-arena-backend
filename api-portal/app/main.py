import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Request
from fastapi.openapi.utils import get_openapi
from fastapi.openapi.docs import get_swagger_ui_html
from app.api.v1 import challenges
from app.core.config import Config
from app.database.connection import SessionLocal
from app.api.v1 import models as models_router
from app.api.v1 import users
from app.api.v1 import organizations
from app.api.v1 import forecasts
from app.database.challenges.challenge_repository import ChallengeRepository
# Import all models to ensure SQLAlchemy can resolve relationships
from app.database.forecasts.models import Forecast, ChallengeScore
from app.database.challenges.challenge import Challenge, ChallengeParticipant, ChallengeContextData
from app.database.models.model_info import ModelInfo
from app.database.auth.user import User
from app.database.data_portal.time_series import TimeSeriesModel, TimeSeriesDataModel
from app.services.challenge_service import ChallengeService
from app.scheduler.scheduler import ChallengeScheduler
from app.scheduler.dependencies import set_scheduler
from app.api.v1 import api_keys
from app.api.dependencies import require_auth
import asyncio

logger = logging.getLogger("api-portal")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))
    logger.addHandler(handler)
logger.propagate = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configuration-dependent level setting
    logger.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))
    app.state.logger = logger

    async with SessionLocal() as session:
        app.state.challenge_repo = ChallengeRepository(session)

        # Initialize scheduler first
        scheduler = None
        if Config.DATABASE_URL:
            try:
                scheduler = ChallengeScheduler(
                    database_url=Config.DATABASE_URL,
                    logger=app.state.logger,
                    max_restart_attempts=5,  # Auto-restart up to 5 times
                    restart_delay=5.0,  # Wait 5 seconds between restarts
                )
                await scheduler.start()
                # Set global scheduler reference for jobs
                set_scheduler(scheduler)
                try:
                    await scheduler.load_recurring_schedules(Config.CHALLENGE_SCHEDULE_FILE)
                except Exception:
                    app.state.logger.exception("Error loading challenge schedule config")
            except Exception as e:
                app.state.logger.error(f"Failed to initialize scheduler: {e}", exc_info=True)
                scheduler = None
        else:
            app.state.logger.warning("DATABASE_URL not set – Scheduler is disabled")
        app.state.challenge_scheduler = scheduler
        
        # Create ChallengeService with scheduler reference
        app.state.challenge_service = ChallengeService(session, scheduler=scheduler)

        yield

    # Cleanup on shutdown
    cs = getattr(app.state, "challenge_scheduler", None)
    if cs is not None:
        try:
            await asyncio.wait_for(cs.shutdown(), timeout=10.0)
        except asyncio.TimeoutError:
            app.state.logger.warning("Scheduler shutdown timed out after 10 seconds")
        except Exception as e:
            app.state.logger.error(f"Error during scheduler shutdown: {e}", exc_info=True)
        finally:
            set_scheduler(None)  # Clear global reference


app = FastAPI(
    title="API Portal for Time Series Forecasting",
    description="A portal for managing and accessing time series data sources and forecasts.",
    version="0.0.1",
    lifespan=lifespan,
    openapi_tags=[
        {
            "name": "api-keys",
            "description": "API Key management operations"
        },
        {
            "name": "challenges",
            "description": "Forecasting challenge operations"
        },
        {
            "name": "forecasts",
            "description": "Forecast upload and retrieval operations"
        },
        {
            "name": "models",
            "description": "Model information operations"
        },
        {
            "name": "users",
            "description": "User management operations"
        }
    ]
)

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    
    # Filter out admin routes for the public schema
    paths = openapi_schema.get("paths", {})
    public_paths = {}
    for path, methods in paths.items():
        new_methods = {}
        for method, details in methods.items():
            tags = details.get("tags", [])
            if "admin" not in tags:
                new_methods[method] = details
        if new_methods:
            public_paths[path] = new_methods
            
    openapi_schema["paths"] = public_paths
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

@app.get("/admin/openapi.json", include_in_schema=False)
async def get_admin_openapi():
    return get_openapi(
        title=app.title + " (Admin)",
        version=app.version,
        description="Admin API for internal services",
        routes=app.routes,
    )

@app.get("/admin/docs", include_in_schema=False)
async def get_admin_docs():
    return get_swagger_ui_html(
        openapi_url="/admin/openapi.json",
        title=app.title + " - Admin Docs",
    )

# Public endpoints (without authentication)
@app.get("/")
async def root():
    return {
        "message": "API Portal for Time Series Forecasting",
        "version": "0.0.1"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint for Docker containers"""
    return {"status": "healthy"}

# API Key Management (for internal services only)
app.include_router(api_keys.router, prefix="/api/v1")

# User Management (for internal services only)
app.include_router(users.router, prefix="/api/v1")
app.include_router(organizations.router, prefix="/api/v1")

# Protected routers with authentication
app.include_router(challenges.router, prefix="/api/v1", dependencies=[Depends(require_auth)])
app.include_router(models_router.router, prefix="/api/v1", dependencies=[Depends(require_auth)])

# Forecast router (has its own authentication in the endpoint)
app.include_router(forecasts.router, prefix="/api/v1")