import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Request
from fastapi.openapi.utils import get_openapi
from fastapi.openapi.docs import get_swagger_ui_html
from app.api.v1 import challenges
from app.core.config import Config
from app.core.logging_setup import configure_logging, install_access_log_filter
from app.api.v1 import models as models_router
from app.api.v1 import users
from app.api.v1 import organizations
from app.api.v1 import forecasts
from app.database.forecasts.models import Forecast, ChallengeScore
from app.database.challenges.challenge import ChallengeRound, ChallengeDefinition, ChallengeParticipant, ChallengeContextData
from app.database.models.model_info import ModelInfo
from app.database.auth.user import User
from app.database.data_portal.time_series import TimeSeriesModel, TimeSeriesDataModel
from app.scheduler.scheduler import ChallengeScheduler
from app.scheduler.dependencies import set_scheduler
from app.api.v1 import api_keys
from app.api.dependencies import require_auth
from sqlalchemy import text
from app.database.connection import engine
import asyncio

# Configure the root logger before anything else logs. This used to configure only the
# logger named "api-portal", which left every other logger -- including the one the
# scheduler jobs use -- with no handler and a WARNING threshold. See logging_setup.
configure_logging("api-portal")
logger = logging.getLogger("api-portal")

async def wait_for_db(logger, max_retries=10, delay=3.0):
    for attempt in range(1, max_retries + 1):
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info("Database connection established successfully!")
            return True
        except Exception as e:
            logger.warning(f"Database not ready yet (attempt {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(delay)

    logger.error("Could not connect to database after maximum retries.")
    return False


# Idempotent schema patches applied on startup. Each statement must be
# safe to run repeatedly (use IF NOT EXISTS / IF EXISTS). This is a
# pragmatic bridge for the dev DB — init_db.sql remains the source of
# truth for fresh databases.
_SCHEMA_PATCHES = (
    "ALTER TABLE models.model_info ADD COLUMN IF NOT EXISTS paper_url TEXT",
    "ALTER TABLE models.model_info ADD COLUMN IF NOT EXISTS repo_url TEXT",
    "ALTER TABLE models.model_info ADD COLUMN IF NOT EXISTS website_url TEXT",
    "ALTER TABLE models.model_info ADD COLUMN IF NOT EXISTS description TEXT",
    "ALTER TABLE models.model_info ADD COLUMN IF NOT EXISTS arxiv_id TEXT",
    # Probabilistic evaluation (Scaled Quantile Loss). These keep the app from
    # crashing after a dev-DB restore-from-prod wipe; the fuller migration
    # (2026_sql_score.sql) also rebuilds round_model_scores + the ranking views.
    "ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS sql_score DOUBLE PRECISION",
    "ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS sql_per_quantile JSONB",
    "ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS has_quantiles BOOLEAN",
    "ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS quantile_levels_count INTEGER",
    "ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS quantile_crossing_count INTEGER",
    "ALTER TABLE forecasts.daily_rankings ADD COLUMN IF NOT EXISTS metric TEXT NOT NULL DEFAULT 'mase'",
    "ALTER TABLE forecasts.daily_rankings ADD COLUMN IF NOT EXISTS avg_sql DOUBLE PRECISION",
    "ALTER TABLE forecasts.daily_rankings ADD COLUMN IF NOT EXISTS sql_std DOUBLE PRECISION",
    # Widen the daily_rankings unique index to include `metric` (mase- and sql-ranked
    # snapshots must coexist). Rebuild only if the current index lacks metric — no per-boot churn.
    """DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'forecasts' AND indexname = 'idx_daily_rankings_unique'
          AND indexdef LIKE '%metric%'
      ) THEN
        DROP INDEX IF EXISTS forecasts.idx_daily_rankings_unique;
        CREATE UNIQUE INDEX idx_daily_rankings_unique ON forecasts.daily_rankings
          (calculation_date, model_id, scope_type, COALESCE(scope_id, ''), metric);
      END IF;
    END $$""",
    # Rank positions must be unique within a (date, scope, metric) leaderboard —
    # a duplicate means a recompute left stale rows behind. Guarded: while such
    # duplicates still exist the index cannot be built, and failing here would
    # roll back the whole patch batch, so warn instead and retry next boot.
    """DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'forecasts' AND indexname = 'idx_daily_rankings_rank_unique'
      ) THEN
        BEGIN
          CREATE UNIQUE INDEX idx_daily_rankings_rank_unique ON forecasts.daily_rankings
            (calculation_date, scope_type, COALESCE(scope_id, ''), metric, rank_position);
        EXCEPTION WHEN unique_violation THEN
          RAISE WARNING 'idx_daily_rankings_rank_unique not created: duplicate rank_position rows exist in forecasts.daily_rankings — clean them up, index will be created on next startup';
        END;
      END IF;
    END $$""",
)


async def apply_schema_patches(logger):
    try:
        async with engine.begin() as conn:
            for stmt in _SCHEMA_PATCHES:
                await conn.execute(text(stmt))
        logger.info("Schema patches applied (%d statements).", len(_SCHEMA_PATCHES))
    except Exception as e:
        # Non-fatal: log loudly so we notice, but don't block startup —
        # the api can still serve existing endpoints if a patch fails.
        logger.error("Schema patch failed: %s", e, exc_info=True)


async def apply_metadata_seed(logger):
    """Backfill curated paper/repo/website/arxiv_id for reference models.

    Two-step, idempotent (COALESCE preserves any user-set value):

    1. Match by ``name`` — the verbatim registration name from
       ts-arena-models/config.json (e.g. ``"google/timesfm-2.0-500m-pytorch"``).
       This gives per-version precision (Chronos-2 vs Chronos-Bolt).
    2. For rows that still have NULL paper/repo/website (newly registered
       variants, or families we haven't catalogued per-name), fall back to
       matching on ``model_family``.

    See ``app.data.model_metadata_seed`` for the seed data and background.
    ``generate_readable_id`` appends a random suffix, so we
    cannot key the seed by ``readable_id``.
    """
    try:
        from app.data.model_metadata_seed import (
            MODEL_METADATA_SEED,
            MODEL_FAMILY_FALLBACK,
        )
    except Exception as e:
        logger.warning("Could not load model metadata seed: %s", e)
        return

    name_updates = 0
    family_updates = 0
    try:
        async with engine.begin() as conn:
            # Diagnostic: log what's actually in the table so we know
            # whether the seed key shape matches reality.
            try:
                count_row = (
                    await conn.execute(
                        text("SELECT COUNT(*) FROM models.model_info")
                    )
                ).scalar_one()
                sample_rows = (
                    await conn.execute(
                        text(
                            "SELECT name, model_family FROM models.model_info "
                            "ORDER BY id LIMIT 10"
                        )
                    )
                ).fetchall()
                logger.info(
                    "Metadata seed diagnostic: %d total rows; sample (name, family): %s",
                    count_row,
                    [(r[0], r[1]) for r in sample_rows],
                )
            except Exception as diag_err:
                logger.warning("Metadata seed diagnostic skipped: %s", diag_err)

            # Step 1: exact `name` match.
            for name, meta in MODEL_METADATA_SEED.items():
                result = await conn.execute(
                    text(
                        """
                        UPDATE models.model_info
                           SET paper_url   = COALESCE(paper_url,   :paper_url),
                               arxiv_id    = COALESCE(arxiv_id,    :arxiv_id),
                               repo_url    = COALESCE(repo_url,    :repo_url),
                               website_url = COALESCE(website_url, :website_url)
                         WHERE name = :name
                        """
                    ),
                    {
                        "name":        name,
                        "paper_url":   meta.get("paper_url"),
                        "arxiv_id":    meta.get("arxiv_id"),
                        "repo_url":    meta.get("repo_url"),
                        "website_url": meta.get("website_url"),
                    },
                )
                name_updates += result.rowcount or 0

            # Step 2: family fallback for any row still NULL.
            for family, meta in MODEL_FAMILY_FALLBACK.items():
                result = await conn.execute(
                    text(
                        """
                        UPDATE models.model_info
                           SET paper_url   = COALESCE(paper_url,   :paper_url),
                               arxiv_id    = COALESCE(arxiv_id,    :arxiv_id),
                               repo_url    = COALESCE(repo_url,    :repo_url),
                               website_url = COALESCE(website_url, :website_url)
                         WHERE model_family = :family
                           AND (
                                paper_url   IS NULL
                             OR arxiv_id    IS NULL
                             OR repo_url    IS NULL
                             OR website_url IS NULL
                           )
                        """
                    ),
                    {
                        "family":      family,
                        "paper_url":   meta.get("paper_url"),
                        "arxiv_id":    meta.get("arxiv_id"),
                        "repo_url":    meta.get("repo_url"),
                        "website_url": meta.get("website_url"),
                    },
                )
                family_updates += result.rowcount or 0

        logger.info(
            "Model metadata seed applied — name match: %d row(s) across %d names; "
            "family fallback: %d row(s) across %d families.",
            name_updates, len(MODEL_METADATA_SEED),
            family_updates, len(MODEL_FAMILY_FALLBACK),
        )
    except Exception as e:
        logger.error("Metadata seed failed: %s", e, exc_info=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.logger = logger
    
    # Wait for database to be ready before starting other components
    if Config.DATABASE_URL:
        db_ready = await wait_for_db(logger)
        if not db_ready:
            logger.warning("Starting API without stable database connection.")
        else:
            await apply_schema_patches(logger)
            await apply_metadata_seed(logger)

    # Initialize scheduler (uses its own DB connection pool, not SessionLocal)
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
    
    # Note: ChallengeRepository and ChallengeService should be created per-request,
    # not stored in app.state with a long-lived session
    
    try:
        yield
    finally:
        # Cleanup on shutdown - scheduler first, before any other cleanup
        cs = getattr(app.state, "challenge_scheduler", None)
        if cs is not None:
            try:
                # Signal shutdown first to stop new jobs
                set_scheduler(None)
                await asyncio.wait_for(cs.shutdown(), timeout=10.0)
            except asyncio.TimeoutError:
                app.state.logger.warning("Scheduler shutdown timed out after 10 seconds")
            except asyncio.CancelledError:
                app.state.logger.warning("Scheduler shutdown was cancelled")
            except Exception as e:
                app.state.logger.error(f"Error during scheduler shutdown: {e}", exc_info=True)


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

app.include_router(api_keys.router, prefix="/api/v1")

app.include_router(users.router, prefix="/api/v1")
app.include_router(organizations.router, prefix="/api/v1")

app.include_router(challenges.router, prefix="/api/v1", dependencies=[Depends(require_auth)])
app.include_router(models_router.router, prefix="/api/v1", dependencies=[Depends(require_auth)])

app.include_router(forecasts.router, prefix="/api/v1")

# Must come after every include_router above: the filter reads the served surface off
# app.routes so it cannot go stale when a router is added.
install_access_log_filter(app)
