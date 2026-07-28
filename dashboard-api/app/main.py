import anyio.to_thread
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.database.connection import db_connection
from app.api.v1 import models, health, definitions, rounds

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


@app.on_event("startup")
async def startup_event():
    # Endpoints are sync `def`, so FastAPI runs them in anyio's threadpool.
    # Its default of 40 threads exceeds the DB pool, and psycopg2 raises
    # "connection pool exhausted" rather than waiting for a free connection.
    # Capping the threadpool to the pool size makes surplus requests queue in
    # anyio instead, which waits.
    anyio.to_thread.current_default_thread_limiter().total_tokens = settings.DB_POOL_MAX

    print(f"🚀 {settings.API_TITLE} v{settings.API_VERSION} started")
    print(f"📊 Database: {settings.DATABASE_URL.split('@')[-1] if '@' in settings.DATABASE_URL else 'configured'}")


@app.on_event("shutdown")
async def shutdown_event():
    db_connection.close()
    print(f"👋 {settings.API_TITLE} shutting down")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
