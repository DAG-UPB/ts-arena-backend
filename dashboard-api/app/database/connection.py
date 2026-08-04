import os
import sys
import threading
import psycopg2
import psycopg2.extras
import psycopg2.pool
from contextlib import contextmanager
# Starlette's base class, not `fastapi.HTTPException`: the latter subclasses it, so
# catching the base covers handlers raising either one.
from starlette.exceptions import HTTPException
from app.core.config import settings


class DatabaseConnection:
    """DB Connection Manager backed by a thread-safe connection pool.

    Endpoints run in FastAPI's threadpool, so several requests hold a
    connection at once. Opening a fresh connection per request cost a TCP
    handshake plus auth on every call and let the open-connection count grow
    with request concurrency; the pool bounds it instead.
    """

    def __init__(self):
        self.database_url = self._normalize_psycopg2_url(settings.DATABASE_URL)
        self._pool = None
        self._lock = threading.Lock()
        # psycopg2's pool raises "connection pool exhausted" rather than
        # waiting for a free connection. Requests hold a connection across more
        # than one threadpool task, so capping the threadpool does not bound the
        # number of holders. This semaphore does, and makes surplus requests
        # queue instead of failing.
        self._slots = threading.BoundedSemaphore(settings.DB_POOL_MAX)

    def _get_pool(self):
        """Build the pool on first use.

        Deliberately lazy: creating it at import time would make the container
        fail to boot whenever the database is briefly unreachable, and would
        require a live database just to import the app.
        """
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    self._pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=settings.DB_POOL_MIN,
                        maxconn=settings.DB_POOL_MAX,
                        dsn=self.database_url,
                    )
                    print(
                        f"DEBUG: DB pool ready (min={settings.DB_POOL_MIN}, "
                        f"max={settings.DB_POOL_MAX})",
                        file=sys.stderr,
                    )
        return self._pool

    @staticmethod
    def _normalize_psycopg2_url(url: str) -> str:
        """Replace incompatible driver prefixes for psycopg2."""
        if url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql+asyncpg://", "postgresql://", 1)
        if url.startswith("postgresql+psycopg2://"):
            return url.replace("postgresql+psycopg2://", "postgresql://", 1)
        if url.startswith("postgres+psycopg2://"):
            return url.replace("postgres+psycopg2://", "postgres://", 1)
        return url

    @contextmanager
    def get_connection(self):
        """Context manager yielding a pooled DB connection."""
        pool = self._get_pool()
        if not self._slots.acquire(timeout=settings.DB_POOL_WAIT_SECONDS):
            raise TimeoutError(
                f"No database connection available within "
                f"{settings.DB_POOL_WAIT_SECONDS}s (pool size {settings.DB_POOL_MAX})"
            )
        conn = None
        try:
            conn = pool.getconn()
            yield conn
        except HTTPException:
            # Not a database fault. A handler raising 404/401 is ordinary control
            # flow and leaves the connection clean, so it must not be logged as a
            # connection failure nor closed — `finally` rolls it back and returns
            # it to the pool like any other request. Closing here meant every 404
            # cost a fresh TCP handshake plus auth on the next request, which is
            # exactly the churn the pool exists to avoid.
            raise
        except Exception as e:
            print(f"ERROR: Database connection failed: {e}", file=sys.stderr)
            # A connection that errored may be left mid-transaction; drop it
            # from the pool rather than handing the broken state to the next
            # request.
            if conn is not None:
                pool.putconn(conn, close=True)
                conn = None
            raise
        finally:
            # The slot must be released even if returning the connection fails,
            # otherwise the pool bleeds capacity and eventually deadlocks.
            try:
                if conn is not None:
                    try:
                        # Readers only, but an aborted transaction would
                        # otherwise be inherited by the next user of this
                        # connection.
                        conn.rollback()
                    except Exception:
                        # Unusable connection — discard rather than reuse.
                        pool.putconn(conn, close=True)
                    else:
                        pool.putconn(conn)
            finally:
                self._slots.release()

    def close(self):
        """Close every pooled connection (called on app shutdown)."""
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None


# Singleton Instance
db_connection = DatabaseConnection()


def get_db_connection():
    """Dependency for FastAPI Endpoints."""
    with db_connection.get_connection() as conn:
        yield conn
