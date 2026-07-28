import os
import sys
import threading
import psycopg2
import psycopg2.extras
import psycopg2.pool
from contextlib import contextmanager
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
        conn = None
        try:
            conn = pool.getconn()
            yield conn
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
            if conn is not None:
                # Readers only, but an aborted transaction would otherwise be
                # inherited by whoever gets this connection next.
                conn.rollback()
                pool.putconn(conn)

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
