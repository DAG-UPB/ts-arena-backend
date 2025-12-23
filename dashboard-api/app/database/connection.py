import os
import sys
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from app.core.config import settings


class DatabaseConnection:
    """DB Connection Manager"""
    
    def __init__(self):
        self.database_url = self._normalize_psycopg2_url(settings.DATABASE_URL)
        print(f"DEBUG: Normalized DATABASE_URL: {self.database_url}", file=sys.stderr)
    
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
        """Context manager for DB connections."""
        conn = None
        try:
            print("DEBUG: Attempting to connect to the database...", file=sys.stderr)
            conn = psycopg2.connect(self.database_url)
            print("DEBUG: Database connection successful.", file=sys.stderr)
            yield conn
        except Exception as e:
            print(f"ERROR: Database connection failed: {e}", file=sys.stderr)
            raise
        finally:
            if conn:
                conn.close()


# Singleton Instance
db_connection = DatabaseConnection()


def get_db_connection():
    """Dependency for FastAPI Endpoints."""
    with db_connection.get_connection() as conn:
        yield conn
