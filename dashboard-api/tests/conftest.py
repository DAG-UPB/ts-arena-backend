"""Test-collection setup.

`app.core.config` builds its settings from the environment at import time, so anything
importing `app.database.connection` needs a syntactically valid `DATABASE_URL` just to be
collected. It is never connected to — these tests drive the pool through fakes. Set
harmless defaults here, before test modules are imported, so the suite does not require a
real `.env`.
"""
import os

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("API_KEY", "test-api-key")
