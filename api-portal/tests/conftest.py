"""Test-collection setup.

Some modules (anything importing `app.database.connection`, transitively pulled in by
`app.database.*` repository/model modules) call `create_async_engine(Config.DATABASE_URL)`
at import time. That only needs a syntactically valid URL to succeed — it does not connect
to any database. Set harmless defaults here, before test modules are imported, so tests
that exercise DB-adjacent code (e.g. by importing a script/service module for its pure
helper functions) don't require a real `.env`/`DATABASE_URL` to even collect.
"""
import os

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test")
os.environ.setdefault("API_KEY", "test-api-key")
