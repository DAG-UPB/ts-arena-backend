"""How `DatabaseConnection.get_connection` treats exceptions raised by the handler.

The context manager yields inside its own `try`, so anything a route handler raises passes
through it. An `HTTPException` is ordinary control flow (a 404 or 401) and leaves the
connection clean; a `psycopg2` error may leave it mid-transaction and must be discarded.
Before backend-73 both were treated as connection failures, so every 404 logged a spurious
"Database connection failed" and threw away a healthy pooled connection.
"""
import psycopg2
import pytest
from fastapi import HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.database.connection import DatabaseConnection


class FakePool:
    """Records how connections come back: `putconn(conn, close=True)` discards."""

    def __init__(self):
        self.returned = []   # (conn, close) pairs, in order
        self.handed_out = 0

    def getconn(self):
        self.handed_out += 1
        return FakeConn()

    def putconn(self, conn, close=False):
        self.returned.append((conn, close))


class FakeConn:
    def __init__(self, rollback_raises=False):
        self.rollback_raises = rollback_raises
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1
        if self.rollback_raises:
            raise psycopg2.OperationalError("rollback failed")


@pytest.fixture
def db(monkeypatch):
    """A DatabaseConnection wired to a fake pool, never touching a real database."""
    conn = DatabaseConnection()
    pool = FakePool()
    monkeypatch.setattr(conn, "_get_pool", lambda: pool)
    return conn, pool


def test_httpexception_returns_connection_to_pool(db, caplog):
    """A 404 must not close the connection and must not log a DB error."""
    conn, pool = db

    with pytest.raises(HTTPException):
        with conn.get_connection() as c:
            raise HTTPException(status_code=404, detail="Challenge definition not found")

    assert len(pool.returned) == 1
    _, closed = pool.returned[0]
    assert closed is False, "a 404 left the connection clean; it must be reused"
    assert "Database connection failed" not in caplog.text


def test_starlette_httpexception_also_passes_through(db, caplog):
    """FastAPI's HTTPException subclasses Starlette's; both must be treated alike."""
    conn, pool = db

    with pytest.raises(StarletteHTTPException):
        with conn.get_connection():
            raise StarletteHTTPException(status_code=401, detail="API Key missing")

    assert pool.returned[0][1] is False
    assert "Database connection failed" not in caplog.text


def test_real_db_error_still_discards_and_logs(db, caplog):
    """A genuine psycopg2 fault may leave a transaction open — keep discarding it."""
    conn, pool = db

    with caplog.at_level("ERROR", logger="app.database.connection"):
        with pytest.raises(psycopg2.OperationalError):
            with conn.get_connection():
                raise psycopg2.OperationalError("server closed the connection unexpectedly")

    assert any(closed for _, closed in pool.returned), "broken connection must be closed"
    assert "Database connection failed" in caplog.text


def test_httpexception_rolls_back_before_reuse(db):
    """A handler may raise after issuing a query; the next user must not inherit it."""
    conn, pool = db

    with pytest.raises(HTTPException):
        with conn.get_connection() as c:
            raise HTTPException(status_code=404, detail="not found")

    returned_conn, closed = pool.returned[0]
    assert returned_conn.rollbacks == 1
    assert closed is False


def test_slot_released_after_httpexception(db):
    """The semaphore must be freed, or the pool bleeds capacity and deadlocks."""
    conn, pool = db

    for _ in range(3):
        with pytest.raises(HTTPException):
            with conn.get_connection():
                raise HTTPException(status_code=404, detail="not found")

    # A fourth checkout still succeeds, so no slot leaked.
    with conn.get_connection():
        pass
    assert pool.handed_out == 4


def test_successful_request_returns_connection_open(db):
    conn, pool = db

    with conn.get_connection() as c:
        assert c is not None

    assert pool.returned[0][1] is False
