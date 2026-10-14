"""How `DatabaseConnection.get_connection` checks connections out and treats exceptions.

The context manager yields inside its own `try`, so anything a route handler raises passes
through it. An `HTTPException` is ordinary control flow (a 404 or 401) and leaves the
connection clean; a `psycopg2` error may leave it mid-transaction and must be discarded.
Before backend-73 both were treated as connection failures, so every 404 logged a spurious
"Database connection failed" and threw away a healthy pooled connection.

On checkout every connection is probed first. One the server or network closed while it sat
idle in the pool is discarded and replaced, instead of failing the request with a 500.
"""
import psycopg2
import psycopg2.pool
import pytest
from fastapi import HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.config import settings
from app.database.connection import KEEPALIVES, DatabaseConnection


class FakePool:
    """Records how connections come back: `putconn(conn, close=True)` discards.

    Hands out the connections queued in `idle` first, then fresh live ones.
    """

    def __init__(self):
        self.idle = []
        self.returned = []   # (conn, close) pairs, in order
        self.handed_out = 0

    def getconn(self):
        self.handed_out += 1
        return self.idle.pop(0) if self.idle else FakeConn()

    def putconn(self, conn, close=False):
        self.returned.append((conn, close))


class FakeConn:
    def __init__(self, rollback_raises=False, dead=False):
        self.rollback_raises = rollback_raises
        self.dead = dead
        self.autocommit = False
        self.executed = []   # (sql, autocommit at the time) pairs
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor(self)

    def rollback(self):
        self.rollbacks += 1
        if self.rollback_raises:
            raise psycopg2.OperationalError("rollback failed")


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql):
        if self.conn.dead:
            raise psycopg2.OperationalError("server closed the connection unexpectedly")
        self.conn.executed.append((sql, self.conn.autocommit))


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


def test_dead_connection_on_checkout_is_replaced(db, caplog):
    """A connection closed while idle is discarded; the request gets a live one."""
    conn, pool = db
    dead = FakeConn(dead=True)
    pool.idle = [dead]

    with caplog.at_level("WARNING", logger="app.database.connection"):
        with conn.get_connection() as c:
            assert c is not dead

    assert pool.returned[0] == (dead, True), "the dead connection must be closed"
    assert pool.returned[1] == (c, False)
    assert "Discarded a dead pooled connection" in caplog.text
    assert "Database connection failed" not in caplog.text


def test_probe_leaves_no_transaction_open(db):
    """The probe runs in autocommit and hands the connection back in normal mode."""
    conn, pool = db

    with conn.get_connection() as c:
        assert c.executed == [("SELECT 1", True)]
        assert c.autocommit is False


def test_every_idle_connection_dead_still_yields_a_live_one(db, monkeypatch):
    """A network drop can take all idle connections at once; at most DB_POOL_MIN sit idle."""
    conn, pool = db
    monkeypatch.setattr(settings, "DB_POOL_MIN", 3)
    pool.idle = [FakeConn(dead=True) for _ in range(3)]

    with conn.get_connection() as c:
        assert not c.dead

    assert [closed for _, closed in pool.returned] == [True, True, True, False]


def test_unreachable_database_raises_and_frees_the_slot(db, monkeypatch, caplog):
    """If even a fresh connection fails the probe, the request fails as before."""
    conn, pool = db
    monkeypatch.setattr(settings, "DB_POOL_MIN", 1)
    pool.getconn = lambda: FakeConn(dead=True)

    for _ in range(settings.DB_POOL_MAX + 1):
        with pytest.raises(psycopg2.OperationalError):
            with conn.get_connection():
                pytest.fail("handler must not run without a live connection")

    assert all(closed for _, closed in pool.returned)
    assert "Database connection failed" in caplog.text


def test_pool_connects_with_tcp_keepalives(monkeypatch):
    captured = {}

    class CapturingPool:
        def __init__(self, minconn, maxconn, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(psycopg2.pool, "ThreadedConnectionPool", CapturingPool)
    DatabaseConnection()._get_pool()

    assert {k: captured[k] for k in KEEPALIVES} == KEEPALIVES
