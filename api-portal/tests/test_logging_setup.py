"""Tests for the process-wide logging configuration (backend #76).

Covers the three things the issue turns on: that LOG_LEVEL can no longer abort startup,
that non-service loggers actually reach a handler now, and that the access-log filter
drops scanner 404s while keeping the ones an operator needs.
"""

import io
import logging
import re
import time

import pytest

from app.core.logging_setup import (
    OffSurface404Filter,
    configure_logging,
    install_access_log_filter,
    resolve_level,
    route_segments,
)


@pytest.fixture(autouse=True)
def restore_logging():
    """Put the root logger back the way we found it -- these tests reconfigure it."""
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    access = logging.getLogger("uvicorn.access")
    saved_filters = access.filters[:]
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    access.filters[:] = saved_filters


def access_record(path, status, client="203.0.113.7:51234", method="GET"):
    """A record shaped exactly like uvicorn's access log emits."""
    return logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='%s - "%s %s HTTP/%s" %d',
        args=(client, method, path, "1.1", status),
        exc_info=None,
    )


class TestResolveLevel:
    """The bug that made LOG_LEVEL a startup landmine."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("INFO", logging.INFO),
            ("info", logging.INFO),          # used to raise TypeError
            ("Debug", logging.DEBUG),
            ("  warning  ", logging.WARNING),
            ("WARN", logging.WARNING),
            ("CRITICAL", logging.CRITICAL),
            (logging.ERROR, logging.ERROR),
        ],
    )
    def test_accepts_any_casing_and_ints(self, value, expected):
        assert resolve_level(value) == expected

    @pytest.mark.parametrize("value", ["nonsense", "debu9", "", None, "  "])
    def test_falls_back_instead_of_raising(self, value):
        assert resolve_level(value, logging.INFO) == logging.INFO

    def test_result_is_always_usable_by_setlevel(self):
        # The old code returned a function here, which setLevel rejected.
        for value in ("info", "INFO", "nope", None):
            logging.getLogger("probe").setLevel(resolve_level(value))


class TestConfigureLogging:
    def test_non_service_loggers_now_reach_a_handler(self):
        stream = io.StringIO()
        configure_logging("api-portal", level="INFO", stream=stream)

        # This is the logger the ELO job uses. It produced nothing before.
        logging.getLogger("challenge-scheduler").info("Starting periodic ELO ranking calculation job")

        assert "Starting periodic ELO ranking calculation job" in stream.getvalue()
        assert "challenge-scheduler" in stream.getvalue()

    def test_every_line_is_timestamped_and_levelled(self):
        stream = io.StringIO()
        configure_logging("api-portal", level="INFO", stream=stream)
        logging.getLogger("anything").warning("hello")

        line = stream.getvalue().strip()
        stamp, level, name, message = (part.strip() for part in line.split("|", 3))
        assert level == "WARNING"
        assert name == "anything"
        assert message == "hello"
        # "2026-08-17 09:49:36,498Z"
        assert stamp.endswith("Z")
        assert len(stamp) == len("0000-00-00 00:00:00,000Z")

    def test_uvicorn_loggers_are_adopted_by_root(self):
        stream = io.StringIO()
        configure_logging("api-portal", level="INFO", stream=stream)

        for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
            uvicorn_logger = logging.getLogger(name)
            assert uvicorn_logger.handlers == []
            assert uvicorn_logger.propagate is True
            # uvicorn only emits access logs when this is true.
            assert uvicorn_logger.hasHandlers()

    def test_noisy_libraries_are_quieted(self):
        configure_logging("api-portal", level="INFO", stream=io.StringIO())
        assert logging.getLogger("httpx").level == logging.WARNING
        assert logging.getLogger("apscheduler").level == logging.WARNING

    def test_log_levels_env_overrides_a_single_library(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVELS", "httpx=DEBUG, apscheduler=error")
        configure_logging("api-portal", level="INFO", stream=io.StringIO())
        assert logging.getLogger("httpx").level == logging.DEBUG
        assert logging.getLogger("apscheduler").level == logging.ERROR

    def test_repeated_calls_do_not_stack_handlers(self):
        for _ in range(3):
            configure_logging("api-portal", level="INFO", stream=io.StringIO())
        assert len(logging.getLogger().handlers) == 1


class TestOffSurface404Filter:
    SEGMENTS = {"", "api", "health", "admin", "docs", "redoc", "openapi.json"}

    @pytest.fixture
    def log_filter(self):
        return OffSurface404Filter(self.SEGMENTS)

    @pytest.mark.parametrize(
        "path",
        [
            "/wp-content/plugins/hellopress/wp_filemanager.php",
            "/admin.php",
            "/cgi-bin/index.php",
            "/1.php",
            "/.env",
            "/phpmyadmin/index.php",
        ],
    )
    def test_drops_scanner_404s(self, log_filter, path):
        assert log_filter.filter(access_record(path, 404)) is False

    @pytest.mark.parametrize(
        "path",
        [
            "/api/v1/challenge/rounds/99999",      # real client, wrong round id
            "/api/v1/forecasts/1/2",
            "/health",
            "/docs",
            "/",
        ],
    )
    def test_keeps_on_surface_404s(self, log_filter, path):
        assert log_filter.filter(access_record(path, 404)) is True

    @pytest.mark.parametrize("status", [200, 201, 301, 401, 403, 422, 500])
    def test_only_ever_touches_404s(self, log_filter, status):
        assert log_filter.filter(access_record("/wp-content/x.php", status)) is True

    def test_query_string_does_not_hide_the_path(self, log_filter):
        assert log_filter.filter(access_record("/classwithtostring.php?p=1", 404)) is False
        assert log_filter.filter(access_record("/api/v1/challenge/rounds?status=x", 404)) is True

    def test_non_access_records_pass_through(self, log_filter):
        record = logging.LogRecord(
            name="uvicorn.access", level=logging.INFO, pathname=__file__, lineno=1,
            msg="something else entirely", args=(), exc_info=None,
        )
        assert log_filter.filter(record) is True

    def test_counts_what_it_dropped(self, log_filter):
        for _ in range(3):
            log_filter.filter(access_record("/wp-login.php", 404))
        log_filter.filter(access_record("/api/v1/challenge/rounds/1", 404))
        assert log_filter.suppressed_count == 3

    def test_emits_a_summary_once_the_interval_elapses(self, caplog):
        log_filter = OffSurface404Filter(self.SEGMENTS, interval_seconds=0.0)
        with caplog.at_level(logging.INFO, logger="access-filter"):
            log_filter.filter(access_record("/wp-login.php", 404, client="198.51.100.4:1"))
            log_filter.filter(access_record("/admin.php", 404, client="198.51.100.9:2"))

        summaries = [r.message for r in caplog.records if r.name == "access-filter"]
        assert len(summaries) == 2
        assert "off-surface 404 access lines" in summaries[0]
        assert "1 client(s)" in summaries[0]

    def test_counters_reset_after_a_summary(self):
        log_filter = OffSurface404Filter(self.SEGMENTS, interval_seconds=0.0)
        log_filter.filter(access_record("/wp-login.php", 404))
        assert log_filter.suppressed_count == 0  # flushed into the summary


class TestRouteSegmentsAndInstall:
    def test_segments_are_read_off_the_real_app(self):
        from app.main import app

        segments = route_segments(app)
        assert {"api", "health", "admin", ""} <= segments
        assert "wp-content" not in segments

    def test_filter_installed_on_the_real_app_keeps_its_own_surface(self, monkeypatch):
        from app.main import app

        monkeypatch.delenv("ACCESS_LOG_FILTER", raising=False)
        log_filter = install_access_log_filter(app)
        assert log_filter is not None
        try:
            assert log_filter.filter(access_record("/api/v1/challenge/rounds/1", 404)) is True
            assert log_filter.filter(access_record("/wp-content/x.php", 404)) is False
        finally:
            logging.getLogger("uvicorn.access").removeFilter(log_filter)

    def test_install_is_idempotent(self, monkeypatch):
        from app.main import app

        monkeypatch.delenv("ACCESS_LOG_FILTER", raising=False)
        access = logging.getLogger("uvicorn.access")
        before = len(access.filters)
        for _ in range(3):
            install_access_log_filter(app)
        assert len(access.filters) == before + 1

    def test_env_kill_switch(self, monkeypatch):
        from app.main import app

        monkeypatch.setenv("ACCESS_LOG_FILTER", "off")
        assert install_access_log_filter(app) is None


class TestSummaryInterval:
    def test_env_sets_the_interval(self, monkeypatch):
        from app.core.logging_setup import _summary_interval

        monkeypatch.setenv("ACCESS_LOG_SUMMARY_INTERVAL_SECONDS", "30")
        assert _summary_interval() == 30.0

    @pytest.mark.parametrize("raw", ["", "not-a-number", "  "])
    def test_bad_values_fall_back_to_the_default(self, monkeypatch, raw):
        from app.core.logging_setup import (
            SUPPRESSION_SUMMARY_INTERVAL_SECONDS,
            _summary_interval,
        )

        monkeypatch.setenv("ACCESS_LOG_SUMMARY_INTERVAL_SECONDS", raw)
        assert _summary_interval() == SUPPRESSION_SUMMARY_INTERVAL_SECONDS


class TestSummaryWindow:
    def test_window_is_timed_from_the_first_suppression(self, caplog):
        """Not from process start, or the first summary reports the idle time before it."""
        log_filter = OffSurface404Filter({"api"}, interval_seconds=0.3)
        time.sleep(1.0)  # service sits idle before any scanner shows up

        with caplog.at_level(logging.INFO, logger="access-filter"):
            log_filter.filter(access_record("/wp-login.php", 404))   # opens the window
            assert not caplog.records                                # idle time doesn't count
            time.sleep(0.4)
            log_filter.filter(access_record("/admin.php", 404))      # closes it

        assert len(caplog.records) == 1
        message = caplog.records[0].message
        assert message.startswith("suppressed 2 off-surface 404 access lines in ")

        reported = float(re.search(r" in ([\d.]+)s ", message).group(1))
        assert 0.3 <= reported < 0.9, f"window should be ~0.4s, not the 1.4s since start: {message}"
