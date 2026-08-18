# app/core/logging_setup.py

"""Process-wide logging configuration.

Why this exists (backend #76): the service used to configure exactly one logger, named
after the service, and left the root logger untouched. uvicorn's own LOGGING_CONFIG has
no "root" key either, so the root logger kept its default level of WARNING with no
handler at all. Consequences:

  * every logger that was not literally the service logger -- `challenge-scheduler`,
    every `getLogger(__name__)`, `apscheduler` -- had its INFO records dropped before
    they were even constructed, and
  * its WARNING and above escaped through `logging.lastResort`: bare message text on
    stderr, no timestamp, no level, no logger name.

That is why the ELO ranking job could be dead for ten days without the container log
saying anything either way. On top of that, uvicorn's access log has no timestamp, so a
log window could not even be dated, and vulnerability scanners filled it with 404s for
WordPress paths that no operator will ever care about.

So: configure the ROOT logger, adopt uvicorn's three loggers into it, and drop the
scanner 404s while keeping a periodic count of what was dropped.

This file is duplicated verbatim in api-portal, dashboard-api and data-portal. Each
service's Dockerfile copies only its own subdirectory and the build context is set per
app in Coolify, so a shared package would require changing all three build contexts --
a Coolify write we do not have. Keep the three copies in sync by hand until then;
`api-portal/tests/test_logging_setup.py` turns drift between them into a failing test
(ts-arena #15 subtask E), including the case where a fourth service adds a copy.

A further copy lives in the separate ts-arena-console repo, at
`app/core/logging_setup.py`. That one is deliberately NOT byte-identical -- its
`LOG_FORMAT` carries `%(process)d`, because console-api runs `uvicorn --workers 2` and
its two workers are otherwise indistinguishable in one container log. No test in this
repo can see it, so changes worth having must be carried across by hand.
"""

import logging
import os
import sys
import threading
import time
from collections import Counter

# Matches what api-portal already emitted for its own lines, so existing greps still
# work. `Z` because the converter below is gmtime -- an undated log window is bad, an
# ambiguously dated one is worse.
LOG_FORMAT = "%(asctime)sZ | %(levelname)s | %(name)s | %(message)s"

# Libraries that log at INFO by default and tell an operator nothing they asked for.
# Override any of these with LOG_LEVELS (see _apply_logger_levels).
NOISY_LIBRARIES = (
    "apscheduler",
    "asyncpg",
    "httpcore",
    "httpx",
    "sqlalchemy.engine",
    "urllib3",
)

# One summary line at most this often, per process.
SUPPRESSION_SUMMARY_INTERVAL_SECONDS = 300.0

# How many distinct paths to name in that summary line.
SUPPRESSION_SUMMARY_TOP_N = 5

_LEVEL_NAMES = {
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def resolve_level(value, default=logging.INFO):
    """Turn a LOG_LEVEL string into a level int, without ever raising.

    The previous implementation was `getattr(logging, Config.LOG_LEVEL, logging.INFO)`,
    which looks safe and is not: `getattr(logging, "info")` resolves to the module
    *function* `logging.info`, so the default never fires and `setLevel` raises
    `TypeError: Level not an integer or a valid string`. Since that call was the first
    statement of the lifespan, `LOG_LEVEL=info` -- the obvious lowercase spelling of a
    documented operator knob -- took the whole service down at startup.
    """
    if isinstance(value, int):
        return value
    if not value:
        return default
    return _LEVEL_NAMES.get(str(value).strip().upper(), default)


class UTCFormatter(logging.Formatter):
    """Formatter whose `%(asctime)s` is UTC rather than local container time."""

    converter = time.gmtime


class OffSurface404Filter(logging.Filter):
    """Drop uvicorn access-log lines for 404s on paths the app never served.

    A 404 is *off-surface* when the first segment of its path is not the first segment
    of any route this app has registered. `/wp-content/plugins/x.php` and `/admin.php`
    are dropped; `/api/v1/challenge/rounds/99999` -- a real client using a wrong round
    id, which is the diagnostically useful case -- is kept, because `api` is a segment
    this app serves.

    Deriving the surface from `app.routes` rather than a hardcoded prefix list means the
    filter cannot go stale when a router is added.

    Suppression is counted, never silent: every so often the next suppressed record
    triggers one summary line naming the volume and the top paths, so the fact that the
    service is being scanned survives as a single line instead of hundreds. Note the
    corollary -- the final, incomplete window is only reported once another suppressed
    record arrives, so a count below the interval can sit unreported at shutdown.
    """

    def __init__(self, allowed_segments, summary_logger=None,
                 interval_seconds=SUPPRESSION_SUMMARY_INTERVAL_SECONDS):
        super().__init__()
        self.allowed_segments = set(allowed_segments)
        # A logger that is NOT the one this filter is attached to, or the summary would
        # be filtered by its own filter.
        self._summary_logger = summary_logger or logging.getLogger("access-filter")
        self._interval = interval_seconds
        self._lock = threading.Lock()
        self._suppressed = 0
        self._paths = Counter()
        self._clients = set()
        self._window_started = time.monotonic()

    @staticmethod
    def _parse(record):
        """Pull (path, status) out of a uvicorn access record, or None if it isn't one.

        uvicorn logs access lines as
        `'%s - "%s %s HTTP/%s" %d' % (client_addr, method, path_with_query,
        http_version, status)` -- verified identical in 0.24.0 (dashboard-api's pin) and
        0.52.3 (what `uvicorn[standard]` resolves to today). Anything else that reaches
        this logger is left alone.
        """
        args = record.args
        if not isinstance(args, tuple) or len(args) != 5:
            return None
        client_addr, _method, path_with_query, _http_version, status = args
        if not isinstance(status, int):
            return None
        path = str(path_with_query).split("?", 1)[0]
        return client_addr, path, status

    def filter(self, record):
        parsed = self._parse(record)
        if parsed is None:
            return True

        client_addr, path, status = parsed
        if status != 404:
            return True
        if first_segment(path) in self.allowed_segments:
            return True

        self._note_suppressed(client_addr, path)
        return False

    def _note_suppressed(self, client_addr, path):
        with self._lock:
            if self._suppressed == 0:
                # Time the window from the first suppression, not from process start,
                # or the first summary reports however long the service happened to be
                # idle before the scanners showed up.
                self._window_started = time.monotonic()

            self._suppressed += 1
            self._paths[path] += 1
            self._clients.add(client_addr)

            elapsed = time.monotonic() - self._window_started
            if elapsed < self._interval:
                return

            summary = self._format_summary(elapsed)
            self._reset_window()

        # Emit outside the lock: the handler does I/O.
        self._summary_logger.info(summary)

    def _format_summary(self, elapsed):
        top = ", ".join(
            f"{path} ({count})"
            for path, count in self._paths.most_common(SUPPRESSION_SUMMARY_TOP_N)
        )
        return (
            f"suppressed {self._suppressed} off-surface 404 access lines in "
            f"{elapsed:.1f}s from {len(self._clients)} client(s); "
            f"{len(self._paths)} distinct paths; top: {top}"
        )

    def _reset_window(self):
        self._suppressed = 0
        self._paths.clear()
        self._clients.clear()
        self._window_started = time.monotonic()

    # Exposed for tests and for anyone who wants the counters without waiting for the
    # summary interval to elapse.
    @property
    def suppressed_count(self):
        with self._lock:
            return self._suppressed


def first_segment(path):
    """`/api/v1/challenge/rounds` -> `api`; `/` -> `''`."""
    stripped = str(path).lstrip("/")
    if not stripped:
        return ""
    return stripped.split("/", 1)[0]


def _iter_route_paths(routes, prefix="", depth=0):
    """Yield the full path of every route reachable from `routes`.

    Two shapes have to be handled. Older FastAPI flattens `include_router()` straight
    into `app.routes`, so each route already carries its full path. Newer FastAPI (the
    0.141 that api-portal resolves to) instead appends an `_IncludedRouter` wrapper whose
    `path` is None and whose real routes and mount prefix hang off an `include_context`.
    Reading only the top level there would miss the entire `/api/v1` surface and get
    every genuine 404 suppressed -- exactly the opposite of what this filter is for.
    """
    if depth > 10:  # routers do not nest this deep; this is a cycle guard, not a limit
        return

    for route in routes or ():
        path = getattr(route, "path", None)
        if isinstance(path, str):
            yield prefix + path
            continue

        context = getattr(route, "include_context", None)
        included = getattr(context, "included_router", None)
        if included is not None:
            yield from _iter_route_paths(
                getattr(included, "routes", ()),
                prefix + (getattr(context, "prefix", "") or ""),
                depth + 1,
            )
            continue

        nested = getattr(route, "routes", None)
        if nested:
            yield from _iter_route_paths(nested, prefix, depth + 1)


def route_segments(app):
    """First path segment of every route the app has registered.

    Includes FastAPI's own docs routes, which are live unless explicitly disabled.
    """
    return {first_segment(path) for path in _iter_route_paths(getattr(app, "routes", ()))}


def _apply_logger_levels(default_level):
    """Quiet the noisy libraries, then apply any per-logger overrides.

    LOG_LEVELS is a comma-separated list of `logger=LEVEL` pairs, e.g.
    `LOG_LEVELS=httpx=INFO,apscheduler=DEBUG`. It is how you get library tracing back on
    demand without turning the whole service to DEBUG.
    """
    for name in NOISY_LIBRARIES:
        # Never make a library louder than the service itself.
        logging.getLogger(name).setLevel(max(logging.WARNING, default_level))

    for pair in os.getenv("LOG_LEVELS", "").split(","):
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        name, _, level = pair.partition("=")
        logging.getLogger(name.strip()).setLevel(resolve_level(level, default_level))


def configure_logging(service_name=None, level=None, stream=None):
    """Configure the root logger for the whole process. Call once, as early as possible.

    Everything -- application modules, uvicorn, libraries -- goes through one handler on
    **stdout**, so application and access lines land on one ordered stream. They used to
    be split across stderr and stdout respectively and Docker interleaved them
    arbitrarily, which made the relative order of an access line and the traceback it
    caused unknowable.

    Returns the resolved level so callers can log it.
    """
    resolved = resolve_level(level if level is not None else os.getenv("LOG_LEVEL"), logging.INFO)

    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(UTCFormatter(LOG_FORMAT))

    root = logging.getLogger()
    for existing in root.handlers[:]:
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(resolved)

    # Adopt uvicorn's loggers into the root handler so its lines -- crucially the access
    # lines -- get the same timestamp and level as everything else. uvicorn decides
    # whether to emit access logs at all via `access_logger.hasHandlers()`, which walks
    # up to root, so clearing these handlers does not disable access logging.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True
        uvicorn_logger.setLevel(logging.NOTSET)

    _apply_logger_levels(resolved)

    # `warnings.warn` writes straight to stderr, several untimestamped lines per
    # warning. On dashboard-api that was 60 of the first 82 lines in a fresh
    # container -- pydantic protected-namespace warnings, one block per model field.
    # Routing them through the `py.warnings` logger puts them on the same stream as
    # everything else, one timestamped line each.
    # Disarm first: captureWarnings(True) is a no-op once armed (it only swaps
    # showwarning while its saved original is None), so anything that restores
    # showwarning afterwards -- pytest's warnings plugin does exactly this -- would
    # leave capture permanently off. Re-arming keeps configure_logging idempotent.
    logging.captureWarnings(False)
    logging.captureWarnings(True)

    if service_name:
        # Keep the old service-named logger working for modules that fetch it by name;
        # it now just inherits root instead of owning a handler.
        logging.getLogger(service_name).setLevel(logging.NOTSET)

    return resolved


def _summary_interval():
    raw = os.getenv("ACCESS_LOG_SUMMARY_INTERVAL_SECONDS")
    if not raw:
        return SUPPRESSION_SUMMARY_INTERVAL_SECONDS
    try:
        return max(0.0, float(raw))
    except ValueError:
        return SUPPRESSION_SUMMARY_INTERVAL_SECONDS


def install_access_log_filter(app, interval_seconds=None):
    """Attach the off-surface 404 filter to `uvicorn.access`.

    Must be called *after* every router is included, since the allowed surface is read
    off `app.routes`. Set `ACCESS_LOG_FILTER=off` to keep the raw flood -- useful when
    you are actually investigating the scan traffic rather than working around it --
    and `ACCESS_LOG_SUMMARY_INTERVAL_SECONDS` to change how often the count is reported.

    Returns the installed filter, or None if disabled.
    """
    if os.getenv("ACCESS_LOG_FILTER", "on").strip().lower() in ("off", "0", "false", "no"):
        logging.getLogger("access-filter").info(
            "off-surface 404 access-log filter disabled by ACCESS_LOG_FILTER"
        )
        return None

    access_logger = logging.getLogger("uvicorn.access")
    for existing in access_logger.filters[:]:
        if isinstance(existing, OffSurface404Filter):
            access_logger.removeFilter(existing)

    log_filter = OffSurface404Filter(
        route_segments(app),
        interval_seconds=_summary_interval() if interval_seconds is None else interval_seconds,
    )
    access_logger.addFilter(log_filter)
    return log_filter
