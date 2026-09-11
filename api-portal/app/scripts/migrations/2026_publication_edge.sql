-- =====================================================================================
-- backend-87: let the continuous aggregates reach the publication edge
-- =====================================================================================
-- Idempotent migration for LIVE databases (there is no migration tool yet; init_db.sql is
-- for fresh installs only). Apply to DEV first; prod is human-gated. Safe to re-run.
--
-- WHAT WAS WRONG
--
-- A continuous aggregate with `materialized_only = true` returns only materialised buckets,
-- and a positive `end_offset` keeps the materialization watermark at ~now. So the view's
-- `max(ts)` could never exceed ~now, no matter how far ahead the raw data actually extended.
--
-- `ChallengeService._prepare_context_data` derives the round's forecast window from exactly
-- that value:
--
--     new_start_time = global_max_ts + frequency_timedelta
--     new_end_time   = new_start_time + horizon
--
-- so the window collapsed to [now, now + horizon]. For SMARD day-ahead prices — published
-- from ~12:45 CET on D-1 for delivery day D — that window sat entirely inside data that was
-- already public. Definitions 1 and 4 were a lookup, not a forecast, from round 10152
-- (2026-04-28) onward. Measured lead for those series: max 1 d 08:38, p95 1 d 05:45, with
-- 79.3 % of points ingested more than 2 h ahead of their own timestamp.
--
-- Neither setting was wrong alone. `end_offset` is the textbook value (one bucket width) and
-- `materialized_only` was never set at all — it took TimescaleDB's default, which flipped
-- from false to true in 2.13.0 (2023-11-28). On an older release the same DDL would have
-- unioned in the unmaterialised rows and this would never have happened.
--
-- WHAT THIS CHANGES
--
-- Real-time aggregation: the view becomes (materialised buckets below the watermark) UNION
-- ALL (live time_bucket over raw above it), so rows the publisher has already released are
-- visible immediately and `max(ts)` becomes the true publication edge. This is per series and
-- automatic — day-ahead prices get context through D+1 23:45 and a window starting D+2 00:00
-- (matching the seeded rounds), while load and generation series have nothing in raw beyond
-- now and are unchanged. No per-challenge carve-out, which would have to be repeated for every
-- future publish-ahead source and fails silently when missed.
--
-- `end_offset` is deliberately left alone: it still prevents a *filling* bucket being
-- materialised. What must not depend on it is the visibility of future rows. The read side
-- drops the filling bucket itself — see `in_progress_bucket_start` in
-- api-portal/app/database/data_portal/time_series_repository.py.
--
-- This also removes the materialisation lag from reads, which on prod has been running ~14 h
-- rather than 15 minutes (see backend-85).
--
-- COST (measured on dev 2026-09-11; context read of 15 series x 1000 points, server-side
-- EXPLAIN ANALYZE, warm, summed):
--
--            materialized_only=true   lag 15 min   lag 14 h (prod today)
--   15-min           31.5 ms            93.0 ms          122.5 ms
--   1-hour           23.7 ms            72.5 ms           84.8 ms
--
-- Roughly 3-4x on an operation that runs once per round and costs tens of milliseconds.
-- Context prep for the affected definitions is currently measured in *minutes* (backend-85),
-- so this is not a term that matters.
--
-- These are metadata-only ALTERs: no rewrite, no lock on the raw hypertable, instant.
-- Reversible with `= true`.
-- =====================================================================================

BEGIN;

ALTER MATERIALIZED VIEW data_portal.time_series_15min
    SET (timescaledb.materialized_only = false);

ALTER MATERIALIZED VIEW data_portal.time_series_1h
    SET (timescaledb.materialized_only = false);

-- Not used by any active challenge definition today, but set for the same reason: the defect
-- is "the aggregate cannot hold future rows", and leaving one view behind re-arms the trap for
-- whoever next reads it.
ALTER MATERIALIZED VIEW data_portal.time_series_1d
    SET (timescaledb.materialized_only = false);

COMMIT;

-- Verify: all three should report `materialized_only = f`.
--
--   SELECT view_name, materialized_only
--     FROM timescaledb_information.continuous_aggregates
--    ORDER BY view_name;
--
-- And the aggregate's max should now track raw rather than the watermark. On a day-ahead
-- price series this should be ahead of now() during the afternoon:
--
--   SELECT max(ts) AS agg_max,
--          (SELECT max(ts) FROM data_portal.time_series_data WHERE series_id = 68) AS raw_max,
--          now()
--     FROM data_portal.time_series_15min
--    WHERE series_id = 68;
