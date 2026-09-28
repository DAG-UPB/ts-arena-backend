-- =====================================================================================
-- Forecast scores: MASE scaled on the forecast context (Hyndman & Koehler 2006)
-- =====================================================================================
-- Idempotent migration for LIVE databases (there is no migration tool yet; init_db.sql is
-- for fresh installs only). Apply to DEV first; prod is human-gated. Safe to re-run.
--
-- Adds two tables and changes nothing that exists:
--   forecasts.series_scale     one context scale per (round, series)
--   forecasts.forecast_scores  MAE, RMSE, MASE and SQL per (round, model, series)
--
-- ForecastScoringService (api-portal) writes them, and does nothing until they exist.
-- No view, ranking or Elo reads them yet. forecasts.scores is untouched.
-- History is filled by `python -m app.scripts.backfill_forecast_scores`.
--
-- The table definitions below match init_db.sql exactly.
-- =====================================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS forecasts.series_scale (
    round_id INTEGER NOT NULL REFERENCES challenges.rounds(id) ON DELETE CASCADE,
    series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    m SMALLINT NOT NULL,                 -- seasonal lag actually used, in steps of the round frequency
    scale DOUBLE PRECISION,              -- mean |y_t - y_{t-m}| over the context; NULL = no lag pair
    last_value DOUBLE PRECISION,         -- last context value: the persistence forecast behind naive_mae
    n_points INTEGER NOT NULL,           -- context points with a value
    n_pairs INTEGER NOT NULL,            -- lag pairs averaged (pairs across a gap are skipped)
    context_start TIMESTAMPTZ,           -- window the context was read from (series_pseudo.min_ts)
    context_end TIMESTAMPTZ,             -- ... through series_pseudo.max_ts
    source TEXT NOT NULL CHECK (source IN ('context_data', 'scd2_as_of_round_creation')),
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (round_id, series_id)
);

COMMENT ON COLUMN forecasts.series_scale.source IS
'context_data: computed from the context as served, while the round was in registration. scd2_as_of_round_creation: rebuilt from data_portal.time_series_data_scd2 as of rounds.created_at (per point the newest version with a value), where the served context was not captured.';

CREATE TABLE IF NOT EXISTS forecasts.forecast_scores (
    round_id INTEGER NOT NULL REFERENCES challenges.rounds(id) ON DELETE CASCADE,
    model_id INTEGER NOT NULL REFERENCES models.model_info(id) ON DELETE CASCADE,
    series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    mae DOUBLE PRECISION CHECK (mae IS NULL OR (mae >= 0 AND mae < 'Infinity'::float8)),
    rmse DOUBLE PRECISION CHECK (rmse IS NULL OR (rmse >= 0 AND rmse < 'Infinity'::float8)),
    naive_mae DOUBLE PRECISION CHECK (naive_mae IS NULL OR (naive_mae >= 0 AND naive_mae < 'Infinity'::float8)),
    n_points INTEGER,                    -- evaluated points (the MAE's sample size)
    scale DOUBLE PRECISION,              -- copy of forecasts.series_scale.scale used here
    mase DOUBLE PRECISION CHECK (mase IS NULL OR (mase >= 0 AND mase < 'Infinity'::float8)),
    sql_score DOUBLE PRECISION CHECK (sql_score IS NULL OR (sql_score >= 0 AND sql_score < 'Infinity'::float8)),
    sql_per_quantile JSONB,
    has_quantiles BOOLEAN,
    quantile_levels_count INTEGER,
    quantile_crossing_count INTEGER,
    forecast_count INTEGER,              -- in-window forecast points: the coverage denominator
    data_coverage DOUBLE PRECISION,
    final_evaluation BOOLEAN NOT NULL DEFAULT FALSE,
    evaluation_status TEXT NOT NULL,
    error_message TEXT,
    method TEXT NOT NULL,                -- how mase was computed, e.g. 'hk2006_context_m1'
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (round_id, model_id, series_id)
);

COMMIT;
