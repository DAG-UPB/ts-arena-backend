-- Forecast scores: MASE scaled on the forecast context. Idempotent; run as the schema owner.
-- Adds forecasts.series_scale and forecasts.forecast_scores, changes nothing that exists.
-- Table definitions match init_db.sql.

BEGIN;

CREATE TABLE IF NOT EXISTS forecasts.series_scale (
    round_id INTEGER NOT NULL REFERENCES challenges.rounds(id) ON DELETE CASCADE,
    series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    m SMALLINT NOT NULL,
    scale DOUBLE PRECISION,
    last_value DOUBLE PRECISION,
    n_points INTEGER NOT NULL,
    n_pairs INTEGER NOT NULL,
    context_start TIMESTAMPTZ,
    context_end TIMESTAMPTZ,
    source TEXT NOT NULL CHECK (source IN ('context_data', 'scd2_as_of_round_creation')),
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (round_id, series_id)
);

CREATE TABLE IF NOT EXISTS forecasts.forecast_scores (
    round_id INTEGER NOT NULL REFERENCES challenges.rounds(id) ON DELETE CASCADE,
    model_id INTEGER NOT NULL REFERENCES models.model_info(id) ON DELETE CASCADE,
    series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    mae DOUBLE PRECISION CHECK (mae IS NULL OR (mae >= 0 AND mae < 'Infinity'::float8)),
    rmse DOUBLE PRECISION CHECK (rmse IS NULL OR (rmse >= 0 AND rmse < 'Infinity'::float8)),
    naive_mae DOUBLE PRECISION CHECK (naive_mae IS NULL OR (naive_mae >= 0 AND naive_mae < 'Infinity'::float8)),
    n_points INTEGER,
    scale DOUBLE PRECISION,
    mase DOUBLE PRECISION CHECK (mase IS NULL OR (mase >= 0 AND mase < 'Infinity'::float8)),
    sql_score DOUBLE PRECISION CHECK (sql_score IS NULL OR (sql_score >= 0 AND sql_score < 'Infinity'::float8)),
    sql_per_quantile JSONB,
    has_quantiles BOOLEAN,
    quantile_levels_count INTEGER,
    quantile_crossing_count INTEGER,
    forecast_count INTEGER,
    data_coverage DOUBLE PRECISION,
    final_evaluation BOOLEAN NOT NULL DEFAULT FALSE,
    evaluation_status TEXT NOT NULL,
    error_message TEXT,
    method TEXT NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (round_id, model_id, series_id)
);

COMMIT;
