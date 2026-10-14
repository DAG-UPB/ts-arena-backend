-- Arena Score: one row per date, scope, metric and model. Idempotent; run as the schema owner.
-- Adds forecasts.daily_arena_scores, changes nothing that exists.
-- Table definition matches init_db.sql.

BEGIN;

CREATE TABLE IF NOT EXISTS forecasts.daily_arena_scores (
    calculation_date DATE NOT NULL,
    scope_type TEXT NOT NULL CHECK (scope_type IN ('global', 'definition', 'frequency_horizon')),
    scope_id TEXT,                          -- NULL for global
    metric TEXT NOT NULL CHECK (metric IN ('point', 'probabilistic')),
    model_id INTEGER NOT NULL REFERENCES models.model_info(id) ON DELETE CASCADE,
    score DOUBLE PRECISION,                 -- NULL for a model outside the fitted group
    ci_lower DOUBLE PRECISION,
    ci_upper DOUBLE PRECISION,
    rank_best INTEGER,                      -- NULL unless ranked
    rank_worst INTEGER,
    provisional BOOLEAN NOT NULL,
    low_coverage BOOLEAN NOT NULL,
    unranked_disconnected BOOLEAN NOT NULL,
    coverage DOUBLE PRECISION,
    tasks_played INTEGER NOT NULL,
    rounds_played INTEGER NOT NULL,
    weeks_played INTEGER NOT NULL,
    mase_30d DOUBLE PRECISION,              -- NULL below 80 % participation
    participation_30d DOUBLE PRECISION,
    sql_30d DOUBLE PRECISION,
    sql_participation_30d DOUBLE PRECISION,
    tasks_30d INTEGER,
    method JSONB NOT NULL,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_arena_scores_unique
ON forecasts.daily_arena_scores (calculation_date, scope_type, COALESCE(scope_id, ''), metric, model_id);

COMMIT;
