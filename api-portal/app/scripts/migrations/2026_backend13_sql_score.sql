-- =====================================================================================
-- backend #13 — Probabilistic evaluation (Scaled Quantile Loss) + SQL-metric rankings
-- =====================================================================================
-- Idempotent migration for LIVE databases (there is no migration tool yet; init_db.sql is
-- for fresh installs only — see ts-arena-backend/CLAUDE.md). Apply to DEV first; prod is
-- human-gated. Safe to re-run.
--
-- What it does:
--   1. Adds probabilistic columns to forecasts.scores.
--   2. Adds `metric` + cumulative-SQL columns to forecasts.daily_rankings and widens the
--      unique index to include `metric` (so mase- and sql-ranked snapshots coexist).
--   3. Rebuilds forecasts.round_model_scores to carry sum_sql/sum_sql_sq/num_sql.
--   4. Recreates the dependent views (v_ranking_base, v_daily_rankings_leaderboard,
--      v_monthly_and_latest_rankings) with the new columns.
--
-- The view/matview bodies below are kept byte-for-byte in sync with init_db.sql.
-- =====================================================================================

BEGIN;

-- 1) forecasts.scores: probabilistic columns ------------------------------------------
ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS sql_score DOUBLE PRECISION;
ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS sql_per_quantile JSONB;
ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS has_quantiles BOOLEAN;
ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS quantile_levels_count INTEGER;
ALTER TABLE forecasts.scores ADD COLUMN IF NOT EXISTS quantile_crossing_count INTEGER;

-- 2) forecasts.daily_rankings: metric + cumulative SQL snapshot -----------------------
ALTER TABLE forecasts.daily_rankings ADD COLUMN IF NOT EXISTS metric TEXT NOT NULL DEFAULT 'mase';
DO $$ BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM pg_constraint WHERE conname = 'daily_rankings_metric_check'
  ) THEN
      ALTER TABLE forecasts.daily_rankings
          ADD CONSTRAINT daily_rankings_metric_check CHECK (metric IN ('mase', 'sql'));
  END IF;
END $$;
ALTER TABLE forecasts.daily_rankings ADD COLUMN IF NOT EXISTS avg_sql DOUBLE PRECISION;
ALTER TABLE forecasts.daily_rankings ADD COLUMN IF NOT EXISTS sql_std DOUBLE PRECISION;

-- Widen the unique index to include metric.
DROP INDEX IF EXISTS forecasts.idx_daily_rankings_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_rankings_unique
ON forecasts.daily_rankings(
    calculation_date,
    model_id,
    scope_type,
    COALESCE(scope_id, ''),
    metric
);

-- 3) Rebuild round_model_scores (matview cannot ADD COLUMN) ---------------------------
-- The hourly refresh job (forecasts.refresh_round_scores) refers to the matview by name,
-- so it survives the recreate; do NOT re-add the job.
DROP MATERIALIZED VIEW IF EXISTS forecasts.round_model_scores;
CREATE MATERIALIZED VIEW forecasts.round_model_scores AS
-- Global Scope
SELECT
    r.id as round_id,
    r.registration_start::date as round_date,
    s.model_id,
    'global' as scope_type,
    CAST(NULL AS TEXT) as scope_id,
    SUM(s.mase) as sum_mase,
    SUM(s.mase * s.mase) as sum_mase_sq,
    SUM(s.rmse) as sum_rmse,
    SUM(s.sql_score) as sum_sql,
    SUM(s.sql_score * s.sql_score) as sum_sql_sq,
    COUNT(s.sql_score) as num_sql,
    COUNT(*) as num_scores
FROM forecasts.scores s
JOIN challenges.rounds r ON s.round_id = r.id
WHERE s.mase IS NOT NULL
  AND s.mase != 'NaN'::double precision
  AND s.mase != 'Infinity'::double precision
  AND s.mase != '-Infinity'::double precision
  AND s.final_evaluation = TRUE
  AND r.is_cancelled = FALSE
  AND NOT EXISTS (
      SELECT 1 FROM challenges.definition_series_scd2 ds
      WHERE ds.definition_id = r.definition_id
        AND ds.series_id = s.series_id
        AND ds.is_excluded = TRUE
  )
GROUP BY r.id, r.registration_start::date, s.model_id

UNION ALL

-- Definition Scope
SELECT
    r.id as round_id,
    r.registration_start::date as round_date,
    s.model_id,
    'definition' as scope_type,
    CAST(r.definition_id AS TEXT) as scope_id,
    SUM(s.mase) as sum_mase,
    SUM(s.mase * s.mase) as sum_mase_sq,
    SUM(s.rmse) as sum_rmse,
    SUM(s.sql_score) as sum_sql,
    SUM(s.sql_score * s.sql_score) as sum_sql_sq,
    COUNT(s.sql_score) as num_sql,
    COUNT(*) as num_scores
FROM forecasts.scores s
JOIN challenges.rounds r ON s.round_id = r.id
WHERE s.mase IS NOT NULL
  AND s.mase != 'NaN'::double precision
  AND s.mase != 'Infinity'::double precision
  AND s.mase != '-Infinity'::double precision
  AND s.final_evaluation = TRUE
  AND r.is_cancelled = FALSE
  AND NOT EXISTS (
      SELECT 1 FROM challenges.definition_series_scd2 ds
      WHERE ds.definition_id = r.definition_id
        AND ds.series_id = s.series_id
        AND ds.is_excluded = TRUE
  )
GROUP BY r.id, r.registration_start::date, s.model_id, r.definition_id

UNION ALL

-- Frequency+Horizon Scope
SELECT
    r.id as round_id,
    r.registration_start::date as round_date,
    s.model_id,
    'frequency_horizon' as scope_type,
    CONCAT(d.frequency::text, '::', d.horizon::text) as scope_id,
    SUM(s.mase) as sum_mase,
    SUM(s.mase * s.mase) as sum_mase_sq,
    SUM(s.rmse) as sum_rmse,
    SUM(s.sql_score) as sum_sql,
    SUM(s.sql_score * s.sql_score) as sum_sql_sq,
    COUNT(s.sql_score) as num_sql,
    COUNT(*) as num_scores
FROM forecasts.scores s
JOIN challenges.rounds r ON s.round_id = r.id
JOIN challenges.definitions d ON r.definition_id = d.id
WHERE s.mase IS NOT NULL
  AND s.mase != 'NaN'::double precision
  AND s.mase != 'Infinity'::double precision
  AND s.mase != '-Infinity'::double precision
  AND s.final_evaluation = TRUE
  AND r.is_cancelled = FALSE
  AND NOT EXISTS (
      SELECT 1 FROM challenges.definition_series_scd2 ds
      WHERE ds.definition_id = r.definition_id
        AND ds.series_id = s.series_id
        AND ds.is_excluded = TRUE
  )
GROUP BY r.id, r.registration_start::date, s.model_id, d.frequency, d.horizon;

CREATE INDEX IF NOT EXISTS idx_round_scores_unique
ON forecasts.round_model_scores(round_id, model_id, scope_type);
CREATE INDEX IF NOT EXISTS idx_round_scores_date ON forecasts.round_model_scores(round_date);
CREATE INDEX IF NOT EXISTS idx_round_scores_model ON forecasts.round_model_scores(model_id);
CREATE INDEX IF NOT EXISTS idx_round_scores_scope ON forecasts.round_model_scores(scope_type, scope_id);

-- 4) Recreate dependent views ---------------------------------------------------------
-- v_ranking_base: new columns are appended at the END so CREATE OR REPLACE works without a
-- drop, keeping the drift view forecasts.v_model_series_mase (which depends on it) valid.
CREATE OR REPLACE VIEW forecasts.v_ranking_base AS
SELECT
    cs.round_id,
    cs.model_id,
    cs.series_id,
    cs.mase,
    cs.rmse,
    cs.final_evaluation,
    cs.calculated_at,
    cr.name AS round_name,
    cr.horizon,
    cr.end_time AS round_end_time,
    cr.start_time AS round_start_time,
    cr.definition_id,
    cd.name AS definition_name,
    cd.domains AS definition_domains,
    mi.name AS model_name,
    u.username,
    ts.name AS series_name,
    ts.frequency,
    ts.unique_id,
    dc.domain,
    dc.category,
    dc.subcategory,
    cs.sql_score,
    cs.has_quantiles
FROM forecasts.scores cs
JOIN challenges.rounds cr ON cr.id = cs.round_id
LEFT JOIN challenges.v_active_definitions cd ON cr.definition_id = cd.id
JOIN models.model_info mi ON mi.id = cs.model_id
JOIN auth.users u ON u.id = mi.user_id
JOIN data_portal.time_series ts ON ts.series_id = cs.series_id
LEFT JOIN data_portal.domain_category dc ON ts.domain_category_id = dc.id
WHERE cs.mase IS NOT NULL
  AND cs.mase != 'NaN'
  AND cs.mase != 'Infinity'
  AND cs.mase != '-Infinity'
  AND cs.final_evaluation
  AND cr.is_cancelled = FALSE
  AND NOT EXISTS (
      SELECT 1 FROM challenges.definition_series_scd2 ds
      WHERE ds.definition_id = cr.definition_id
        AND ds.series_id = cs.series_id
        AND ds.is_excluded = TRUE
  );

-- leaderboard + monthly gain columns in the middle (SELECT dr.* fan-out), so they must be
-- dropped and recreated. v_monthly depends on the leaderboard → drop it first.
DROP VIEW IF EXISTS forecasts.v_monthly_and_latest_rankings;
DROP VIEW IF EXISTS forecasts.v_daily_rankings_leaderboard;

CREATE VIEW forecasts.v_daily_rankings_leaderboard AS
SELECT
    dr.id,
    dr.calculation_date,
    dr.elo_rating_median,
    dr.elo_ci_lower,
    dr.elo_ci_upper,
    dr.matches_played,
    dr.rank_position,
    dr.n_bootstraps,
    dr.calculation_duration_ms,
    dr.calculated_at,
    dr.scope_type,
    dr.scope_id,
    dr.metric,
    dr.avg_mase,
    dr.mase_std,
    dr.avg_rmse,
    dr.avg_sql,
    dr.sql_std,
    dr.evaluated_count,
    (dr.calculation_date = MAX(dr.calculation_date) OVER (PARTITION BY dr.scope_type, dr.scope_id, dr.metric)) AS is_latest,
    mi.id as model_id,
    mi.name as model_name,
    mi.readable_id,
    mi.model_family,
    mi.model_type,
    mi.architecture,
    mi.model_size,
    u.username,
    o.name as organization_name,
    CASE WHEN dr.scope_type = 'definition' THEN cd.id END as definition_id,
    CASE WHEN dr.scope_type = 'definition' THEN cd.name END as definition_name,
    CASE WHEN dr.scope_type = 'definition' THEN cd.schedule_id END as definition_schedule_id
FROM forecasts.daily_rankings dr
JOIN models.model_info mi ON dr.model_id = mi.id
JOIN auth.users u ON mi.user_id = u.id
LEFT JOIN auth.organizations o ON mi.organization_id = o.id
LEFT JOIN challenges.definitions cd ON dr.scope_type = 'definition' AND dr.scope_id = cd.id::text
ORDER BY dr.calculation_date DESC, dr.scope_type, dr.scope_id NULLS FIRST, dr.elo_rating_median DESC;

CREATE VIEW forecasts.v_monthly_and_latest_rankings AS
SELECT
    dr.*,
    (dr.calculation_date = (date_trunc('month', dr.calculation_date) + interval '1 month - 1 day')::date) as is_month_end
FROM forecasts.v_daily_rankings_leaderboard dr
WHERE
    dr.is_latest = TRUE
    OR (dr.calculation_date = (date_trunc('month', dr.calculation_date) + interval '1 month - 1 day')::date);

-- 5) Re-grant SELECT to the read-only role on the recreated objects -------------------
-- round_model_scores + the leaderboard/monthly views were DROP+CREATEd, so they lose any
-- prior grants. On DAG-UPB DBs `ALTER DEFAULT PRIVILEGES ... IN SCHEMA forecasts` already
-- re-grants automatically IF this migration is run as the object owner (internaluser); the
-- explicit GRANTs below make the migration correct regardless of who runs it or whether
-- default privileges are configured. Guarded so a DB without the role does not fail.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly_user') THEN
    GRANT SELECT ON forecasts.round_model_scores TO readonly_user;
    GRANT SELECT ON forecasts.v_ranking_base TO readonly_user;
    GRANT SELECT ON forecasts.v_daily_rankings_leaderboard TO readonly_user;
    GRANT SELECT ON forecasts.v_monthly_and_latest_rankings TO readonly_user;
  END IF;
END $$;

COMMIT;

-- Populate the rebuilt matview outside the transaction is unnecessary: CREATE MATERIALIZED
-- VIEW ... (default WITH DATA) already populated it above. A subsequent hourly refresh keeps
-- it current. To refresh immediately: REFRESH MATERIALIZED VIEW forecasts.round_model_scores;
