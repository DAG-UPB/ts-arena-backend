-- Weekly 30-day MASE: per Monday since 2025-01-06, the displayed MASE (geometric mean over the
-- rounds ended in the 30 days before) and the MASE relative to the field, per scope and model.
-- Idempotent; run as the schema owner. Adds forecasts.weekly_mase_30d, changes nothing that exists.
-- The method literal must equal MASE_METHOD; a method change re-creates the view.
-- Refreshed by the Arena Score job. Definition matches init_db.sql.

BEGIN;

CREATE MATERIALIZED VIEW IF NOT EXISTS forecasts.weekly_mase_30d AS
WITH scored AS (
    SELECT f.round_id, f.series_id, f.model_id, r.definition_id,
           d.frequency::text AS frequency, d.horizon::text AS horizon,
           (r.end_time AT TIME ZONE 'UTC')::date AS end_day,
           f.mase
    FROM forecasts.forecast_scores f
    JOIN challenges.rounds r ON r.id = f.round_id
    JOIN challenges.definitions d ON d.id = r.definition_id
    WHERE f.final_evaluation
      AND f.evaluation_status IN ('complete', 'partial')
      AND f.mase IS NOT NULL
      AND f.method = 'hk2006_context_daily'
      AND NOT COALESCE(r.is_cancelled, FALSE)
      AND r.end_time >= TIMESTAMPTZ '2024-12-07 00:00:00+00'
      AND NOT EXISTS (
          SELECT 1 FROM challenges.definition_series_scd2 ds
          WHERE ds.definition_id = r.definition_id
            AND ds.series_id = f.series_id
            AND ds.is_excluded
      )
),
field AS (
    SELECT round_id, series_id, avg(ln(mase)) AS field_log
    FROM scored
    WHERE mase > 0
    GROUP BY round_id, series_id
),
day_model AS (
    SELECT s.end_day, s.definition_id, s.model_id,
           count(*) AS tasks,
           count(*) FILTER (WHERE s.mase > 0) AS mase_positive,
           sum(ln(s.mase)) FILTER (WHERE s.mase > 0) AS mase_log_sum,
           sum(ln(s.mase) - fl.field_log) FILTER (WHERE s.mase > 0) AS rel_log_sum
    FROM scored s
    LEFT JOIN field fl ON fl.round_id = s.round_id AND fl.series_id = s.series_id
    GROUP BY s.end_day, s.definition_id, s.model_id
),
day_definition AS (
    SELECT end_day, definition_id, frequency, horizon,
           count(DISTINCT (round_id, series_id)) AS tasks
    FROM scored
    GROUP BY end_day, definition_id, frequency, horizon
),
scope_map AS (
    SELECT DISTINCT definition_id, 'global'::text AS scope_type, NULL::text AS scope_id
    FROM day_definition
    UNION
    SELECT DISTINCT definition_id, 'definition', definition_id::text
    FROM day_definition
    UNION
    SELECT DISTINCT definition_id, 'frequency_horizon', frequency || '::' || horizon
    FROM day_definition
    WHERE frequency IS NOT NULL AND horizon IS NOT NULL
),
weeks AS (
    SELECT w::date AS week
    FROM generate_series(
        TIMESTAMP '2025-01-06',
        date_trunc('week', now() AT TIME ZONE 'UTC'),
        INTERVAL '7 days'
    ) AS w
),
scope_week AS (
    SELECT w.week, m.scope_type, m.scope_id, sum(dd.tasks) AS scope_tasks
    FROM weeks w
    JOIN day_definition dd ON dd.end_day >= w.week - 30 AND dd.end_day < w.week
    JOIN scope_map m ON m.definition_id = dd.definition_id
    GROUP BY w.week, m.scope_type, m.scope_id
),
model_week AS (
    SELECT w.week, m.scope_type, m.scope_id, dm.model_id,
           sum(dm.tasks) AS tasks,
           sum(dm.mase_positive) AS mase_positive,
           sum(dm.mase_log_sum) AS mase_log_sum,
           sum(dm.rel_log_sum) AS rel_log_sum
    FROM weeks w
    JOIN day_model dm ON dm.end_day >= w.week - 30 AND dm.end_day < w.week
    JOIN scope_map m ON m.definition_id = dm.definition_id
    GROUP BY w.week, m.scope_type, m.scope_id, dm.model_id
)
SELECT mw.week, mw.scope_type, mw.scope_id, mw.model_id,
       CASE WHEN 5 * mw.tasks >= 4 * sw.scope_tasks AND mw.mase_positive > 0
            THEN exp(mw.mase_log_sum / mw.mase_positive) END AS mase_30d,
       CASE WHEN 5 * mw.tasks >= 4 * sw.scope_tasks AND mw.mase_positive > 0
            THEN exp(mw.rel_log_sum / mw.mase_positive) END AS mase_rel_30d,
       mw.tasks::double precision / sw.scope_tasks AS participation_30d,
       mw.tasks::integer AS tasks_30d,
       sw.scope_tasks::integer AS scope_tasks
FROM model_week mw
JOIN scope_week sw
  ON sw.week = mw.week
 AND sw.scope_type = mw.scope_type
 AND sw.scope_id IS NOT DISTINCT FROM mw.scope_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_weekly_mase_30d_unique
ON forecasts.weekly_mase_30d (week, scope_type, scope_id, model_id) NULLS NOT DISTINCT;

CREATE INDEX IF NOT EXISTS idx_weekly_mase_30d_scope
ON forecasts.weekly_mase_30d (scope_type, scope_id, week);

COMMIT;
