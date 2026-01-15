-- ==========================================================
-- Data Portal - TimescaleDB
-- ==========================================================

-- === Extensions ===
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- ==========================================================
-- 0) Schema-Definition
-- ==========================================================
CREATE SCHEMA IF NOT EXISTS data_portal;
SET search_path TO data_portal, public;

-- ==========================================================
-- 1) Utility Function: updated_at Trigger
-- ==========================================================
CREATE OR REPLACE FUNCTION data_portal.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ==========================================================
-- 2) Domain- / Category-Hierarchie
-- ==========================================================
CREATE TABLE data_portal.domain_category (
  id SERIAL PRIMARY KEY,
  domain TEXT NOT NULL,
  category TEXT,
  subcategory TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(domain, category, subcategory)
);

CREATE TRIGGER trg_domain_category_updated_at
BEFORE UPDATE ON data_portal.domain_category
FOR EACH ROW
EXECUTE FUNCTION data_portal.update_updated_at_column();

-- ==========================================================
-- 3) Time Series Metadata
-- ==========================================================
CREATE TABLE data_portal.time_series (
  series_id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  description TEXT,
  api_endpoint TEXT,
  frequency INTERVAL,
  unit TEXT,
  update_frequency TEXT,
  update_frequency_timepoint TEXT,
  ts_timezone TEXT,
  domain_category_id INTEGER REFERENCES data_portal.domain_category(id),
  endpoint_prefix TEXT UNIQUE NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_time_series_updated_at
BEFORE UPDATE ON data_portal.time_series
FOR EACH ROW
EXECUTE FUNCTION data_portal.update_updated_at_column();

-- ==========================================================
-- 4) Operational Time Series Data
-- ==========================================================
CREATE TABLE data_portal.time_series_data (
  series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ,
  PRIMARY KEY(series_id, ts)
);

SELECT create_hypertable('data_portal.time_series_data', 'ts', 'series_id', 4, 
                         chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

SELECT add_retention_policy('data_portal.time_series_data', INTERVAL '200 days', if_not_exists => TRUE);

CREATE TRIGGER trg_time_series_data_updated_at
BEFORE UPDATE ON data_portal.time_series_data
FOR EACH ROW
EXECUTE FUNCTION data_portal.update_updated_at_column();

-- ==========================================================
-- 5) Historical Time Series Data (SCD Typ 2)
-- ==========================================================
CREATE TABLE data_portal.time_series_data_scd2 (
  sk BIGSERIAL,
  series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  valid_to TIMESTAMPTZ,
  valid_during TSTZRANGE NOT NULL GENERATED ALWAYS AS (tstzrange(valid_from, valid_to, '[)')) STORED,
  is_current BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ,
  PRIMARY KEY (sk, valid_from)
);

CREATE UNIQUE INDEX uq_tsd2_current ON data_portal.time_series_data_scd2(series_id, ts, valid_from) WHERE is_current = TRUE;

-- idx for PIT-Queries (GiST Range)
CREATE INDEX idx_tsd2_valid_during
  ON data_portal.time_series_data_scd2 USING GIST(series_id, ts, valid_during);

-- Hypertable over valid_from
SELECT create_hypertable(
  'data_portal.time_series_data_scd2',
  'valid_from',
  chunk_time_interval => INTERVAL '1 month',
  if_not_exists => TRUE
);

-- Compression & Policy
ALTER TABLE data_portal.time_series_data_scd2 SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'series_id, ts',
  timescaledb.compress_orderby = 'valid_from DESC'
);
SELECT add_compression_policy('data_portal.time_series_data_scd2', INTERVAL '120 days', if_not_exists => TRUE);

CREATE TRIGGER trg_tsd2_updated_at
BEFORE UPDATE ON data_portal.time_series_data_scd2
FOR EACH ROW
EXECUTE FUNCTION data_portal.update_updated_at_column();

-- ==========================================================
-- 6) Function: Point-in-time Query
-- ==========================================================
CREATE OR REPLACE FUNCTION data_portal.f_time_series_data_at_time(
  at_time TIMESTAMPTZ,
  p_series_id INTEGER DEFAULT NULL
)
RETURNS TABLE(
  series_id INTEGER,
  ts TIMESTAMPTZ,
  value DOUBLE PRECISION,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
  SELECT
      scd.series_id,
      scd.ts,
      scd.value,
      scd.valid_from,
      scd.valid_to
  FROM data_portal.time_series_data_scd2 scd
  WHERE scd.valid_during @> at_time
    AND (p_series_id IS NULL OR scd.series_id = p_series_id);
END;
$$ LANGUAGE plpgsql STABLE;

-- ==========================================================
-- API Portal - Schemas
-- ==========================================================

-- === Schema: auth ===
CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE auth.organizations (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE auth.users (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE,
    organization_id INTEGER REFERENCES auth.organizations(id),
    user_type TEXT DEFAULT 'external', -- 'external' or 'internal'
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_users_organization ON auth.users(organization_id);

CREATE TABLE auth.api_keys (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES auth.users(id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL UNIQUE,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    last_used TIMESTAMPTZ
);
CREATE INDEX idx_api_keys_user_id ON auth.api_keys(user_id);

-- === Schema: models ===
CREATE SCHEMA IF NOT EXISTS models;

CREATE TABLE models.model_info (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES auth.users(id) ON DELETE CASCADE,
    organization_id INTEGER REFERENCES auth.organizations(id),
    name TEXT NOT NULL,
    readable_id TEXT UNIQUE,
    model_type TEXT,
    model_family TEXT,
    model_size INTEGER, -- in millions
    hosting TEXT,
    architecture TEXT,
    pretraining_data TEXT,
    publishing_date DATE,
    parameters JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (user_id, name)
);
CREATE INDEX idx_model_info_organization ON models.model_info(organization_id);

-- === Schema: challenges ===
CREATE SCHEMA IF NOT EXISTS challenges;

CREATE TABLE challenges.challenges (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    context_length INTEGER NOT NULL,
    registration_start TIMESTAMPTZ,
    registration_end TIMESTAMPTZ,
    horizon INTERVAL NOT NULL,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    preparation_params JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

COMMENT ON COLUMN challenges.challenges.context_length IS 
'Number of historical data points to use as context for forecasting';

COMMENT ON COLUMN challenges.challenges.preparation_params IS 
'JSON object containing parameters for context data preparation';

CREATE TABLE challenges.challenge_participants (
    id SERIAL PRIMARY KEY,
    challenge_id INTEGER REFERENCES challenges.challenges(id) ON DELETE CASCADE,
    model_id INTEGER REFERENCES models.model_info(id) ON DELETE CASCADE,
    registered_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (challenge_id, model_id)
);

CREATE TABLE challenges.challenge_context_data (
    id BIGSERIAL,
    challenge_id INTEGER REFERENCES challenges.challenges(id) ON DELETE CASCADE,
    series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION,
    metadata JSONB,
    PRIMARY KEY (id, ts),
    UNIQUE (challenge_id, series_id, ts)
);
SELECT create_hypertable('challenges.challenge_context_data', 'ts', if_not_exists => TRUE);
CREATE INDEX idx_context_challenge_series ON challenges.challenge_context_data(challenge_id, series_id);

-- Stores the selected (pseudo) series name per challenge and series
CREATE TABLE challenges.challenge_series_pseudo (
  id SERIAL PRIMARY KEY,
  challenge_id INTEGER REFERENCES challenges.challenges(id) ON DELETE CASCADE,
  series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
  challenge_series_name TEXT NOT NULL,
  min_ts TIMESTAMPTZ,
  max_ts TIMESTAMPTZ,
  value_avg DOUBLE PRECISION,
  value_std DOUBLE PRECISION,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (challenge_id, series_id)
);

COMMENT ON COLUMN challenges.challenge_series_pseudo.min_ts IS 
'First timestamp in the context data for this series';

COMMENT ON COLUMN challenges.challenge_series_pseudo.max_ts IS 
'Last timestamp in the context data for this series';

COMMENT ON COLUMN challenges.challenge_series_pseudo.value_avg IS 
'Average value of the context data for this series';

COMMENT ON COLUMN challenges.challenge_series_pseudo.value_std IS 
'Standard deviation of the context data for this series';

CREATE INDEX idx_challenge_series_pseudo_challenge ON challenges.challenge_series_pseudo(challenge_id);

-- === Schema: forecasts ===
CREATE SCHEMA IF NOT EXISTS forecasts;

CREATE TABLE forecasts.forecasts (
    id BIGSERIAL,
    challenge_id INTEGER REFERENCES challenges.challenges(id) ON DELETE CASCADE,
    model_id INTEGER REFERENCES models.model_info(id) ON DELETE CASCADE,
    series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    predicted_value DOUBLE PRECISION NOT NULL,
    probabilistic_values JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (id, ts),
    UNIQUE (challenge_id, model_id, series_id, ts)
);
SELECT create_hypertable('forecasts.forecasts', 'ts', if_not_exists => TRUE);

CREATE TABLE forecasts.challenge_scores (
    id SERIAL PRIMARY KEY,
    challenge_id INTEGER REFERENCES challenges.challenges(id) ON DELETE CASCADE,
    model_id INTEGER REFERENCES models.model_info(id) ON DELETE CASCADE,
    series_id INTEGER REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    mase DOUBLE PRECISION,
    rmse DOUBLE PRECISION,
    forecast_count INTEGER DEFAULT 0,
    actual_count INTEGER DEFAULT 0,
    evaluated_count INTEGER DEFAULT 0,
    data_coverage DOUBLE PRECISION DEFAULT 0,
    final_evaluation BOOLEAN DEFAULT FALSE,
    evaluation_status TEXT DEFAULT 'pending',
    error_message TEXT,
    calculated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (challenge_id, model_id, series_id)
);

-- ==========================================================
-- View: Challenge Status
-- ==========================================================
CREATE OR REPLACE VIEW challenges.v_challenges_with_status AS
SELECT
    c.*,
    CASE
        WHEN NOW() < c.registration_start THEN 'announced'
        WHEN NOW() >= c.registration_start AND NOW() <= c.registration_end THEN 'registration'
        WHEN NOW() > c.registration_end AND NOW() <= c.end_time THEN 'active'
        WHEN NOW() > c.end_time THEN 'completed'
        ELSE 'undefined'
    END AS status
FROM
    challenges.challenges c;

-- ==========================================================
-- View: Challenge Context Data Range
-- ==========================================================
CREATE OR REPLACE VIEW challenges.v_challenge_context_data_range AS 
SELECT DISTINCT
    challenge_id,
    series_id,
    MIN(ts) OVER (PARTITION BY challenge_id, series_id) AS min_ts,
    MAX(ts) OVER (PARTITION BY challenge_id, series_id) AS max_ts,
    FIRST_VALUE(value) OVER (
        PARTITION BY challenge_id, series_id
        ORDER BY ts DESC
    ) AS latest_value
FROM challenges.challenge_context_data;

-- ==========================================================
-- View: Challenges with Metadata (for filtering)
-- ==========================================================
CREATE OR REPLACE VIEW challenges.v_challenges_with_metadata AS
SELECT
    c.id as challenge_id,
    c.name,
    c.description,
    c.registration_start,
    c.registration_end,
    c.start_time,
    c.end_time,
    c.context_length,
    c.horizon,
    c.preparation_params,
    c.created_at,
    c.updated_at,
    -- Status aus v_challenges_with_status
    CASE
        WHEN NOW() < c.registration_start THEN 'announced'
        WHEN NOW() >= c.registration_start AND NOW() <= c.registration_end THEN 'registration'
        WHEN NOW() > c.registration_end AND NOW() <= c.end_time THEN 'active'
        WHEN NOW() > c.end_time THEN 'completed'
        ELSE 'undefined'
    END AS status,
    -- Zeitreihen-Statistik
    COUNT(DISTINCT csp.series_id) as n_time_series,
    -- Aggregierte Domain-Informationen (Arrays)
    ARRAY_AGG(DISTINCT dc.domain ORDER BY dc.domain) 
        FILTER (WHERE dc.domain IS NOT NULL) AS domains,
    ARRAY_AGG(DISTINCT dc.category ORDER BY dc.category) 
        FILTER (WHERE dc.category IS NOT NULL) AS categories,
    ARRAY_AGG(DISTINCT dc.subcategory ORDER BY dc.subcategory) 
        FILTER (WHERE dc.subcategory IS NOT NULL) AS subcategories,
    -- Aggregierte Frequencies (als INTERVAL Array)
    ARRAY_AGG(DISTINCT ts.frequency ORDER BY ts.frequency) 
        FILTER (WHERE ts.frequency IS NOT NULL) AS frequencies,
    -- Model & Forecast Counts (Subqueries für Performance)
    (SELECT COUNT(DISTINCT f.model_id) 
     FROM forecasts.forecasts f 
     WHERE f.challenge_id = c.id) AS model_count,
    (SELECT COUNT(*) 
     FROM forecasts.forecasts f 
     WHERE f.challenge_id = c.id) AS forecast_count
FROM challenges.challenges c
LEFT JOIN challenges.challenge_series_pseudo csp ON csp.challenge_id = c.id
LEFT JOIN data_portal.time_series ts ON ts.series_id = csp.series_id
LEFT JOIN data_portal.domain_category dc ON ts.domain_category_id = dc.id
GROUP BY 
    c.id, c.name, c.description, c.registration_start, c.registration_end,
    c.start_time, c.end_time, c.context_length, c.horizon, 
    c.preparation_params, c.created_at, c.updated_at;

COMMENT ON VIEW challenges.v_challenges_with_metadata IS 
'Challenges mit aggregierten Metadaten für erweiterte Filterung.
Enthält Arrays von domains, categories, subcategories und frequencies 
aller zugehörigen Zeitreihen.';

-- ==========================================================
-- 7) View: Data Availability Check
-- ==========================================================
CREATE OR REPLACE VIEW data_portal.v_data_availability AS
SELECT 
    ts.series_id,
    ts.name,
    ts.frequency,
    ts.endpoint_prefix,
    dc.domain,
    dc.category,
    dc.subcategory,
    MAX(tsd.ts) as last_data_timestamp,
    NOW() as current_timestamp,
    CASE 
        WHEN ts.frequency = '1 hour' THEN NOW() - INTERVAL '6 hours'
        WHEN ts.frequency = '1 day' THEN NOW() - INTERVAL '2 days'
        WHEN ts.frequency = '15 minutes' THEN NOW() - INTERVAL '6 hours'
        ELSE NOW() - INTERVAL '2 days'
    END as expected_threshold,
    CASE 
        WHEN MAX(tsd.ts) >= CASE 
            WHEN ts.frequency = '1 hour' THEN NOW() - INTERVAL '12 hours'
            WHEN ts.frequency = '1 day' THEN NOW() - INTERVAL '2 days'
            WHEN ts.frequency = '15 minutes' THEN NOW() - INTERVAL '6 hours'
            ELSE NOW() - INTERVAL '2 days'
        END THEN TRUE
        ELSE FALSE
    END as has_recent_data
FROM data_portal.time_series ts
LEFT JOIN data_portal.time_series_data tsd ON ts.series_id = tsd.series_id
LEFT JOIN data_portal.domain_category dc ON ts.domain_category_id = dc.id
GROUP BY ts.series_id, ts.name, ts.frequency, ts.endpoint_prefix, dc.domain, dc.category, dc.subcategory;

COMMENT ON VIEW data_portal.v_data_availability IS 
'Checks if time series have recent data based on their configured frequency:
- 1 hour: data within last 6 hours
- 1 day: data within last 2 days
- 15 minutes: data within last 6 hours
- default: data within last 2 days';

-- ==========================================================
-- 8) View: Ranking Base (for enhanced model rankings)
-- ==========================================================
CREATE OR REPLACE VIEW forecasts.v_ranking_base AS
SELECT
    cs.challenge_id,
    cs.model_id,
    cs.series_id,
    cs.mase,
    cs.rmse,
    cs.final_evaluation,
    cs.calculated_at,
    -- Challenge Info
    c.name AS challenge_name,
    c.horizon,
    c.end_time AS challenge_end_time,
    c.start_time AS challenge_start_time,
    -- Model Info
    mi.name AS model_name,
    u.username,
    -- Time Series Info
    ts.name AS series_name,
    ts.frequency,
    ts.endpoint_prefix,
    -- Domain Info
    dc.domain,
    dc.category,
    dc.subcategory
FROM forecasts.challenge_scores cs
JOIN challenges.challenges c ON c.id = cs.challenge_id
JOIN models.model_info mi ON mi.id = cs.model_id
JOIN auth.users u ON u.id = mi.user_id
JOIN data_portal.time_series ts ON ts.series_id = cs.series_id
LEFT JOIN data_portal.domain_category dc ON ts.domain_category_id = dc.id
WHERE cs.mase IS NOT NULL
  AND cs.mase != 'NaN'
  AND cs.mase != 'Infinity'
  AND cs.mase != '-Infinity';

COMMENT ON VIEW forecasts.v_ranking_base IS 
'Base view for model rankings with all filter dimensions. Filters out invalid MASE values (NULL, NaN, Infinity).';

-- ==========================================================
-- 9) Indexes for Ranking Performance
-- ==========================================================

-- Index for challenge_scores lookup
CREATE INDEX IF NOT EXISTS idx_challenge_scores_lookup 
ON forecasts.challenge_scores(challenge_id, model_id, series_id) 
WHERE mase IS NOT NULL;

-- Index for time-based filtering
CREATE INDEX IF NOT EXISTS idx_challenges_end_time 
ON challenges.challenges(end_time) 
WHERE end_time IS NOT NULL;

-- Index for domain/frequency filtering
CREATE INDEX IF NOT EXISTS idx_time_series_domain_category 
ON data_portal.time_series(domain_category_id, frequency);

-- Index for domain category lookup
CREATE INDEX IF NOT EXISTS idx_domain_category_lookup 
ON data_portal.domain_category(domain, category, subcategory);

-- Index for model_info user lookup
CREATE INDEX IF NOT EXISTS idx_model_info_user 
ON models.model_info(user_id);

-- Index für challenge_series_pseudo
CREATE INDEX IF NOT EXISTS idx_challenge_series_pseudo_series
ON challenges.challenge_series_pseudo(series_id);

-- Index für challenges (Zeit-basierte Filterung)
CREATE INDEX IF NOT EXISTS idx_challenges_time_range
ON challenges.challenges(registration_start, registration_end, end_time);

-- ==========================================================
-- Final message
-- ==========================================================
DO $$
BEGIN
  RAISE NOTICE '=== Data Portal schema initialized successfully ===';
END $$;