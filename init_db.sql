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
-- 0) Schema Definition
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
-- 2) Domain / Category Hierarchy
-- ==========================================================
CREATE TABLE data_portal.domain_category (
  id SERIAL PRIMARY KEY,
  domain TEXT NOT NULL,
  subdomain TEXT,
  category TEXT,
  subcategory TEXT,
  aggregation_level TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(domain, subdomain, category, subcategory, aggregation_level)
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
  aggregation_level_name TEXT,
  unit TEXT,
  update_frequency TEXT,
  ts_timezone TEXT,
  imputation_policy TEXT,
  domain_category_id INTEGER REFERENCES data_portal.domain_category(id),
  unique_id TEXT UNIQUE NOT NULL,
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

SELECT add_retention_policy('data_portal.time_series_data', INTERVAL '5 years', if_not_exists => TRUE);

CREATE TRIGGER trg_time_series_data_updated_at
BEFORE UPDATE ON data_portal.time_series_data
FOR EACH ROW
EXECUTE FUNCTION data_portal.update_updated_at_column();

-- ==========================================================
-- 5) Historical Time Series Data (SCD Type 2)
-- ==========================================================
CREATE TABLE data_portal.time_series_data_scd2 (
  sk BIGSERIAL,
  series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL,
  value DOUBLE PRECISION,  -- NULL allowed for gap markers
  quality_code SMALLINT NOT NULL DEFAULT 0,  -- 0 = Original, 1 = Imputed
  valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  valid_to TIMESTAMPTZ,
  valid_during TSTZRANGE NOT NULL GENERATED ALWAYS AS (tstzrange(valid_from, valid_to, '[)')) STORED,
  is_current BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ,
  PRIMARY KEY (sk, valid_from)
);

COMMENT ON COLUMN data_portal.time_series_data_scd2.quality_code IS 
'Data quality flag: 0 = Original (raw data), 1 = Imputed (interpolated/filled). NULL values with quality_code=1 indicate gaps too large for interpolation.';

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
  quality_code SMALLINT,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ
) AS $$
BEGIN
  RETURN QUERY
  SELECT
      scd.series_id,
      scd.ts,
      scd.value,
      scd.quality_code,
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

-- ==========================================================
-- Challenge Definitions (from YAML configuration)
-- ==========================================================
CREATE TABLE challenges.definitions (
    id SERIAL PRIMARY KEY,
    schedule_id TEXT UNIQUE NOT NULL,     -- YAML id: "smard_dam_challenge_24h_15min"
    name TEXT NOT NULL,                    -- Human readable name
    description TEXT,
    domains TEXT[],                        -- Domain filters for time series selection
    subdomains TEXT[],                     -- Subdomain filters
    categories TEXT[],                     -- Categories filters
    subcategories TEXT[],                  -- Subcategories filters
    context_length INTEGER NOT NULL,
    horizon INTERVAL NOT NULL,
    frequency INTERVAL NOT NULL,
    cron_schedule TEXT,                    -- Cron expression: "0 14 * * *"
    n_time_series INTEGER NOT NULL,        -- Target number of time series to include
    registration_duration INTERVAL,        -- Duration of registration window
    evaluation_delay INTERVAL DEFAULT '0 hours',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    run_on_startup BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE challenges.definitions IS 
'Challenge definitions/templates from YAML configuration. Each represents a recurring challenge type.';

COMMENT ON COLUMN challenges.definitions.schedule_id IS 
'Unique identifier matching the YAML schedule id (e.g., smard_dam_challenge_24h_15min)';

COMMENT ON COLUMN challenges.definitions.domains IS 
'List of domains to filter time series by (e.g., ["Smard", "Fingrid"])';

COMMENT ON COLUMN challenges.definitions.subdomains IS 
'List of subdomains to filter time series by';

COMMENT ON COLUMN challenges.definitions.categories IS 
'List of categories to filter time series by';

COMMENT ON COLUMN challenges.definitions.subcategories IS 
'List of subcategories to filter time series by';

COMMENT ON COLUMN challenges.definitions.is_active IS 
'When FALSE, scheduler skips this challenge. Historical rounds remain accessible.';

COMMENT ON COLUMN challenges.definitions.frequency IS 
'Target frequency for this challenge (e.g., 15 minutes, 1 hour). Determines which aggregation view to use.';

-- ==========================================================
-- Challenge Definition Series (SCD Type 2)
-- ==========================================================
CREATE TABLE challenges.definition_series_scd2 (
    sk BIGSERIAL PRIMARY KEY,
    definition_id INTEGER NOT NULL REFERENCES challenges.definitions(id) ON DELETE CASCADE,
    series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    is_required BOOLEAN NOT NULL DEFAULT TRUE,  -- Required series vs optional pool
    valid_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    valid_to TIMESTAMPTZ,                       -- NULL = currently active
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ensure only one current assignment per definition + series
CREATE UNIQUE INDEX uq_def_series_current 
ON challenges.definition_series_scd2(definition_id, series_id) 
WHERE is_current = TRUE;

CREATE INDEX idx_def_series_definition ON challenges.definition_series_scd2(definition_id);
CREATE INDEX idx_def_series_series ON challenges.definition_series_scd2(series_id);

COMMENT ON TABLE challenges.definition_series_scd2 IS 
'SCD Type 2 table tracking which time series are assigned to each challenge definition over time.
When a series is added or removed, the previous record is closed (valid_to set, is_current=FALSE) 
and a new record is created.';

COMMENT ON COLUMN challenges.definition_series_scd2.is_required IS 
'TRUE = required series that must be included. FALSE = part of optional random selection pool.';

-- ==========================================================
-- Challenge Rounds (individual instances of challenge definitions)
-- ==========================================================
CREATE TABLE challenges.rounds (
    id SERIAL PRIMARY KEY,
    definition_id INTEGER REFERENCES challenges.definitions(id) ON DELETE CASCADE,
    name TEXT UNIQUE NOT NULL,             -- Generated: "definition description - timestamp"
    description TEXT,
    context_length INTEGER NOT NULL,
    horizon INTERVAL NOT NULL,
    frequency INTERVAL,
    registration_start TIMESTAMPTZ,
    registration_end TIMESTAMPTZ,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    status TEXT DEFAULT 'registration' 
        CHECK (status IN ('registration', 'active', 'completed', 'cancelled')),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE challenges.rounds IS 
'Individual challenge round instances. Each row represents one execution of a challenge_definition.';

COMMENT ON COLUMN challenges.rounds.definition_id IS 
'Links to the parent challenge definition.';

COMMENT ON COLUMN challenges.rounds.status IS 
'Lifecycle status: announced (pre-registration), registration (accepting participants), 
active (forecasting in progress), completed (ended), cancelled (aborted).';

COMMENT ON COLUMN challenges.rounds.context_length IS 
'Number of historical data points to use as context for forecasting';

CREATE INDEX idx_rounds_definition ON challenges.rounds(definition_id);
CREATE INDEX idx_rounds_status ON challenges.rounds(status);
CREATE INDEX idx_rounds_time_range ON challenges.rounds(registration_start, registration_end, end_time);

-- ==========================================================
-- Challenge Participants
-- ==========================================================
CREATE TABLE challenges.participants (
    id SERIAL PRIMARY KEY,
    round_id INTEGER REFERENCES challenges.rounds(id) ON DELETE CASCADE,
    model_id INTEGER REFERENCES models.model_info(id) ON DELETE CASCADE,
    registered_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (round_id, model_id)
);

CREATE INDEX idx_participants_round ON challenges.participants(round_id);

-- ==========================================================
-- Challenge Context Data
-- ==========================================================
CREATE TABLE challenges.context_data (
    id BIGSERIAL,
    round_id INTEGER REFERENCES challenges.rounds(id) ON DELETE CASCADE,
    series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION,
    metadata JSONB,
    PRIMARY KEY (id, ts),
    UNIQUE (round_id, series_id, ts)
);
SELECT create_hypertable('challenges.context_data', 'ts', if_not_exists => TRUE);
CREATE INDEX idx_context_round_series ON challenges.context_data(round_id, series_id);

-- ==========================================================
-- Challenge Series Pseudo (anonymized series names per round)
-- ==========================================================
CREATE TABLE challenges.series_pseudo (
  id SERIAL PRIMARY KEY,
  round_id INTEGER REFERENCES challenges.rounds(id) ON DELETE CASCADE,
  series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
  challenge_series_name TEXT NOT NULL,
  min_ts TIMESTAMPTZ,
  max_ts TIMESTAMPTZ,
  value_avg DOUBLE PRECISION,
  value_std DOUBLE PRECISION,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (round_id, series_id)
);

COMMENT ON COLUMN challenges.series_pseudo.min_ts IS 
'First timestamp in the context data for this series';

COMMENT ON COLUMN challenges.series_pseudo.max_ts IS 
'Last timestamp in the context data for this series';

COMMENT ON COLUMN challenges.series_pseudo.value_avg IS 
'Average value of the context data for this series';

COMMENT ON COLUMN challenges.series_pseudo.value_std IS 
'Standard deviation of the context data for this series';

CREATE INDEX idx_series_pseudo_round ON challenges.series_pseudo(round_id);

-- === Schema: forecasts ===
CREATE SCHEMA IF NOT EXISTS forecasts;

CREATE TABLE forecasts.forecasts (
    id BIGSERIAL,
    round_id INTEGER REFERENCES challenges.rounds(id) ON DELETE CASCADE,
    model_id INTEGER REFERENCES models.model_info(id) ON DELETE CASCADE,
    series_id INTEGER NOT NULL REFERENCES data_portal.time_series(series_id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    predicted_value DOUBLE PRECISION NOT NULL,
    probabilistic_values JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (id, ts),
    UNIQUE (round_id, model_id, series_id, ts)
);
SELECT create_hypertable('forecasts.forecasts', 'ts', if_not_exists => TRUE);
CREATE INDEX idx_forecasts_round ON forecasts.forecasts(round_id);

CREATE TABLE forecasts.scores (
    id SERIAL PRIMARY KEY,
    round_id INTEGER REFERENCES challenges.rounds(id) ON DELETE CASCADE,
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
    UNIQUE (round_id, model_id, series_id)
);
CREATE INDEX idx_scores_round ON forecasts.scores(round_id);

-- ==========================================================
-- View: Challenge Round Status (computed from timestamps)
-- ==========================================================
CREATE OR REPLACE VIEW challenges.v_rounds_with_status AS
SELECT
    cr.*,
    cd.schedule_id AS definition_schedule_id,
    cd.name AS definition_name,
    cd.domains AS definition_domains,
    cd.subdomains AS definition_subdomains,
    cd.categories AS definition_categories,
    cd.subcategories AS definition_subcategories,
    CASE
        WHEN NOW() >= cr.registration_start AND NOW() <= cr.registration_end THEN 'registration'
        WHEN NOW() > cr.registration_end AND NOW() <= cr.end_time THEN 'active'
        WHEN NOW() > cr.end_time THEN 'completed'
        ELSE 'undefined'
    END AS computed_status
FROM challenges.rounds cr
LEFT JOIN challenges.definitions cd ON cr.definition_id = cd.id;

-- ==========================================================
-- View: Challenge Context Data Range
-- ==========================================================
CREATE OR REPLACE VIEW challenges.v_context_data_range AS 
SELECT DISTINCT
    round_id,
    series_id,
    MIN(ts) OVER (PARTITION BY round_id, series_id) AS min_ts,
    MAX(ts) OVER (PARTITION BY round_id, series_id) AS max_ts,
    FIRST_VALUE(value) OVER (
        PARTITION BY round_id, series_id
        ORDER BY ts DESC
    ) AS latest_value
FROM challenges.context_data;

-- ==========================================================
-- View: Challenge Rounds with Metadata (for participant filtering)
-- ==========================================================
CREATE OR REPLACE VIEW challenges.v_rounds_with_metadata AS
SELECT
    cr.id as round_id,
    cr.name,
    cr.description,
    cr.definition_id,
    cd.schedule_id AS definition_schedule_id,
    cd.name AS definition_name,
    cd.domains,
    cd.subdomains,
    cd.categories as definition_categories,
    cd.subcategories as definition_subcategories,
    cr.registration_start,
    cr.registration_end,
    cr.start_time,
    cr.end_time,
    cr.context_length,
    cr.horizon,
    cr.frequency,
    cr.status,
    cr.created_at,
    cr.updated_at,
    -- Computed status from timestamps (fallback)
    CASE
        WHEN NOW() >= cr.registration_start AND NOW() <= cr.registration_end THEN 'registration'
        WHEN NOW() > cr.registration_end AND NOW() <= cr.end_time THEN 'active'
        WHEN NOW() > cr.end_time THEN 'completed'
        ELSE 'undefined'
    END AS computed_status,
    -- Time series statistics
    COUNT(DISTINCT csp.series_id) as n_time_series,
    -- Aggregated domain information (arrays)
    ARRAY_AGG(DISTINCT dc.category ORDER BY dc.category) 
        FILTER (WHERE dc.category IS NOT NULL) AS categories,
    ARRAY_AGG(DISTINCT dc.subcategory ORDER BY dc.subcategory) 
        FILTER (WHERE dc.subcategory IS NOT NULL) AS subcategories,
    -- Model & Forecast Counts (subqueries for performance)
    (SELECT COUNT(DISTINCT f.model_id) 
     FROM forecasts.forecasts f 
     WHERE f.round_id = cr.id) AS model_count,
    (SELECT COUNT(*) 
     FROM forecasts.forecasts f 
     WHERE f.round_id = cr.id) AS forecast_count
FROM challenges.rounds cr
LEFT JOIN challenges.definitions cd ON cr.definition_id = cd.id
LEFT JOIN challenges.series_pseudo csp ON csp.round_id = cr.id
LEFT JOIN data_portal.time_series ts ON ts.series_id = csp.series_id
LEFT JOIN data_portal.domain_category dc ON ts.domain_category_id = dc.id
GROUP BY 
    cr.id, cr.name, cr.description, cr.definition_id, cd.schedule_id, cd.name, cd.domains, cd.subdomains, cd.categories, cd.subcategories,
    cr.registration_start, cr.registration_end, cr.start_time, cr.end_time, 
    cr.context_length, cr.horizon, cr.frequency, cr.status, cr.created_at, cr.updated_at;

COMMENT ON VIEW challenges.v_rounds_with_metadata IS 
'Challenge rounds with aggregated metadata for participant-facing filtering.
Includes definition info, round timing, and aggregated domain/category data from time series.';

-- ==========================================================
-- 7) View: Data Availability Check
-- ==========================================================
CREATE OR REPLACE VIEW data_portal.v_data_availability AS
SELECT 
    ts.series_id,
    ts.name,
    ts.frequency,
    ts.unique_id,
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
GROUP BY ts.series_id, ts.name, ts.frequency, ts.unique_id, dc.domain, dc.category, dc.subcategory;

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
    cs.round_id,
    cs.model_id,
    cs.series_id,
    cs.mase,
    cs.rmse,
    cs.final_evaluation,
    cs.calculated_at,
    -- Round Info
    cr.name AS round_name,
    cr.horizon,
    cr.end_time AS round_end_time,
    cr.start_time AS round_start_time,
    cr.definition_id,
    cd.name AS definition_name,
    cd.domains AS definition_domains,
    -- Model Info
    mi.name AS model_name,
    u.username,
    -- Time Series Info
    ts.name AS series_name,
    ts.frequency,
    ts.unique_id,
    -- Domain Info
    dc.domain,
    dc.category,
    dc.subcategory
FROM forecasts.scores cs
JOIN challenges.rounds cr ON cr.id = cs.round_id
LEFT JOIN challenges.definitions cd ON cr.definition_id = cd.id
JOIN models.model_info mi ON mi.id = cs.model_id
JOIN auth.users u ON u.id = mi.user_id
JOIN data_portal.time_series ts ON ts.series_id = cs.series_id
LEFT JOIN data_portal.domain_category dc ON ts.domain_category_id = dc.id
WHERE cs.mase IS NOT NULL
  AND cs.mase != 'NaN'
  AND cs.mase != 'Infinity'
  AND cs.mase != '-Infinity'
  AND cs.final_evaluation;
COMMENT ON VIEW forecasts.v_ranking_base IS 
'Base view for model rankings with all filter dimensions. Filters out invalid MASE values (NULL, NaN, Infinity).';

-- ==========================================================
-- 9) Indexes for Ranking Performance
-- ==========================================================

-- Index for challenge_scores lookup
CREATE INDEX IF NOT EXISTS idx_scores_lookup 
ON forecasts.scores(round_id, model_id, series_id) 
WHERE mase IS NOT NULL;

-- Index for time-based filtering on challenge rounds
CREATE INDEX IF NOT EXISTS idx_rounds_end_time 
ON challenges.rounds(end_time) 
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

-- Index for challenge_series_pseudo
CREATE INDEX IF NOT EXISTS idx_series_pseudo_series
ON challenges.series_pseudo(series_id);

CREATE INDEX idx_forecasts_round_model 
ON forecasts.forecasts(round_id, model_id);

-- Composite index for round + series queries (used by dashboard get_series_forecasts)
CREATE INDEX IF NOT EXISTS idx_forecasts_round_series 
ON forecasts.forecasts(round_id, series_id);

CREATE INDEX IF NOT EXISTS idx_forecasts_series_id ON forecasts.forecasts(series_id);
CREATE INDEX IF NOT EXISTS idx_scores_series_id ON forecasts.scores(series_id);
CREATE INDEX IF NOT EXISTS idx_context_data_series_id ON challenges.context_data(series_id);

-- Composite index for series_pseudo lookups by round + series
CREATE INDEX IF NOT EXISTS idx_series_pseudo_round_series 
ON challenges.series_pseudo(round_id, series_id);


-- ==========================================================
-- 10) Continuous Aggregates for Multi-Granularity Time Series
-- ==========================================================

-- Quarter-hourly aggregation (15 minutes)
-- Contains all series with frequency <= 15 minutes
CREATE MATERIALIZED VIEW IF NOT EXISTS data_portal.time_series_15min
WITH (timescaledb.continuous) AS
SELECT 
    series_id,
    time_bucket('15 minutes', ts) AS ts,
    AVG(value) AS value,
    COUNT(*) AS sample_count,
    MIN(value) AS min_value,
    MAX(value) AS max_value
FROM data_portal.time_series_data
GROUP BY series_id, time_bucket('15 minutes', ts)
WITH NO DATA;

-- Refresh policy: Every 5 minutes, looks back 1 day
SELECT add_continuous_aggregate_policy('data_portal.time_series_15min',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

-- Compression after 14 days
ALTER MATERIALIZED VIEW data_portal.time_series_15min SET (
    timescaledb.compress = true
);
SELECT add_compression_policy('data_portal.time_series_15min', 
    compress_after => INTERVAL '14 days',
    if_not_exists => TRUE);

-- ----------------------------------------------------------
-- Hourly aggregation (1 hour)
-- Contains all series with frequency <= 1 hour
CREATE MATERIALIZED VIEW IF NOT EXISTS data_portal.time_series_1h
WITH (timescaledb.continuous) AS
SELECT 
    series_id,
    time_bucket('1 hour', ts) AS ts,
    AVG(value) AS value,
    COUNT(*) AS sample_count,
    MIN(value) AS min_value,
    MAX(value) AS max_value
FROM data_portal.time_series_data
GROUP BY series_id, time_bucket('1 hour', ts)
WITH NO DATA;

-- Refresh policy: Every 15 minutes, looks back 2 days
SELECT add_continuous_aggregate_policy('data_portal.time_series_1h',
    start_offset => INTERVAL '2 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE
);

-- Compression after 30 days
ALTER MATERIALIZED VIEW data_portal.time_series_1h SET (
    timescaledb.compress = true
);
SELECT add_compression_policy('data_portal.time_series_1h', 
    compress_after => INTERVAL '30 days',
    if_not_exists => TRUE);

-- ----------------------------------------------------------
-- Daily aggregation (1 day)
-- Contains all series with frequency <= 1 day
CREATE MATERIALIZED VIEW IF NOT EXISTS data_portal.time_series_1d
WITH (timescaledb.continuous) AS
SELECT 
    series_id,
    time_bucket('1 day', ts) AS ts,
    AVG(value) AS value,
    COUNT(*) AS sample_count,
    MIN(value) AS min_value,
    MAX(value) AS max_value
FROM data_portal.time_series_data
GROUP BY series_id, time_bucket('1 day', ts)
WITH NO DATA;

-- Refresh policy: Every hour, looks back 7 days
SELECT add_continuous_aggregate_policy('data_portal.time_series_1d',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Compression after 90 days
ALTER MATERIALIZED VIEW data_portal.time_series_1d SET (
    timescaledb.compress = true
);
SELECT add_compression_policy('data_portal.time_series_1d', 
    compress_after => INTERVAL '90 days',
    if_not_exists => TRUE);

COMMENT ON MATERIALIZED VIEW data_portal.time_series_15min IS 
'Continuous aggregate for 15-minute data. Aggregates all time series with frequency <= 15 minutes.';

COMMENT ON MATERIALIZED VIEW data_portal.time_series_1h IS 
'Continuous aggregate for hourly data. Aggregates all time series with frequency <= 1 hour.';

COMMENT ON MATERIALIZED VIEW data_portal.time_series_1d IS 
'Continuous aggregate for daily data. Aggregates all time series with frequency <= 1 day.';

-- ==========================================================
-- 11) ELO Rating System (Bootstrapped)
-- ==========================================================

-- ELO Ratings Table
-- Stores bootstrapped ELO ratings per model, optionally scoped to definition
CREATE TABLE IF NOT EXISTS forecasts.elo_ratings (
    id SERIAL PRIMARY KEY,
    model_id INTEGER NOT NULL REFERENCES models.model_info(id) ON DELETE CASCADE,
    definition_id INTEGER REFERENCES challenges.definitions(id) ON DELETE CASCADE,
    -- NULL = Global ELO across all challenges
    -- NOT NULL = Definition-specific ELO
    
    time_period_days INTEGER,
    -- NULL = All-time (no time filter)
    -- 7, 30, 90, 365 = Last N days based on challenges.rounds.end_time
    
    -- ELO Scores (Median + Confidence Interval from 500 bootstraps)
    elo_score DOUBLE PRECISION NOT NULL,
    elo_ci_lower DOUBLE PRECISION,  -- 2.5% Quantile
    elo_ci_upper DOUBLE PRECISION,  -- 97.5% Quantile
    
    -- Metadata
    n_matches INTEGER DEFAULT 0,        -- Number of series/matches included
    n_bootstraps INTEGER DEFAULT 500,   -- Number of bootstrap iterations
    
    -- Performance tracking
    calculation_duration_ms INTEGER,    -- Duration of calculation in ms
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Unique constraint: one row per model per scope (definition + time period)
-- COALESCE handles NULL values: definition_id=-1 means global, time_period_days=0 means all-time
CREATE UNIQUE INDEX IF NOT EXISTS idx_elo_unique_model_scope 
ON forecasts.elo_ratings(model_id, COALESCE(definition_id, -1), COALESCE(time_period_days, 0));

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_elo_model ON forecasts.elo_ratings(model_id);
CREATE INDEX IF NOT EXISTS idx_elo_definition ON forecasts.elo_ratings(definition_id) WHERE definition_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_elo_time_period ON forecasts.elo_ratings(time_period_days) WHERE time_period_days IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_elo_score_desc ON forecasts.elo_ratings(elo_score DESC);
CREATE INDEX IF NOT EXISTS idx_elo_calculated_at ON forecasts.elo_ratings(calculated_at);



-- Performance index for ELO calculation queries
CREATE INDEX IF NOT EXISTS idx_scores_elo_lookup 
ON forecasts.scores(series_id, model_id, mase) 
WHERE mase IS NOT NULL AND final_evaluation = TRUE;

COMMENT ON TABLE forecasts.elo_ratings IS 
'Bootstrapped ELO ratings for models. Each row represents a model''s ELO score, either global (definition_id IS NULL) or for a specific challenge definition. 
Calculated via 100 bootstrap iterations where each time series is a "match" comparing models by MASE.';

COMMENT ON COLUMN forecasts.elo_ratings.elo_score IS 
'Median ELO rating from N bootstrap iterations. Higher is better. Base rating is 1000.';

COMMENT ON COLUMN forecasts.elo_ratings.elo_ci_lower IS 
'2.5% quantile of ELO ratings from bootstraps (lower bound of 95% CI).';

COMMENT ON COLUMN forecasts.elo_ratings.elo_ci_upper IS 
'97.5% quantile of ELO ratings from bootstraps (upper bound of 95% CI).';

COMMENT ON COLUMN forecasts.elo_ratings.calculation_duration_ms IS 
'Time taken to calculate this ELO rating in milliseconds. Used for performance monitoring.';

-- View: ELO Leaderboard with model and definition info
CREATE OR REPLACE VIEW forecasts.v_elo_leaderboard AS
SELECT 
    er.id as elo_id,
    er.elo_score,
    er.elo_ci_lower,
    er.elo_ci_upper,
    er.n_matches,
    er.n_bootstraps,
    er.calculation_duration_ms,
    er.calculated_at,
    -- Model info
    mi.id as model_id,
    mi.name as model_name,
    mi.readable_id,
    mi.model_family,
    mi.model_type,
    -- User info
    u.username,
    o.name as organization_name,
    -- Definition info (NULL for global ELO)
    er.definition_id,
    cd.name as definition_name,
    cd.schedule_id as definition_schedule_id,
    -- Rank within scope
    ROW_NUMBER() OVER (
        PARTITION BY er.definition_id 
        ORDER BY er.elo_score DESC
    ) as rank
FROM forecasts.elo_ratings er
JOIN models.model_info mi ON er.model_id = mi.id
JOIN auth.users u ON mi.user_id = u.id
LEFT JOIN auth.organizations o ON mi.organization_id = o.id
LEFT JOIN challenges.definitions cd ON er.definition_id = cd.id
ORDER BY er.definition_id NULLS FIRST, er.elo_score DESC;

COMMENT ON VIEW forecasts.v_elo_leaderboard IS 
'Leaderboard view combining ELO ratings with model and definition metadata. 
Includes rank within each scope (global or per-definition).';

-- ==========================================================
-- Final message
-- ==========================================================
DO $$
BEGIN
  RAISE NOTICE '=== Data Portal schema initialized successfully ===';
END $$;