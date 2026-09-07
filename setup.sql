-- ============================================================
-- setup.sql
-- Snowflake schema setup for the Health Data Observability Platform
-- Run once manually (or via a setup task) before the pipeline runs
-- ============================================================

-- Database and schemas
CREATE DATABASE IF NOT EXISTS HEALTH_OBSERVABILITY;

USE DATABASE HEALTH_OBSERVABILITY;

CREATE SCHEMA IF NOT EXISTS RAW;          -- immutable, append-only snapshots
CREATE SCHEMA IF NOT EXISTS STAGING;      -- dbt staging models land here
CREATE SCHEMA IF NOT EXISTS MARTS;        -- dbt fact/dim tables land here
CREATE SCHEMA IF NOT EXISTS OBSERVABILITY; -- pipeline logs, run summaries, freshness checks

-- ============================================================
-- RAW LAYER — immutable, one row per (source, snapshot_date, record)
-- Never overwritten. Enables "as-reported" history and revision tracking.
-- ============================================================

CREATE TABLE IF NOT EXISTS RAW.COVID_CASES_RAW (
    run_id            STRING NOT NULL,        -- unique per pipeline run
    snapshot_date     DATE NOT NULL,           -- the reporting week this data represents
    ingested_at       TIMESTAMP_NTZ NOT NULL,  -- when we pulled it (may differ from snapshot_date due to lag)
    state             STRING,
    fips_code         STRING,
    new_cases         INTEGER,
    new_deaths        INTEGER,
    total_cases       INTEGER,
    total_deaths      INTEGER,
    raw_payload       VARIANT,                 -- full original JSON record, for auditability
    PRIMARY KEY (run_id, snapshot_date, state)
);

CREATE TABLE IF NOT EXISTS RAW.FLU_SURVEILLANCE_RAW (
    run_id            STRING NOT NULL,
    snapshot_date     DATE NOT NULL,
    ingested_at       TIMESTAMP_NTZ NOT NULL,
    state             STRING,
    fips_code         STRING,
    ili_activity_level STRING,                 -- CDC's flu activity index
    percent_ili       FLOAT,
    raw_payload       VARIANT,
    PRIMARY KEY (run_id, snapshot_date, state)
);

-- ============================================================
-- OBSERVABILITY LAYER — pipeline health, independent of business data
-- ============================================================

CREATE TABLE IF NOT EXISTS OBSERVABILITY.PIPELINE_LOGS (
    log_id            STRING DEFAULT UUID_STRING(),
    run_id            STRING NOT NULL,
    pipeline_name     STRING NOT NULL,
    stage             STRING NOT NULL,          -- e.g. 'extract', 'validate', 'load'
    status            STRING NOT NULL,          -- 'success', 'failure', 'warning'
    row_count_in      INTEGER,
    row_count_out     INTEGER,
    duration_ms       INTEGER,
    error_message     STRING,
    logged_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (log_id)
);

CREATE TABLE IF NOT EXISTS OBSERVABILITY.ANOMALY_FLAGS (
    flag_id           STRING DEFAULT UUID_STRING(),
    run_id            STRING NOT NULL,
    source            STRING NOT NULL,          -- 'covid' or 'flu'
    state             STRING,
    metric_name       STRING,                   -- e.g. 'new_cases'
    observed_value    FLOAT,
    expected_range_low  FLOAT,
    expected_range_high FLOAT,
    z_score           FLOAT,
    flagged_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (flag_id)
);

-- ============================================================
-- Warehouse (adjust size as needed — XS is plenty for this project's volume)
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS HEALTH_OBS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;