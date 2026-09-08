-- ============================================================
-- setup.sql
-- Snowflake schema setup for the Health Data Observability Platform
-- ============================================================

-- Database and schemas
CREATE DATABASE IF NOT EXISTS HEALTH_OBSERVABILITY;

USE DATABASE HEALTH_OBSERVABILITY;

CREATE SCHEMA IF NOT EXISTS RAW;          
CREATE SCHEMA IF NOT EXISTS STAGING;      -- dbt staging models land here
CREATE SCHEMA IF NOT EXISTS MARTS;        -- dbt fact/dim tables land here
CREATE SCHEMA IF NOT EXISTS OBSERVABILITY; -- pipeline logs, run summaries, freshness checks


CREATE TABLE IF NOT EXISTS RAW.COVID_DEATHS_RAW (
    run_id                          STRING NOT NULL,
    snapshot_date                   DATE NOT NULL,           
    ingested_at                     TIMESTAMP_NTZ NOT NULL,
    state                           STRING,
    covid_19_deaths                 INTEGER,
    total_deaths                    INTEGER,
    pneumonia_deaths                INTEGER,
    pneumonia_and_covid_19_deaths   INTEGER,
    influenza_deaths                INTEGER,
    raw_payload                     VARIANT,
    PRIMARY KEY (run_id, snapshot_date, state)
);

CREATE TABLE IF NOT EXISTS RAW.FLU_SURVEILLANCE_RAW (
    run_id            STRING NOT NULL,
    snapshot_date     DATE NOT NULL,
    ingested_at       TIMESTAMP_NTZ NOT NULL,
    state             STRING,
    fips_code         STRING,
    ili_activity_level STRING,                 
    percent_ili       FLOAT,
    raw_payload       VARIANT,
    PRIMARY KEY (run_id, snapshot_date, state)
);


CREATE TABLE IF NOT EXISTS OBSERVABILITY.PIPELINE_LOGS (
    log_id            STRING DEFAULT UUID_STRING(),
    run_id            STRING NOT NULL,
    pipeline_name     STRING NOT NULL,
    stage             STRING NOT NULL,          
    status            STRING NOT NULL,          
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
    source            STRING NOT NULL,          
    state             STRING,
    metric_name       STRING,                 
    observed_value    FLOAT,
    expected_range_low  FLOAT,
    expected_range_high FLOAT,
    z_score           FLOAT,
    flagged_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (flag_id)
);

CREATE WAREHOUSE IF NOT EXISTS HEALTH_OBS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;