-- ============================================================
-- 02: Bronze layer — raw data, minimally processed.
-- ============================================================
--
-- Design principle: ingest full original payload as VARIANT (JSON).
-- This lets us re-derive Silver/Gold without re-hitting the API
-- if our transformation logic changes.
--
-- VARIANT columns in Snowflake can store arbitrary nested JSON
-- and are queryable with standard SQL path syntax:
--     SELECT raw_data:title::string FROM bronze.raw_adzuna;
--
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA BRONZE;

-- ---------- Adzuna (private sector, real-time) ----------
CREATE TABLE IF NOT EXISTS raw_adzuna (
    job_id              VARCHAR,
    raw_data            VARIANT,
    search_query        VARCHAR,      -- what keyword we searched for
    search_country      VARCHAR,      -- us / gb / ...
    api_pull_date       DATE,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------- USAJobs Current (federal, real-time) ----------
CREATE TABLE IF NOT EXISTS raw_usajobs_current (
    control_number      VARCHAR,
    raw_data            VARIANT,
    api_pull_date       DATE,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------- USAJobs Historical (federal, backfill) ----------
-- Loaded from the parquet files published by abigailhaddad/usajobs_historical.
-- Columns here mirror the rationalized schema in that dataset so we can
-- COPY straight from staged parquet files.
CREATE TABLE IF NOT EXISTS raw_usajobs_historical (
    control_number      VARCHAR,
    raw_data            VARIANT,       -- entire row as JSON for flexibility
    source_year         INT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Sanity check
SHOW TABLES IN SCHEMA BRONZE;
