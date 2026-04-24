-- ============================================================================
-- 07_historical_backfill.sql
-- Bronze table DDL for the USAJobs historical dataset backfill.
-- Run once before triggering the historical_backfill_dag DAG.
-- ============================================================================

USE DATABASE JUDYDREAM;
USE SCHEMA BRONZE;

-- Bronze landing table for abigailhaddad/usajobs_historical parquet data.
-- raw_data stores the full normalised row as JSON VARIANT so the schema
-- stays flexible across years (column names shift between parquet releases).
CREATE TABLE IF NOT EXISTS BRONZE.raw_usajobs_historical (
    position_id           VARCHAR         NOT NULL,
    source_year           INTEGER,
    raw_data              VARIANT         NOT NULL,
    ingestion_timestamp   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_raw_usajobs_historical PRIMARY KEY (position_id)
);

-- Index on source_year helps the Silver MERGE filter by year if needed.
CREATE INDEX IF NOT EXISTS idx_hist_year
    ON BRONZE.raw_usajobs_historical (source_year);

-- ── verification queries ──────────────────────────────────────────────────────

-- Check row counts per year after backfill:
-- SELECT source_year, COUNT(*) AS rows
-- FROM BRONZE.raw_usajobs_historical
-- GROUP BY 1 ORDER BY 1;

-- Spot-check a few rows:
-- SELECT position_id, source_year,
--        raw_data:job_title::STRING      AS job_title,
--        raw_data:posted_date::STRING    AS posted_date,
--        raw_data:state::STRING          AS state
-- FROM BRONZE.raw_usajobs_historical
-- LIMIT 10;
