-- ============================================================
-- 06: Schema updates + test-data cleanup
-- ============================================================
-- Run this script ONCE before the first production pipeline run.
--
-- SECTION 1  Test data cleanup  — removes rows loaded during dev/testing
-- SECTION 2  Add apply_url     — new column in Silver and Gold tables
-- ============================================================

USE DATABASE JOBSEEKERS;


-- ============================================================
-- SECTION 1: Test-data cleanup
-- ============================================================
-- Option A (recommended): delete only rows ingested before a cutoff date.
--   Set :cutoff_date to the date you consider "production start".
-- Option B: full truncate — wipes everything and starts clean.
-- Run ONE of these options, never both.
-- ============================================================

-- ---------- Option A: targeted delete by ingestion date ----------
-- Adjust the date literal to match your actual production start date.

USE SCHEMA BRONZE;
DELETE FROM raw_adzuna          WHERE ingestion_timestamp < '2026-04-23 00:00:00';
DELETE FROM raw_usajobs_current WHERE ingestion_timestamp < '2026-04-23 00:00:00';

USE SCHEMA SILVER;
DELETE FROM jobs_unified        WHERE ingestion_timestamp < '2026-04-23 00:00:00';

USE SCHEMA GOLD;
-- Gold is fully derived; truncate and let silver_to_gold DAG repopulate.
TRUNCATE TABLE fact_job_postings;
TRUNCATE TABLE dim_location;
TRUNCATE TABLE dim_company;
TRUNCATE TABLE dim_category;
-- dim_date is a static calendar spine — do NOT truncate it.

-- ---------- Option B: full truncate (clean slate) ----------
-- Uncomment only if you want to wipe all data and restart from scratch.
-- USE SCHEMA BRONZE;
-- TRUNCATE TABLE raw_adzuna;
-- TRUNCATE TABLE raw_usajobs_current;
-- TRUNCATE TABLE raw_usajobs_historical;
-- USE SCHEMA SILVER;
-- TRUNCATE TABLE jobs_unified;
-- USE SCHEMA GOLD;
-- TRUNCATE TABLE fact_job_postings;
-- TRUNCATE TABLE dim_location;
-- TRUNCATE TABLE dim_company;
-- TRUNCATE TABLE dim_category;


-- ============================================================
-- SECTION 2: Add apply_url column
-- ============================================================
-- Both Adzuna (redirect_url) and USAJobs (ApplyURI[0]) return application
-- URLs in their API responses. The Bronze VARIANT already stores them;
-- we just need the column to surface them in Silver and Gold.
-- ============================================================

USE SCHEMA SILVER;
ALTER TABLE jobs_unified
    ADD COLUMN IF NOT EXISTS apply_url VARCHAR;

USE SCHEMA GOLD;
ALTER TABLE fact_job_postings
    ADD COLUMN IF NOT EXISTS apply_url VARCHAR;

-- Verify
SHOW COLUMNS IN TABLE SILVER.jobs_unified;
SHOW COLUMNS IN TABLE GOLD.fact_job_postings;
