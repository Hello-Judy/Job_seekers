-- ============================================================
-- 14: JSON file format for OEWS NDJSON loading.
-- ============================================================
--
-- This replaces the CSV-based approach in 12_fix_oews_file_format.sql.
-- After running this file, the OEWS pipeline will load NDJSON instead of
-- CSV — each row arrives as a VARIANT keyed by header names, completely
-- decoupling our schema from BLS's column ordering decisions.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT ff_json_bls_oews
    TYPE                          = JSON
    -- Snowflake reads NDJSON natively when STRIP_OUTER_ARRAY is FALSE and
    -- the file is one JSON object per line.
    STRIP_OUTER_ARRAY             = FALSE
    COMPRESSION                   = AUTO;

SHOW FILE FORMATS LIKE 'ff_json_bls_oews' IN SCHEMA BRONZE;
