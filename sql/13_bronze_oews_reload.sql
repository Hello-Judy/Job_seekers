-- ============================================================
-- 13: Rebuild BRONZE.raw_bls_oews with a single VARIANT column.
-- ============================================================
--
-- Old design (file 07): one typed column per OEWS field, plus a `raw_record`
-- VARIANT for replay. That gave us positional COPY against fixed `$1..$N`.
-- BLS broke positional ordering with the May-2024 layout, so we walk away
-- from positional entirely.
--
-- New design: one VARIANT column `record` that holds the entire row keyed by
-- header name. Silver-layer SQL then extracts the fields it needs by KEY
-- (record:OCC_CODE::STRING, record:A_MEDIAN::FLOAT, etc.). Robust to any
-- future column reordering or addition by BLS.
--
-- The bookkeeping columns (survey_year, source_file, ingestion_timestamp)
-- stay typed because the loader sets them at COPY time, not from the CSV.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA BRONZE;

-- Drop the misshapen Bronze table and rebuild. We don't carry data forward
-- because every row currently sitting in raw_bls_oews has fields shifted
-- one column to the right.
DROP TABLE IF EXISTS raw_bls_oews;

CREATE TABLE raw_bls_oews (
    record               VARIANT,                       -- entire OEWS row, keyed by header
    survey_year          INT,
    source_file          VARCHAR,
    ingestion_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE raw_bls_oews CLUSTER BY (survey_year);

DESCRIBE TABLE raw_bls_oews;
