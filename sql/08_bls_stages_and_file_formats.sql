-- ============================================================
-- 08: Stages and file formats for BLS bulk loads.
-- ============================================================
--
-- Why not stream INSERTs row-by-row?
--   QCEW singlefiles are 1–2 GB each and contain ~10–15 M rows. Loading them
--   via INSERT ... PARSE_JSON would take hours and burn warehouse credits.
--   PUT + COPY INTO is the Snowflake-native pattern: one TCP upload of the
--   compressed file, then a single bulk-parallel load on the server side.
--
-- Stage type:
--   We use an internal user stage. No external S3 needed for a coursework
--   project — keeps credentials simple and the data inside the Snowflake
--   security perimeter.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA BRONZE;

-- ---------- File formats ----------
CREATE OR REPLACE FILE FORMAT ff_csv_bls_qcew
    TYPE              = CSV
    FIELD_DELIMITER   = ','
    PARSE_HEADER      = FALSE
    SKIP_HEADER       = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF           = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION       = AUTO              -- accepts both .csv and .csv.gz
    TRIM_SPACE        = TRUE;

CREATE OR REPLACE FILE FORMAT ff_csv_bls_oews
    TYPE              = CSV
    FIELD_DELIMITER   = ','
    SKIP_HEADER       = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF           = ('', '*', '**', '#', 'NULL')   -- OEWS uses suppression markers
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE        = TRUE
    COMPRESSION       = AUTO;

-- ---------- Internal stages ----------
-- One per dataset so we can clean them independently after each load.
CREATE STAGE IF NOT EXISTS stg_bls_qcew
    FILE_FORMAT = ff_csv_bls_qcew
    COMMENT = 'Drop QCEW singlefile CSVs here for bulk loading';

CREATE STAGE IF NOT EXISTS stg_bls_oews
    FILE_FORMAT = ff_csv_bls_oews
    COMMENT = 'Drop OEWS area/national/MSA CSVs here for bulk loading';

SHOW STAGES IN SCHEMA BRONZE;
SHOW FILE FORMATS IN SCHEMA BRONZE;
