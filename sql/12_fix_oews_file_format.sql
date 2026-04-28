-- ============================================================
-- 12: Fix OEWS bronze loading — column-name-based, not positional.
-- ============================================================
--
-- Why this file exists
-- --------------------
-- The original `08_bls_stages_and_file_formats.sql` configured the OEWS file
-- format for positional CSV parsing, and `bls_loader.py` used `$1, $2, ...`
-- column references. That was fine for the pre-2024 OEWS workbooks but BLS
-- shipped May-2024 OEWS as one consolidated `all_data_M_2024.xlsx` whose
-- column order is different from the legacy per-area workbooks.
-- The result: every typed column in BRONZE.raw_bls_oews held data that
-- belonged to a different field.
--
-- The fix
-- -------
-- Stop relying on column position. Configure the OEWS file format with
-- `PARSE_HEADER = TRUE`, then COPY rows in as a VARIANT keyed by the actual
-- header names. Whatever order BLS publishes columns in, the keys land in
-- the right slots.
--
-- Run this AFTER the table-level fix in 13_bronze_oews_reload.sql.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA BRONZE;

-- A single new file format that parses headers from row 1 of the CSV. With
-- this in place, COPY ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE will route
-- each header column into the matching VARIANT key.
CREATE OR REPLACE FILE FORMAT ff_csv_bls_oews_named
    TYPE                          = CSV
    FIELD_DELIMITER               = ','
    PARSE_HEADER                  = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
    NULL_IF                       = ('', '*', '**', '#', 'NULL')
    EMPTY_FIELD_AS_NULL           = TRUE
    TRIM_SPACE                    = TRUE
    COMPRESSION                   = AUTO;

SHOW FILE FORMATS IN SCHEMA BRONZE;
