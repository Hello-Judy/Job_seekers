-- ============================================================
-- 07: Bronze layer additions — BLS datasets.
-- ============================================================
--
-- This file adds three new Bronze tables to back the BLS integration:
--
--   raw_bls_qcew         Quarterly Census of Employment & Wages
--                        (NAICS 6-digit × county FIPS × quarter, 1990–present)
--                        ~50 GB uncompressed for the full historical range.
--
--   raw_bls_oews         Occupational Employment & Wage Statistics
--                        (SOC 6-digit × MSA/state × annual, 2019–present)
--                        Provides the wage ground truth our Adzuna / USAJobs
--                        postings get joined against.
--
--   raw_bls_jolts        Job Openings & Labor Turnover Survey
--                        (state × industry × monthly, 2001–present)
--                        Used for hiring-trend and labor-tightness panels.
--
-- Design notes
-- ------------
-- The QCEW row count is too large to land via PARSE_JSON one-row-per-INSERT.
-- We COPY INTO the table from staged CSV files, but still keep one VARIANT
-- column (raw_record) so we can replay any row with full fidelity if our
-- silver transformations change. This mirrors the lakehouse principle the
-- rest of the project follows: Bronze is faithful, Silver is opinionated.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA BRONZE;

-- ---------- QCEW: NAICS-based quarterly singlefile ----------
-- File layout: https://data.bls.gov/cew/doc/layouts/csv_quarterly_layout.htm
-- We type the most-queried columns natively and keep the rest in raw_record.
CREATE TABLE IF NOT EXISTS raw_bls_qcew (
    area_fips           VARCHAR,        -- 5-char county FIPS, 'US000', 'CSAxxx', etc.
    own_code            INT,            -- 0=Total, 1=Federal, 5=Private, ...
    industry_code       VARCHAR,        -- NAICS code, '10' = total all industries
    agglvl_code         INT,            -- aggregation level (county/MSA/state/nation)
    size_code           INT,            -- establishment size class (always 0 in singlefile)
    year                INT,
    qtr                 INT,            -- 1..4
    disclosure_code     VARCHAR,        -- 'N' = not disclosable

    qtrly_estabs        INT,
    month1_emplvl       INT,
    month2_emplvl       INT,
    month3_emplvl       INT,
    total_qtrly_wages   FLOAT,
    taxable_qtrly_wages FLOAT,
    qtrly_contributions FLOAT,
    avg_wkly_wage       FLOAT,

    -- Original record retained verbatim for re-derivation.
    raw_record          VARIANT,

    source_file         VARCHAR,        -- e.g. '2024_qtrly_singlefile.csv'
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE raw_bls_qcew CLUSTER BY (year, qtr, area_fips);


-- ---------- OEWS: occupational wages by area ----------
-- We land it semi-structured because the wage columns have suppression markers
-- ('*', '#', '**') that break typed loads. The Silver step interprets them.
CREATE TABLE IF NOT EXISTS raw_bls_oews (
    survey_year         INT,            -- e.g. 2024 for the May-2024 panel
    area                VARCHAR,        -- BLS area code
    area_title          VARCHAR,
    area_type           INT,            -- 1=US, 2=state, 4=MSA, ...
    occ_code            VARCHAR,        -- SOC 6-digit, e.g. '15-1252'
    occ_title           VARCHAR,
    o_group             VARCHAR,        -- 'major' | 'minor' | 'broad' | 'detailed'
    naics               VARCHAR,
    naics_title         VARCHAR,

    raw_record          VARIANT,        -- entire OEWS row, suppression flags intact
    source_file         VARCHAR,        -- 'oesm24nat.xlsx', 'MSA_M2024_dl.xlsx', ...
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE raw_bls_oews CLUSTER BY (survey_year, area_type);


-- ---------- JOLTS: openings/hires/quits ----------
-- Pulled via the BLS Public Data API which returns JSON, so VARIANT is the
-- natural landing format here. One row per series_id × period.
CREATE TABLE IF NOT EXISTS raw_bls_jolts (
    series_id           VARCHAR,        -- e.g. 'JTS000000000000000JOL'
    year                INT,
    period              VARCHAR,        -- 'M01'..'M12'
    period_name         VARCHAR,        -- 'January'..'December'
    value               FLOAT,
    footnotes           VARCHAR,
    raw_record          VARIANT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

ALTER TABLE raw_bls_jolts CLUSTER BY (year, series_id);


SHOW TABLES IN SCHEMA BRONZE;
