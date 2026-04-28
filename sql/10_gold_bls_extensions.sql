-- ============================================================
-- 10: Gold layer extensions for BLS analytics.
-- ============================================================
--
-- Adds:
--   dim_occupation     conformed SOC dimension (sourced from Silver resolver)
--   fact_qcew          QCEW employment fact (county/state/national × NAICS × quarter)
--   fact_oews_wages    OEWS wage fact (area × SOC × annual)
--
-- And amends the existing fact_job_postings:
--   adds soc_key FK so we can join postings to OEWS at query time
--
-- After this, the star schema has FOUR fact tables sharing TWO conformed
-- dimensions (dim_date, dim_location) plus the new dim_occupation. That's
-- the canonical "Kimball constellation" layout — worth highlighting in the
-- methodology section of the paper.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA GOLD;


-- ---------- dim_occupation (conformed) ----------
-- Mirrors silver.dim_occupation_soc, but with the soc_key surrogate stable
-- across refreshes. The MERGE in silver_to_gold_bls.py preserves keys.
CREATE TABLE IF NOT EXISTS dim_occupation (
    occupation_key      INT AUTOINCREMENT PRIMARY KEY,
    soc_code            VARCHAR,
    soc_title           VARCHAR,
    soc_major_group     VARCHAR,
    soc_major_title     VARCHAR
);


-- ---------- fact_qcew ----------
-- Grain: one row per (area, industry, ownership, year, quarter).
-- Foreign keys to dim_date (year-quarter to first day of quarter), dim_location.
CREATE TABLE IF NOT EXISTS fact_qcew (
    qcew_key            VARCHAR PRIMARY KEY,           -- carried over from Silver
    date_key            INT,                           -- FK: first day of quarter
    location_key        INT,                           -- FK: dim_location

    area_fips           VARCHAR,
    area_type           VARCHAR,
    industry_code       VARCHAR,
    industry_level      INT,
    own_code            INT,
    own_label           VARCHAR,

    qtrly_estabs        INT,
    avg_monthly_emplvl  FLOAT,
    total_qtrly_wages   FLOAT,
    avg_wkly_wage       FLOAT,

    year                INT,
    qtr                 INT
);

ALTER TABLE fact_qcew CLUSTER BY (year, qtr, area_type);


-- ---------- fact_oews_wages ----------
-- Grain: one row per (survey_year, area, occ_code).
CREATE TABLE IF NOT EXISTS fact_oews_wages (
    oews_key            VARCHAR PRIMARY KEY,
    date_key            INT,                           -- FK: May 1 of survey_year
    occupation_key      INT,                           -- FK: dim_occupation
    location_key        INT,                           -- FK: dim_location (nullable for nationwide)

    survey_year         INT,
    area                VARCHAR,
    area_title          VARCHAR,
    area_type           INT,
    occ_code            VARCHAR,
    occ_title           VARCHAR,

    tot_emp             FLOAT,
    a_mean              FLOAT,
    a_pct10             FLOAT,
    a_pct25             FLOAT,
    a_median            FLOAT,
    a_pct75             FLOAT,
    a_pct90             FLOAT,
    h_mean              FLOAT,
    h_median            FLOAT
);

ALTER TABLE fact_oews_wages CLUSTER BY (survey_year, occ_code);


-- ---------- amend fact_job_postings: add soc_key FK ----------
-- IF NOT EXISTS prevents this from breaking re-runs of this file.
ALTER TABLE fact_job_postings ADD COLUMN IF NOT EXISTS occupation_key INT;


SHOW TABLES IN SCHEMA GOLD;
