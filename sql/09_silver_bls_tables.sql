-- ============================================================
-- 09: Silver layer additions — BLS datasets.
-- ============================================================
--
-- Two new Silver tables, plus one mapping table that lives in Silver because
-- it is used to enrich both jobs_unified and the BLS facts:
--
--   silver.bls_qcew_quarterly   typed, deduped, with surrogate key
--   silver.bls_oews_annual      flattened OEWS, suppression markers parsed
--   silver.dim_occupation_soc   the SOC resolver lookup table
--
-- Why a Silver "dim" instead of putting it directly in Gold?
--   The SOC mapping is reused by *three* downstream tables:
--     1. jobs_unified rows, to join to OEWS wages
--     2. fact_job_postings (Gold), as the SOC dimension key
--     3. fact_oews_wages (Gold), as the SOC dimension key
--   Putting it in Silver lets us conform on it without circular dependencies.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA SILVER;


-- ---------- bls_qcew_quarterly ----------
-- One row per (area_fips, industry_code, own_code, year, qtr).
-- Bronze can have duplicates if a singlefile is re-COPY'd manually outside
-- Snowflake's load-history window; the Silver MERGE collapses them by max
-- ingestion_timestamp so the most recent wins.
CREATE TABLE IF NOT EXISTS bls_qcew_quarterly (
    qcew_key            VARCHAR PRIMARY KEY,           -- area||ind||own||year||qtr
    area_fips           VARCHAR,
    area_type           VARCHAR,                       -- 'national' | 'state' | 'msa' | 'county' | 'csa' | 'other'
    state_fips          VARCHAR,                       -- first 2 chars of area_fips when relevant
    industry_code       VARCHAR,
    industry_level      INT,                           -- 2..6 for NAICS, 0 for special aggregates
    own_code            INT,
    own_label           VARCHAR,                       -- 'Total' | 'Federal' | 'State' | 'Local' | 'Private'
    year                INT,
    qtr                 INT,

    qtrly_estabs        INT,
    avg_monthly_emplvl  FLOAT,                         -- mean of month1/2/3
    total_qtrly_wages   FLOAT,
    avg_wkly_wage       FLOAT,

    ingestion_timestamp TIMESTAMP_NTZ
);

ALTER TABLE bls_qcew_quarterly CLUSTER BY (year, qtr, state_fips);


-- ---------- bls_oews_annual ----------
-- One row per (survey_year, area, occ_code). Keeps both hourly and annual
-- wage percentiles so the dashboard can render either.
CREATE TABLE IF NOT EXISTS bls_oews_annual (
    oews_key            VARCHAR PRIMARY KEY,           -- year||area||occ_code
    survey_year         INT,
    area                VARCHAR,
    area_title          VARCHAR,
    area_type           INT,                           -- 1=US, 2=state, 4=MSA
    occ_code            VARCHAR,                       -- SOC 6-digit, '15-1252'
    occ_title           VARCHAR,
    o_group             VARCHAR,                       -- 'major' | 'minor' | 'broad' | 'detailed'

    tot_emp             FLOAT,
    a_mean              FLOAT,                         -- annual mean wage
    a_pct10             FLOAT,
    a_pct25             FLOAT,
    a_median            FLOAT,
    a_pct75             FLOAT,
    a_pct90             FLOAT,
    h_mean              FLOAT,                         -- hourly mean wage
    h_median            FLOAT,

    ingestion_timestamp TIMESTAMP_NTZ
);

ALTER TABLE bls_oews_annual CLUSTER BY (survey_year, occ_code);


-- ---------- dim_occupation_soc ----------
-- The SOC resolver writes here. Each row links one normalized job-title
-- string to its best SOC match plus the confidence score that produced it.
--
-- match_method:
--    'exact'   - title matched a BLS Direct Match Title File entry
--    'fuzzy'   - rapidfuzz token_sort_ratio >= confidence_threshold
--    'manual'  - a human override (we don't seed any here; left for paper)
--    'unmapped'- nothing matched; soc_code is '99-9999'
CREATE TABLE IF NOT EXISTS dim_occupation_soc (
    soc_key             INT AUTOINCREMENT PRIMARY KEY,
    soc_code            VARCHAR,                       -- '15-1252' or '99-9999' (sentinel)
    soc_title           VARCHAR,
    soc_major_group     VARCHAR,                       -- '15-0000' = Computer & Mathematical
    soc_major_title     VARCHAR,                       -- 'Computer and Mathematical Occupations'
    title_normalized    VARCHAR,                       -- lowercased, punctuation stripped, the key for joins
    title_raw           VARCHAR,                       -- one example raw title that resolved here
    match_method        VARCHAR,
    match_confidence    FLOAT,                         -- 1.0 for exact; 0..1 for fuzzy
    posting_count       INT,                           -- how many SILVER.jobs_unified rows resolved here
    last_resolved_at    TIMESTAMP_NTZ
);

-- Snowflake uses automatic micro-partitioning instead of B-tree indexes.
-- Cluster on title_normalized so the resolver's UPDATE-by-title queries
-- prune partitions efficiently.
ALTER TABLE dim_occupation_soc CLUSTER BY (title_normalized);


SHOW TABLES IN SCHEMA SILVER;
