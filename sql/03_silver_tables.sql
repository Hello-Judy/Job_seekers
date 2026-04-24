-- ============================================================
-- 03: Silver layer — cleaned, unified, deduplicated.
-- ============================================================
--
-- All three sources are mapped to this single schema.
-- Downstream Gold-layer queries only need to reason about ONE table.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA SILVER;

CREATE TABLE IF NOT EXISTS jobs_unified (
    -- Identity
    job_id                  VARCHAR,     -- prefixed: 'adzuna_xxx' / 'usajobs_xxx'
    source                  VARCHAR,     -- 'adzuna' | 'usajobs_current' | 'usajobs_historical'
    sector                  VARCHAR,     -- 'federal' | 'private'

    -- Core content
    job_title               VARCHAR,
    job_title_normalized    VARCHAR,     -- lowercased, punctuation stripped
    company_or_agency       VARCHAR,
    job_description         VARCHAR,

    -- Geography
    location_city           VARCHAR,
    location_state          VARCHAR,
    location_country        VARCHAR,
    latitude                FLOAT,
    longitude               FLOAT,

    -- Salary (annualized, USD)
    salary_min              FLOAT,
    salary_max              FLOAT,
    salary_is_predicted     BOOLEAN,

    -- Classification
    job_category            VARCHAR,
    experience_level        VARCHAR,     -- 'entry' | 'mid' | 'senior' | 'unknown'
    employment_type         VARCHAR,     -- 'full_time' | 'part_time' | 'intern' | 'unknown'
    remote_type             VARCHAR,     -- 'remote' | 'hybrid' | 'onsite' | 'unknown'
    is_entry_level          BOOLEAN,

    -- Dates
    posted_date             DATE,
    close_date              DATE,

    -- Audit
    ingestion_timestamp     TIMESTAMP_NTZ,

    -- Natural primary key: source + job_id is unique
    CONSTRAINT pk_jobs_unified PRIMARY KEY (source, job_id)
);

-- Index-like clustering to speed up typical queries
-- (Snowflake uses automatic micro-partitioning, but clustering keys help on
-- very large tables.)
ALTER TABLE jobs_unified CLUSTER BY (sector, posted_date);

SHOW TABLES IN SCHEMA SILVER;
