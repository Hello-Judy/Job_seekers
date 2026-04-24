-- ============================================================
-- 04: Gold layer — star schema for analytics.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA GOLD;

-- ---------- Dimension: date ----------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT          PRIMARY KEY,   -- YYYYMMDD
    full_date       DATE,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      VARCHAR,
    day_of_month    INT,
    day_of_week     INT,
    day_name        VARCHAR,
    is_weekend      BOOLEAN
);

-- ---------- Dimension: location ----------
CREATE TABLE IF NOT EXISTS dim_location (
    location_key    INT          AUTOINCREMENT PRIMARY KEY,
    city            VARCHAR,
    state           VARCHAR,
    country         VARCHAR,
    region          VARCHAR,        -- Northeast / Midwest / South / West
    latitude        FLOAT,
    longitude       FLOAT
);

-- ---------- Dimension: company / agency ----------
CREATE TABLE IF NOT EXISTS dim_company (
    company_key     INT          AUTOINCREMENT PRIMARY KEY,
    company_name    VARCHAR,
    sector          VARCHAR,        -- federal / private
    agency_parent   VARCHAR         -- for federal: VA, DOD, etc.
);

-- ---------- Dimension: category ----------
CREATE TABLE IF NOT EXISTS dim_category (
    category_key    INT          AUTOINCREMENT PRIMARY KEY,
    category_name   VARCHAR,
    category_group  VARCHAR         -- Tech / Admin / Healthcare / ...
);

-- ---------- Fact table ----------
CREATE TABLE IF NOT EXISTS fact_job_postings (
    posting_key         VARCHAR      PRIMARY KEY,  -- source||'::'||job_id
    date_key            INT,                        -- FK
    location_key        INT,                        -- FK
    company_key         INT,                        -- FK
    category_key        INT,                        -- FK

    source              VARCHAR,
    sector              VARCHAR,
    experience_level    VARCHAR,
    is_entry_level      BOOLEAN,
    employment_type     VARCHAR,
    remote_type         VARCHAR,

    salary_min          FLOAT,
    salary_max          FLOAT,
    salary_midpoint     FLOAT,

    posted_date         DATE,
    close_date          DATE
);

ALTER TABLE fact_job_postings CLUSTER BY (sector, posted_date);

SHOW TABLES IN SCHEMA GOLD;
