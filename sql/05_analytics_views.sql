-- ============================================================
-- 05: Analytics views — deduplication audit + dashboard layer
-- ============================================================
--
-- SECTION 1  (SILVER schema): dedup audit + quality base views
-- SECTION 2  (GOLD   schema): 8 multi-dimensional comparison views
--
-- All views are CREATE OR REPLACE, safe to re-run at any time.
-- Dashboard queries SHOULD use GOLD views; SILVER views are for
-- data-quality inspection only.
-- ============================================================

USE DATABASE JOBSEEKERS;


-- ============================================================
-- SECTION 1: Silver — deduplication audit
-- ============================================================

USE SCHEMA SILVER;

-- 1.1  PK violation check  (should always return 0 rows if MERGEs are correct)
CREATE OR REPLACE VIEW v_pk_violations AS
SELECT
    source,
    job_id,
    COUNT(*) AS n
FROM jobs_unified
GROUP BY source, job_id
HAVING COUNT(*) > 1;


-- 1.2  Near-duplicate candidates
--      Same normalised title + company + state + date across ANY source.
--      Identifies jobs reposted with a different ID, or the same open role
--      that appeared under two different search keywords.
--      Note: job_title_normalized collapses punctuation via REGEXP_REPLACE
--      with [^a-zA-Z0-9]+ → ' ', so "Sr., Engineer" == "sr engineer" here.
CREATE OR REPLACE VIEW v_near_duplicates AS
SELECT
    TRIM(REGEXP_REPLACE(job_title_normalized, '\\s+', ' ')) AS title_norm,
    LOWER(TRIM(company_or_agency))                          AS company_norm,
    location_state,
    posted_date,
    COUNT(*)                                                AS n_rows,
    ARRAY_AGG(DISTINCT source)                              AS sources,
    ARRAY_AGG(DISTINCT job_id)                              AS job_ids
FROM jobs_unified
WHERE job_title_normalized IS NOT NULL
  AND company_or_agency    IS NOT NULL
  AND posted_date          IS NOT NULL
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
ORDER BY n_rows DESC;


-- 1.3  Quality-filtered base view
--      Filters out rows with:
--        • NULL job_title / source / sector (un-parseable records)
--        • Implausible annual salary values (keeps NULLs)
--        • salary_min > salary_max (data entry errors)
--      Used by all GOLD views below.
CREATE OR REPLACE VIEW v_jobs_clean AS
SELECT *
FROM jobs_unified
WHERE job_title IS NOT NULL
  AND source     IS NOT NULL
  AND sector     IS NOT NULL
  AND (salary_min IS NULL OR salary_min BETWEEN 10000 AND 500000)
  AND (salary_max IS NULL OR salary_max BETWEEN 10000 AND 500000)
  AND (salary_min IS NULL OR salary_max IS NULL OR salary_min <= salary_max);


-- 1.4  Deduplicated view  (one row per logical job posting)
--      When the same job appears more than once — same title, company, state,
--      and date — keeps the row with the latest ingestion_timestamp.
--      Priority: USAJobs > Adzuna when both sources have the same posting
--      (federal jobs that were also scraped by a private aggregator).
CREATE OR REPLACE VIEW v_jobs_deduped AS
SELECT * EXCLUDE (_dedup_rn)
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                TRIM(REGEXP_REPLACE(job_title_normalized, '\\s+', ' ')),
                LOWER(TRIM(company_or_agency)),
                location_state,
                posted_date
            ORDER BY
                -- prefer usajobs rows; within the same source take the freshest
                CASE WHEN source LIKE 'usajobs%' THEN 0 ELSE 1 END,
                ingestion_timestamp DESC
        ) AS _dedup_rn
    FROM v_jobs_clean
    WHERE job_title_normalized IS NOT NULL
      AND company_or_agency    IS NOT NULL
)
WHERE _dedup_rn = 1;


-- 1.5  Data-quality summary  (run ad-hoc; used by quality.py)
CREATE OR REPLACE VIEW v_quality_summary AS
SELECT
    source,
    COUNT(*)                                                       AS total_rows,
    COUNT(CASE WHEN job_title      IS NULL THEN 1 END)             AS null_title,
    COUNT(CASE WHEN salary_min     IS NULL THEN 1 END)             AS null_salary_min,
    COUNT(CASE WHEN salary_max     IS NULL THEN 1 END)             AS null_salary_max,
    COUNT(CASE WHEN location_state IS NULL THEN 1 END)             AS null_state,
    COUNT(CASE WHEN location_city  IS NULL THEN 1 END)             AS null_city,
    COUNT(CASE WHEN posted_date    IS NULL THEN 1 END)             AS null_posted_date,
    COUNT(CASE WHEN remote_type    = 'unknown' THEN 1 END)         AS unknown_remote,
    COUNT(CASE WHEN experience_level NOT IN ('entry','mid','senior') THEN 1 END)
                                                                   AS bad_exp_level,
    ROUND(COUNT(CASE WHEN salary_min IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1)
                                                                   AS pct_has_salary
FROM jobs_unified
GROUP BY source
ORDER BY source;


-- ============================================================
-- SECTION 2: Gold — multi-dimensional comparison views
-- ============================================================

USE SCHEMA GOLD;

-- 2.1  Federal vs. Private salary comparison
--      One row per (sector, source, experience_level).
--      Streamlit use: box plots / bar charts comparing salary distributions.
CREATE OR REPLACE VIEW v_salary_by_sector AS
SELECT
    sector,
    source,
    experience_level,
    COUNT(*)                                                       AS job_count,
    COUNT(CASE WHEN salary_min IS NOT NULL
                 OR salary_max IS NOT NULL THEN 1 END)             AS jobs_with_salary,
    ROUND(AVG(COALESCE(salary_min, salary_max)), 0)                AS avg_salary_low,
    ROUND(AVG(COALESCE(salary_max, salary_min)), 0)                AS avg_salary_high,
    ROUND(AVG((COALESCE(salary_min,0) + COALESCE(salary_max,0))
              / NULLIF(IFF(salary_min IS NOT NULL,1,0)
                     + IFF(salary_max IS NOT NULL,1,0), 0)), 0)    AS avg_salary_mid,
    ROUND(MEDIAN(salary_min), 0)                                   AS median_salary_min,
    ROUND(MEDIAN(salary_max), 0)                                   AS median_salary_max,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary_min), 0) AS p25_salary,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary_max), 0) AS p75_salary
FROM SILVER.v_jobs_clean
GROUP BY 1, 2, 3;


-- 2.2  Entry-level job availability by sector and category
--      Streamlit use: stacked bar of experience_level mix by sector/category.
CREATE OR REPLACE VIEW v_experience_by_sector AS
SELECT
    sector,
    source,
    experience_level,
    employment_type,
    COALESCE(dcat.category_group, 'Other')                         AS category_group,
    COUNT(*)                                                       AS job_count,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY sector, source),
        2)                                                         AS pct_of_source
FROM SILVER.v_jobs_clean ju
LEFT JOIN GOLD.dim_category dcat ON dcat.category_name = ju.job_category
GROUP BY 1, 2, 3, 4, 5;


-- 2.3  Geographic distribution of postings
--      Streamlit use: choropleth map by state, or city-level dot map.
CREATE OR REPLACE VIEW v_geo_distribution AS
SELECT
    location_state,
    location_city,
    location_country,
    sector,
    source,
    experience_level,
    COUNT(*)                                                       AS job_count,
    COUNT(CASE WHEN is_entry_level THEN 1 END)                    AS entry_level_count,
    ROUND(AVG(
        COALESCE(salary_min, salary_max,
                 (salary_min + salary_max) / 2.0)), 0)             AS avg_salary
FROM SILVER.v_jobs_clean
WHERE location_state IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6;


-- 2.4  Remote / hybrid / onsite breakdown by sector
--      Note: Adzuna remote_type='unknown' means the listing didn't mention
--      remote/hybrid — NOT confirmed onsite. Treat 'unknown' as its own bucket.
--      Streamlit use: grouped bar or pie chart per sector.
CREATE OR REPLACE VIEW v_remote_by_sector AS
SELECT
    sector,
    source,
    remote_type,
    experience_level,
    COUNT(*)                                                       AS job_count,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY sector, source),
        2)                                                         AS pct_of_source
FROM SILVER.v_jobs_clean
GROUP BY 1, 2, 3, 4;


-- 2.5  Job-category breakdown by sector
--      Streamlit use: treemap or horizontal bar showing category mix.
CREATE OR REPLACE VIEW v_category_by_sector AS
SELECT
    sector,
    source,
    ju.job_category,
    COALESCE(dcat.category_group, 'Other')                         AS category_group,
    experience_level,
    COUNT(*)                                                       AS job_count,
    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (PARTITION BY sector, source),
        2)                                                         AS pct_of_source
FROM SILVER.v_jobs_clean ju
LEFT JOIN GOLD.dim_category dcat ON dcat.category_name = ju.job_category
GROUP BY 1, 2, 3, 4, 5;


-- 2.6  Top employers / agencies by posting volume
--      Streamlit use: horizontal bar chart, filterable by sector/experience.
CREATE OR REPLACE VIEW v_top_employers AS
SELECT
    sector,
    source,
    company_or_agency,
    COALESCE(dc.agency_parent, company_or_agency)                  AS employer_label,
    experience_level,
    COUNT(*)                                                       AS job_count,
    COUNT(CASE WHEN is_entry_level THEN 1 END)                    AS entry_level_count,
    ROUND(AVG(salary_min), 0)                                      AS avg_salary_min
FROM SILVER.v_jobs_clean ju
LEFT JOIN GOLD.dim_company dc ON dc.company_name = ju.company_or_agency
WHERE company_or_agency IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
ORDER BY job_count DESC;


-- 2.7  Posting trends over time  (daily → weekly roll-up)
--      Streamlit use: line chart showing posting volume by sector over time.
CREATE OR REPLACE VIEW v_posting_trends AS
SELECT
    DATE_TRUNC('week', posted_date)                                AS week_start,
    sector,
    source,
    experience_level,
    COUNT(*)                                                       AS job_count,
    COUNT(CASE WHEN is_entry_level THEN 1 END)                    AS entry_level_count,
    ROUND(AVG(salary_min), 0)                                      AS avg_salary_min
FROM SILVER.v_jobs_clean
WHERE posted_date IS NOT NULL
  AND posted_date >= DATEADD('month', -6, CURRENT_DATE())
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;


-- 2.8  Federal vs. private salary gap at entry level
--      Core thesis view: do federal entry-level jobs pay more/less/differently
--      than private ones in the same category?
--      Streamlit use: scatter plot or grouped bar, one point per category.
CREATE OR REPLACE VIEW v_entry_salary_gap AS
SELECT
    COALESCE(dcat.category_group, 'Other')                         AS category_group,
    sector,
    COUNT(*)                                                       AS job_count,
    ROUND(AVG(salary_min), 0)                                      AS avg_salary_min,
    ROUND(AVG(salary_max), 0)                                      AS avg_salary_max,
    ROUND(MEDIAN((COALESCE(salary_min,0) + COALESCE(salary_max,0))
                 / NULLIF(IFF(salary_min IS NOT NULL,1,0)
                        + IFF(salary_max IS NOT NULL,1,0), 0)), 0) AS median_salary_mid,
    ROUND(
        AVG(salary_min)
        - LAG(AVG(salary_min)) OVER (PARTITION BY COALESCE(dcat.category_group,'Other')
                                     ORDER BY sector),
        0)                                                         AS salary_gap_vs_prev_sector
FROM SILVER.v_jobs_clean ju
LEFT JOIN GOLD.dim_category dcat ON dcat.category_name = ju.job_category
WHERE is_entry_level = TRUE
  AND (salary_min IS NOT NULL OR salary_max IS NOT NULL)
GROUP BY 1, 2;


-- ============================================================
-- Sanity-check queries (run manually to verify)
-- ============================================================
-- SELECT * FROM SILVER.v_pk_violations;                  -- expect 0 rows
-- SELECT * FROM SILVER.v_quality_summary;                -- inspect nulls
-- SELECT COUNT(*) FROM SILVER.v_near_duplicates;         -- monitor near-dups
-- SELECT * FROM GOLD.v_salary_by_sector LIMIT 20;
-- SELECT * FROM GOLD.v_entry_salary_gap ORDER BY category_group, sector;

SHOW VIEWS IN SCHEMA SILVER;
SHOW VIEWS IN SCHEMA GOLD;
