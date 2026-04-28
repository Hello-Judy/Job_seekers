-- ============================================================
-- 11: Gold-layer analytical views — what the Streamlit dashboard reads.
-- ============================================================
--
-- Three views, each answering one of the questions the TA called out:
--
--   vw_salary_gap_federal_vs_private
--       "Are federal jobs paid more or less than private for the same role?"
--       For every SOC where we have postings on BOTH sides, returns the
--       median advertised salary by sector plus the gap.
--
--   vw_postings_vs_oews
--       "How does what employers advertise compare to the BLS wage benchmark?"
--       Joins fact_job_postings (advertised salary) to fact_oews_wages
--       (national wage percentiles) on the resolved SOC.
--
--   vw_qcew_employment_trend
--       "How has employment in this state/sector evolved over 30+ years?"
--       Pivots fact_qcew into a per-quarter time series, ready for line
--       charts on the dashboard.
--
-- All three are CREATE OR REPLACE so re-running this file is safe.
-- ============================================================

USE DATABASE JOBSEEKERS;
USE SCHEMA GOLD;


-- ---------- 1. Federal vs Private salary gap by SOC ----------
CREATE OR REPLACE VIEW vw_salary_gap_federal_vs_private AS
WITH median_by_sector AS (
    SELECT
        f.occupation_key,
        d.soc_code,
        d.soc_title,
        d.soc_major_title,
        f.sector,
        APPROX_PERCENTILE(f.salary_midpoint, 0.5)            AS median_advertised_salary,
        COUNT(*)                                             AS posting_count
    FROM fact_job_postings f
    JOIN dim_occupation d
      ON d.occupation_key = f.occupation_key
    WHERE f.salary_midpoint IS NOT NULL
      AND f.salary_midpoint BETWEEN 15000 AND 500000        -- ignore obvious outliers
      AND d.soc_code <> '99-9999'
    GROUP BY f.occupation_key, d.soc_code, d.soc_title, d.soc_major_title, f.sector
),
pivoted AS (
    SELECT
        occupation_key, soc_code, soc_title, soc_major_title,
        MAX(CASE WHEN sector = 'federal' THEN median_advertised_salary END)
                                                             AS federal_median,
        MAX(CASE WHEN sector = 'federal' THEN posting_count END)
                                                             AS federal_postings,
        MAX(CASE WHEN sector = 'private' THEN median_advertised_salary END)
                                                             AS private_median,
        MAX(CASE WHEN sector = 'private' THEN posting_count END)
                                                             AS private_postings
    FROM median_by_sector
    GROUP BY occupation_key, soc_code, soc_title, soc_major_title
)
SELECT
    soc_code, soc_title, soc_major_title,
    federal_median, federal_postings,
    private_median, private_postings,
    federal_median - private_median                         AS sector_gap_dollars,
    CASE
        WHEN private_median > 0
        THEN (federal_median - private_median) / private_median
        ELSE NULL
    END                                                      AS sector_gap_pct
FROM pivoted
WHERE federal_median IS NOT NULL
  AND private_median IS NOT NULL
ORDER BY ABS(sector_gap_dollars) DESC NULLS LAST;


-- ---------- 2. Posting wages vs OEWS national benchmark ----------
-- Joins on the most recent OEWS national-area row per SOC. The dashboard
-- can show "this posting pays X% above/below the national median for this role."
CREATE OR REPLACE VIEW vw_postings_vs_oews AS
WITH latest_national_oews AS (
    SELECT
        occ_code,
        ANY_VALUE(occ_title)                                 AS occ_title,
        MAX_BY(a_median, survey_year)                       AS national_a_median,
        MAX_BY(a_pct25,  survey_year)                       AS national_a_pct25,
        MAX_BY(a_pct75,  survey_year)                       AS national_a_pct75,
        MAX(survey_year)                                    AS reference_year
    FROM fact_oews_wages
    WHERE area_type = 1                                     -- national
    GROUP BY occ_code
)
SELECT
    f.posting_key,
    f.sector,
    f.posted_date,
    d.soc_code,
    d.soc_title,
    d.soc_major_title,
    f.salary_midpoint                                       AS advertised_salary,
    o.national_a_median,
    o.national_a_pct25,
    o.national_a_pct75,
    o.reference_year                                        AS oews_reference_year,
    CASE
        WHEN o.national_a_median > 0
        THEN (f.salary_midpoint - o.national_a_median) / o.national_a_median
        ELSE NULL
    END                                                     AS gap_vs_national_median
FROM fact_job_postings f
JOIN dim_occupation        d ON d.occupation_key = f.occupation_key
LEFT JOIN latest_national_oews o ON o.occ_code = d.soc_code
WHERE f.salary_midpoint IS NOT NULL
  AND f.salary_midpoint BETWEEN 15000 AND 500000
  AND d.soc_code <> '99-9999';


-- ---------- 3. QCEW employment trend by state × ownership ----------
-- Long history line chart: pick a state and an ownership category,
-- get one row per (year, qtr) with average monthly employment.
CREATE OR REPLACE VIEW vw_qcew_employment_trend AS
SELECT
    LEFT(area_fips, 2)                                       AS state_fips,
    own_label,
    year,
    qtr,
    DATE_FROM_PARTS(year, (qtr - 1) * 3 + 1, 1)             AS quarter_start_date,
    SUM(avg_monthly_emplvl)                                  AS state_employment,
    SUM(total_qtrly_wages)                                   AS state_qtrly_wages,
    AVG(avg_wkly_wage)                                       AS state_avg_wkly_wage
FROM fact_qcew
WHERE area_type = 'state'
  AND industry_code = '10'                                  -- Total, all industries
GROUP BY LEFT(area_fips, 2), own_label, year, qtr
ORDER BY state_fips, own_label, year, qtr;


SHOW VIEWS IN SCHEMA GOLD;
