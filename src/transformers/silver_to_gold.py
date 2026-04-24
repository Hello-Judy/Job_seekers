"""
Silver → Gold transformer.

Builds the star schema in GOLD from the unified SILVER.jobs_unified table.

Execution order matters — dimensions must be populated before the fact table:
    1. dim_date      (standalone date spine, independent of Silver)
    2. dim_location  (SILVER → distinct city/state/country combos)
    3. dim_company   (SILVER → distinct company/agency names)
    4. dim_category  (SILVER → distinct job categories)
    5. fact_job_postings (joins Silver to all four dims)

All statements are idempotent MERGEs, safe to re-run after incremental Silver
updates. dim_date only uses WHEN NOT MATCHED (calendar attributes never change);
the other dims include WHEN MATCHED UPDATE to pick up label corrections.

Why SQL here (not PySpark)?
    The Silver→Gold step is mostly a GROUP BY + MERGE — Snowflake executes this
    far cheaper than shipping the data to a Spark cluster and back. PySpark
    would only add value if we were doing ML feature engineering or very heavy
    transformations that don't map cleanly to SQL.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.utils.config import settings, require

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# dim_date  —  calendar date spine, 2020-01-01 through 2030-12-31
# ---------------------------------------------------------------------------
# Generated entirely inside Snowflake using TABLE(GENERATOR()), so it doesn't
# depend on Silver at all.  ROWCOUNT=4018 covers the decade precisely.

POPULATE_DIM_DATE_SQL = """
MERGE INTO GOLD.dim_date AS tgt
USING (
    SELECT
        TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))   AS date_key,
        d                                   AS full_date,
        YEAR(d)                             AS year,
        QUARTER(d)                          AS quarter,
        MONTH(d)                            AS month,
        MONTHNAME(d)                        AS month_name,
        DAY(d)                              AS day_of_month,
        DAYOFWEEK(d)                        AS day_of_week,   -- 0=Sun … 6=Sat
        DAYNAME(d)                          AS day_name,
        DAYOFWEEK(d) IN (0, 6)             AS is_weekend
    FROM (
        SELECT DATEADD('day', SEQ4(), DATE '2020-01-01') AS d
        FROM TABLE(GENERATOR(ROWCOUNT => 4018))          -- 2020-01-01 to 2031-01-01
    )
    WHERE d < DATE '2031-01-01'
) AS src
ON tgt.date_key = src.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, year, quarter, month, month_name,
    day_of_month, day_of_week, day_name, is_weekend
) VALUES (
    src.date_key, src.full_date, src.year, src.quarter, src.month, src.month_name,
    src.day_of_month, src.day_of_week, src.day_name, src.is_weekend
)
"""


# ---------------------------------------------------------------------------
# dim_location  —  distinct city/state/country combos from Silver
# ---------------------------------------------------------------------------
# region uses the four US Census Bureau regions plus 'Other' for non-US rows.
# lat/lon are averaged across all Silver rows that share the same (city, state,
# country) — a reasonable centroid when a city appears multiple times.

POPULATE_DIM_LOCATION_SQL = """
MERGE INTO GOLD.dim_location AS tgt
USING (
    SELECT
        location_city                               AS city,
        location_state                              AS state,
        location_country                            AS country,
        CASE
            WHEN location_state IN (
                     'Connecticut','Maine','Massachusetts','New Hampshire',
                     'Rhode Island','Vermont',
                     'New Jersey','New York','Pennsylvania')
                 THEN 'Northeast'
            WHEN location_state IN (
                     'Indiana','Illinois','Michigan','Ohio','Wisconsin',
                     'Iowa','Kansas','Minnesota','Missouri','Nebraska',
                     'North Dakota','South Dakota')
                 THEN 'Midwest'
            WHEN location_state IN (
                     'Delaware','Florida','Georgia','Maryland','North Carolina',
                     'South Carolina','Virginia','District of Columbia','West Virginia',
                     'Alabama','Kentucky','Mississippi','Tennessee',
                     'Arkansas','Louisiana','Oklahoma','Texas')
                 THEN 'South'
            WHEN location_state IN (
                     'Arizona','Colorado','Idaho','New Mexico','Montana',
                     'Utah','Nevada','Wyoming',
                     'Alaska','California','Hawaii','Oregon','Washington')
                 THEN 'West'
            ELSE 'Other'
        END                                         AS region,
        AVG(latitude)                               AS latitude,
        AVG(longitude)                              AS longitude
    FROM SILVER.jobs_unified
    WHERE location_city IS NOT NULL
      AND location_state IS NOT NULL
      AND location_country IS NOT NULL
    GROUP BY 1, 2, 3, 4
) AS src
ON  tgt.city    = src.city
AND tgt.state   = src.state
AND tgt.country = src.country
WHEN MATCHED THEN UPDATE SET
    region    = src.region,
    latitude  = src.latitude,
    longitude = src.longitude
WHEN NOT MATCHED THEN INSERT (city, state, country, region, latitude, longitude)
VALUES (src.city, src.state, src.country, src.region, src.latitude, src.longitude)
"""


# ---------------------------------------------------------------------------
# dim_company  —  distinct company / agency names from Silver
# ---------------------------------------------------------------------------
# agency_parent maps common USAJobs agency strings to their cabinet-level
# department abbreviation; NULL for private-sector companies.

POPULATE_DIM_COMPANY_SQL = """
MERGE INTO GOLD.dim_company AS tgt
USING (
    SELECT DISTINCT
        company_or_agency       AS company_name,
        sector,
        CASE
            WHEN sector = 'federal' THEN
                CASE
                    WHEN LOWER(company_or_agency) LIKE '%department of defense%'          THEN 'DOD'
                    WHEN LOWER(company_or_agency) LIKE '%department of veterans affairs%'  THEN 'VA'
                    WHEN LOWER(company_or_agency) LIKE '%department of justice%'           THEN 'DOJ'
                    WHEN LOWER(company_or_agency) LIKE '%department of homeland security%' THEN 'DHS'
                    WHEN LOWER(company_or_agency) LIKE '%department of state%'             THEN 'DOS'
                    WHEN LOWER(company_or_agency) LIKE '%health and human services%'       THEN 'HHS'
                    WHEN LOWER(company_or_agency) LIKE '%department of the treasury%'      THEN 'Treasury'
                    WHEN LOWER(company_or_agency) LIKE '%department of energy%'            THEN 'DOE'
                    WHEN LOWER(company_or_agency) LIKE '%department of transportation%'    THEN 'DOT'
                    WHEN LOWER(company_or_agency) LIKE '%department of labor%'             THEN 'DOL'
                    WHEN LOWER(company_or_agency) LIKE '%department of commerce%'          THEN 'DOC'
                    WHEN LOWER(company_or_agency) LIKE '%department of agriculture%'       THEN 'USDA'
                    WHEN LOWER(company_or_agency) LIKE '%department of the interior%'      THEN 'DOI'
                    WHEN LOWER(company_or_agency) LIKE '%department of education%'         THEN 'ED'
                    WHEN LOWER(company_or_agency) LIKE '%nasa%'                            THEN 'NASA'
                    WHEN LOWER(company_or_agency) LIKE '%department of the air force%'     THEN 'USAF'
                    WHEN LOWER(company_or_agency) LIKE '%department of the army%'          THEN 'USA'
                    WHEN LOWER(company_or_agency) LIKE '%department of the navy%'          THEN 'USN'
                    ELSE NULL
                END
            ELSE NULL
        END                     AS agency_parent
    FROM SILVER.jobs_unified
    WHERE company_or_agency IS NOT NULL
) AS src
ON tgt.company_name = src.company_name
WHEN MATCHED THEN UPDATE SET
    sector        = src.sector,
    agency_parent = src.agency_parent
WHEN NOT MATCHED THEN INSERT (company_name, sector, agency_parent)
VALUES (src.company_name, src.sector, src.agency_parent)
"""


# ---------------------------------------------------------------------------
# dim_category  —  distinct job category labels from Silver
# ---------------------------------------------------------------------------
# category_group rolls up fine-grained labels (Adzuna free text, USAJobs
# occupation series names) into seven broad buckets for cross-source analysis.

POPULATE_DIM_CATEGORY_SQL = """
MERGE INTO GOLD.dim_category AS tgt
USING (
    SELECT DISTINCT
        job_category        AS category_name,
        CASE
            WHEN LOWER(job_category) LIKE '%information technology%'
              OR LOWER(job_category) LIKE '%software%'
              OR LOWER(job_category) LIKE '% it %'
              OR LOWER(job_category) LIKE '%computer%'
              OR LOWER(job_category) LIKE '%data %'
              OR LOWER(job_category) LIKE '%engineer%'
              OR LOWER(job_category) LIKE '%cybersecurity%'
                 THEN 'Tech'
            WHEN LOWER(job_category) LIKE '%health%'
              OR LOWER(job_category) LIKE '%medical%'
              OR LOWER(job_category) LIKE '%nurse%'
              OR LOWER(job_category) LIKE '%physician%'
              OR LOWER(job_category) LIKE '%clinical%'
              OR LOWER(job_category) LIKE '%dental%'
                 THEN 'Healthcare'
            WHEN LOWER(job_category) LIKE '%finance%'
              OR LOWER(job_category) LIKE '%accounting%'
              OR LOWER(job_category) LIKE '%budget%'
              OR LOWER(job_category) LIKE '%audit%'
              OR LOWER(job_category) LIKE '%fiscal%'
                 THEN 'Finance'
            WHEN LOWER(job_category) LIKE '%legal%'
              OR LOWER(job_category) LIKE '%attorney%'
              OR LOWER(job_category) LIKE '%law %'
              OR LOWER(job_category) LIKE '%judicial%'
              OR LOWER(job_category) LIKE '%paralegal%'
                 THEN 'Legal'
            WHEN LOWER(job_category) LIKE '%human resources%'
              OR LOWER(job_category) LIKE '% hr %'
              OR LOWER(job_category) LIKE '%personnel%'
                 THEN 'HR'
            WHEN LOWER(job_category) LIKE '%logistics%'
              OR LOWER(job_category) LIKE '%supply chain%'
              OR LOWER(job_category) LIKE '%procurement%'
              OR LOWER(job_category) LIKE '%acquisition%'
                 THEN 'Operations'
            WHEN LOWER(job_category) LIKE '%admin%'
              OR LOWER(job_category) LIKE '%clerical%'
              OR LOWER(job_category) LIKE '%management analyst%'
                 THEN 'Admin'
            ELSE 'Other'
        END                 AS category_group
    FROM SILVER.jobs_unified
    WHERE job_category IS NOT NULL
) AS src
ON tgt.category_name = src.category_name
WHEN MATCHED THEN UPDATE SET
    category_group = src.category_group
WHEN NOT MATCHED THEN INSERT (category_name, category_group)
VALUES (src.category_name, src.category_group)
"""


# ---------------------------------------------------------------------------
# fact_job_postings  —  one row per job, FK-linked to all four dims
# ---------------------------------------------------------------------------
# salary_midpoint: average of min+max when both present; single value otherwise.
# dim lookups are LEFT JOINs so a missing/null dimension won't drop the fact row.
# posted_date NULL → date_key NULL (Snowflake FK constraints are advisory only).

POPULATE_FACT_SQL = """
MERGE INTO GOLD.fact_job_postings AS tgt
USING (
    SELECT
        ju.source || '::' || ju.job_id                    AS posting_key,
        TO_NUMBER(TO_CHAR(ju.posted_date, 'YYYYMMDD'))    AS date_key,
        dl.location_key,
        dc.company_key,
        dcat.category_key,
        ju.source,
        ju.sector,
        ju.experience_level,
        ju.is_entry_level,
        ju.employment_type,
        ju.remote_type,
        ju.salary_min,
        ju.salary_max,
        CASE
            WHEN ju.salary_min IS NOT NULL AND ju.salary_max IS NOT NULL
                 THEN (ju.salary_min + ju.salary_max) / 2.0
            WHEN ju.salary_min IS NOT NULL  THEN ju.salary_min
            WHEN ju.salary_max IS NOT NULL  THEN ju.salary_max
            ELSE NULL
        END                                               AS salary_midpoint,
        ju.posted_date,
        ju.close_date,
        ju.apply_url
    FROM SILVER.jobs_unified ju
    LEFT JOIN GOLD.dim_location dl
           ON dl.city    = ju.location_city
          AND dl.state   = ju.location_state
          AND dl.country = ju.location_country
    LEFT JOIN GOLD.dim_company dc
           ON dc.company_name = ju.company_or_agency
    LEFT JOIN GOLD.dim_category dcat
           ON dcat.category_name = ju.job_category
) AS src
ON tgt.posting_key = src.posting_key
WHEN MATCHED THEN UPDATE SET
    date_key         = src.date_key,
    location_key     = src.location_key,
    company_key      = src.company_key,
    category_key     = src.category_key,
    source           = src.source,
    sector           = src.sector,
    experience_level = src.experience_level,
    is_entry_level   = src.is_entry_level,
    employment_type  = src.employment_type,
    remote_type      = src.remote_type,
    salary_min       = src.salary_min,
    salary_max       = src.salary_max,
    salary_midpoint  = src.salary_midpoint,
    posted_date      = src.posted_date,
    close_date       = src.close_date,
    apply_url        = src.apply_url
WHEN NOT MATCHED THEN INSERT (
    posting_key, date_key, location_key, company_key, category_key,
    source, sector, experience_level, is_entry_level, employment_type, remote_type,
    salary_min, salary_max, salary_midpoint, posted_date, close_date, apply_url
) VALUES (
    src.posting_key, src.date_key, src.location_key, src.company_key, src.category_key,
    src.source, src.sector, src.experience_level, src.is_entry_level, src.employment_type, src.remote_type,
    src.salary_min, src.salary_max, src.salary_midpoint, src.posted_date, src.close_date, src.apply_url
)
"""


class SilverToGoldTransformer:
    """Populates the GOLD star schema from SILVER.jobs_unified.

    Call run_all() to refresh everything in dependency order, or call
    individual populate_* methods for targeted refreshes.
    """

    def __init__(self) -> None:
        require(
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
        )

    @contextmanager
    def _connect(self) -> Iterator[SnowflakeConnection]:
        conn = snowflake.connector.connect(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            role=settings.SNOWFLAKE_ROLE,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
        )
        try:
            yield conn
        finally:
            conn.close()

    def _execute_merge(self, sql: str, label: str) -> int:
        logger.info("Running MERGE: %s", label)
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                affected = cur.rowcount
                conn.commit()
            finally:
                cur.close()
        logger.info("%s affected %s rows", label, affected)
        return affected or 0

    def populate_dim_date(self) -> int:
        return self._execute_merge(POPULATE_DIM_DATE_SQL, "dim_date")

    def populate_dim_location(self) -> int:
        return self._execute_merge(POPULATE_DIM_LOCATION_SQL, "dim_location")

    def populate_dim_company(self) -> int:
        return self._execute_merge(POPULATE_DIM_COMPANY_SQL, "dim_company")

    def populate_dim_category(self) -> int:
        return self._execute_merge(POPULATE_DIM_CATEGORY_SQL, "dim_category")

    def populate_fact_job_postings(self) -> int:
        return self._execute_merge(POPULATE_FACT_SQL, "fact_job_postings")

    def run_all(self) -> dict[str, int]:
        """Populate all Gold tables in dependency order. Returns row counts."""
        results: dict[str, int] = {}
        results["dim_date"]          = self.populate_dim_date()
        results["dim_location"]      = self.populate_dim_location()
        results["dim_company"]       = self.populate_dim_company()
        results["dim_category"]      = self.populate_dim_category()
        results["fact_job_postings"] = self.populate_fact_job_postings()
        return results
