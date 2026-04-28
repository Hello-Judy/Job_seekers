"""
Silver → Gold transformer for BLS-derived facts.

Builds three things:
    GOLD.dim_occupation       conformed SOC dimension
    GOLD.fact_qcew            QCEW employment fact
    GOLD.fact_oews_wages      OEWS wage fact

And amends GOLD.fact_job_postings.occupation_key so existing postings can
join to the new dim. The amendment is run separately because it touches
an existing fact rather than building a new one.

dim_location reuse note
-----------------------
Adzuna/USAJobs flow into dim_location keyed on (city, state, country). BLS
data is keyed on FIPS codes. We don't try to reconcile these in Day 2 —
fact_qcew carries area_fips natively and Streamlit joins on it directly.
A future improvement is a FIPS-to-(city,state) resolver; that's out of
scope here and noted in the BLS_INTEGRATION.md doc.
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
# dim_occupation
# ---------------------------------------------------------------------------
# One row per distinct soc_code in the Silver resolver output. soc_major_title
# is hard-coded from the BLS SOC structure (only 22 major groups so a CASE
# expression is cleanest).

POPULATE_DIM_OCCUPATION_SQL = """
MERGE INTO GOLD.dim_occupation AS tgt
USING (
    SELECT DISTINCT
        soc_code,
        ANY_VALUE(soc_title)        AS soc_title,
        soc_major_group,
        CASE soc_major_group
            WHEN '11-0000' THEN 'Management'
            WHEN '13-0000' THEN 'Business and Financial Operations'
            WHEN '15-0000' THEN 'Computer and Mathematical'
            WHEN '17-0000' THEN 'Architecture and Engineering'
            WHEN '19-0000' THEN 'Life, Physical, and Social Science'
            WHEN '21-0000' THEN 'Community and Social Service'
            WHEN '23-0000' THEN 'Legal'
            WHEN '25-0000' THEN 'Educational Instruction and Library'
            WHEN '27-0000' THEN 'Arts, Design, Entertainment, Sports, and Media'
            WHEN '29-0000' THEN 'Healthcare Practitioners and Technical'
            WHEN '31-0000' THEN 'Healthcare Support'
            WHEN '33-0000' THEN 'Protective Service'
            WHEN '35-0000' THEN 'Food Preparation and Serving'
            WHEN '37-0000' THEN 'Building and Grounds Cleaning and Maintenance'
            WHEN '39-0000' THEN 'Personal Care and Service'
            WHEN '41-0000' THEN 'Sales and Related'
            WHEN '43-0000' THEN 'Office and Administrative Support'
            WHEN '45-0000' THEN 'Farming, Fishing, and Forestry'
            WHEN '47-0000' THEN 'Construction and Extraction'
            WHEN '49-0000' THEN 'Installation, Maintenance, and Repair'
            WHEN '51-0000' THEN 'Production'
            WHEN '53-0000' THEN 'Transportation and Material Moving'
            WHEN '55-0000' THEN 'Military Specific'
            ELSE 'Unmapped'
        END                          AS soc_major_title
    FROM SILVER.dim_occupation_soc
    GROUP BY soc_code, soc_major_group
) AS src
ON tgt.soc_code = src.soc_code
WHEN MATCHED THEN UPDATE SET
    soc_title       = src.soc_title,
    soc_major_group = src.soc_major_group,
    soc_major_title = src.soc_major_title
WHEN NOT MATCHED THEN INSERT (soc_code, soc_title, soc_major_group, soc_major_title)
VALUES (src.soc_code, src.soc_title, src.soc_major_group, src.soc_major_title)
"""


# ---------------------------------------------------------------------------
# fact_qcew
# ---------------------------------------------------------------------------
# date_key is the first day of the quarter (e.g. Q3 2024 -> 20240701).
# We don't join to dim_location because QCEW areas are FIPS-coded and don't
# always match our (city, state) postings dim. fact_qcew carries area_fips
# directly and the dashboard joins on it.

POPULATE_FACT_QCEW_SQL = """
MERGE INTO GOLD.fact_qcew AS tgt
USING (
    SELECT
        s.qcew_key,
        TO_NUMBER(TO_CHAR(
            DATE_FROM_PARTS(s.year, (s.qtr - 1) * 3 + 1, 1), 'YYYYMMDD'
        ))                                                  AS date_key,
        NULL::INT                                            AS location_key,
        s.area_fips, s.area_type,
        s.industry_code, s.industry_level,
        s.own_code, s.own_label,
        s.qtrly_estabs, s.avg_monthly_emplvl,
        s.total_qtrly_wages, s.avg_wkly_wage,
        s.year, s.qtr
    FROM SILVER.bls_qcew_quarterly s
) AS src
ON tgt.qcew_key = src.qcew_key
WHEN MATCHED THEN UPDATE SET
    date_key            = src.date_key,
    area_fips           = src.area_fips,
    area_type           = src.area_type,
    industry_code       = src.industry_code,
    industry_level      = src.industry_level,
    own_code            = src.own_code,
    own_label           = src.own_label,
    qtrly_estabs        = src.qtrly_estabs,
    avg_monthly_emplvl  = src.avg_monthly_emplvl,
    total_qtrly_wages   = src.total_qtrly_wages,
    avg_wkly_wage       = src.avg_wkly_wage,
    year                = src.year,
    qtr                 = src.qtr
WHEN NOT MATCHED THEN INSERT (
    qcew_key, date_key, location_key,
    area_fips, area_type,
    industry_code, industry_level, own_code, own_label,
    qtrly_estabs, avg_monthly_emplvl, total_qtrly_wages, avg_wkly_wage,
    year, qtr
) VALUES (
    src.qcew_key, src.date_key, src.location_key,
    src.area_fips, src.area_type,
    src.industry_code, src.industry_level, src.own_code, src.own_label,
    src.qtrly_estabs, src.avg_monthly_emplvl, src.total_qtrly_wages, src.avg_wkly_wage,
    src.year, src.qtr
)
"""


# ---------------------------------------------------------------------------
# fact_oews_wages
# ---------------------------------------------------------------------------
# date_key uses May 1 of survey_year (OEWS reference date is May).

POPULATE_FACT_OEWS_SQL = """
MERGE INTO GOLD.fact_oews_wages AS tgt
USING (
    SELECT
        s.oews_key,
        TO_NUMBER(TO_CHAR(
            DATE_FROM_PARTS(s.survey_year, 5, 1), 'YYYYMMDD'
        ))                                                  AS date_key,
        d.occupation_key,
        NULL::INT                                            AS location_key,
        s.survey_year, s.area, s.area_title, s.area_type,
        s.occ_code, s.occ_title,
        s.tot_emp,
        s.a_mean, s.a_pct10, s.a_pct25, s.a_median, s.a_pct75, s.a_pct90,
        s.h_mean, s.h_median
    FROM SILVER.bls_oews_annual s
    LEFT JOIN GOLD.dim_occupation d
      ON d.soc_code = s.occ_code
) AS src
ON tgt.oews_key = src.oews_key
WHEN MATCHED THEN UPDATE SET
    date_key       = src.date_key,
    occupation_key = src.occupation_key,
    survey_year    = src.survey_year,
    area           = src.area,  area_title = src.area_title,  area_type = src.area_type,
    occ_code       = src.occ_code,  occ_title = src.occ_title,
    tot_emp        = src.tot_emp,
    a_mean   = src.a_mean,    a_pct10  = src.a_pct10,  a_pct25  = src.a_pct25,
    a_median = src.a_median,  a_pct75  = src.a_pct75,  a_pct90  = src.a_pct90,
    h_mean   = src.h_mean,    h_median = src.h_median
WHEN NOT MATCHED THEN INSERT (
    oews_key, date_key, occupation_key, location_key,
    survey_year, area, area_title, area_type,
    occ_code, occ_title,
    tot_emp, a_mean, a_pct10, a_pct25, a_median, a_pct75, a_pct90,
    h_mean, h_median
) VALUES (
    src.oews_key, src.date_key, src.occupation_key, src.location_key,
    src.survey_year, src.area, src.area_title, src.area_type,
    src.occ_code, src.occ_title,
    src.tot_emp, src.a_mean, src.a_pct10, src.a_pct25, src.a_median,
    src.a_pct75, src.a_pct90,
    src.h_mean, src.h_median
)
"""


# ---------------------------------------------------------------------------
# Backfill occupation_key into existing fact_job_postings
# ---------------------------------------------------------------------------
# We resolve via the Silver dim using the same normalization the resolver used.

UPDATE_FACT_POSTINGS_SOC_SQL = """
UPDATE GOLD.fact_job_postings AS f
SET occupation_key = d.occupation_key
FROM SILVER.dim_occupation_soc s
JOIN GOLD.dim_occupation d ON d.soc_code = s.soc_code
JOIN SILVER.jobs_unified u
  ON TRIM(REGEXP_REPLACE(LOWER(u.job_title), '[^a-z0-9 ]+', ' '))
     = s.title_normalized
WHERE f.posting_key = u.source || '::' || u.job_id
"""


class BLSSilverToGoldTransformer:
    def __init__(self) -> None:
        require("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")

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

    def _execute(self, sql: str, label: str) -> int:
        logger.info("Running: %s", label)
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                affected = cur.rowcount or 0
                conn.commit()
            finally:
                cur.close()
        logger.info("%s affected %d rows", label, affected)
        return affected

    def populate_dim_occupation(self) -> int:
        return self._execute(POPULATE_DIM_OCCUPATION_SQL, "dim_occupation")

    def populate_fact_qcew(self) -> int:
        return self._execute(POPULATE_FACT_QCEW_SQL, "fact_qcew")

    def populate_fact_oews(self) -> int:
        return self._execute(POPULATE_FACT_OEWS_SQL, "fact_oews_wages")

    def update_postings_soc(self) -> int:
        return self._execute(UPDATE_FACT_POSTINGS_SOC_SQL, "fact_job_postings.occupation_key")

    def run_all(self) -> dict[str, int]:
        return {
            "dim_occupation":   self.populate_dim_occupation(),
            "fact_qcew":        self.populate_fact_qcew(),
            "fact_oews_wages":  self.populate_fact_oews(),
            "postings_soc_fk":  self.update_postings_soc(),
        }
