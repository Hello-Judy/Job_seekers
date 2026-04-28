"""
Analytics queries — read-only views into the Gold star schema.

Each function returns a small pandas DataFrame already shaped for plotting.
We do as much aggregation as possible inside Snowflake so Streamlit only
ever pulls down a few hundred rows; that keeps the dashboard snappy and
the warehouse-bill predictable.

Why a separate module from snowflake_reader.py?
    snowflake_reader.py serves the existing job-board UI (filters, search,
    saved jobs). The Analytics tab has a completely different access pattern
    — heavy aggregations, no row-level reads. Keeping them in different
    modules makes the cache TTLs and SQL shapes easier to reason about.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st
import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.utils.config import settings


def _conn():
    return snowflake.connector.connect(
        account=settings.SNOWFLAKE_ACCOUNT,
        user=settings.SNOWFLAKE_USER,
        password=settings.SNOWFLAKE_PASSWORD,
        role=settings.SNOWFLAKE_ROLE,
        warehouse=settings.SNOWFLAKE_WAREHOUSE,
        database=settings.SNOWFLAKE_DATABASE,
    )


def _df(sql: str) -> pd.DataFrame:
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ─────────────────────────────────────────────────────────────────────────────
# 1.  Federal vs Private salary gap (the headline insight)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=600)
def federal_vs_private_gap(min_postings: int = 1) -> pd.DataFrame:
    """One row per SOC where we have postings on BOTH sides.

    The min_postings filter lets the UI surface only well-supported
    occupations — useful because some SOCs only have a single posting on
    one side and the median reduces to a point estimate.
    """
    return _df(f"""
        SELECT
            soc_code,
            soc_title,
            soc_major_title,
            federal_median,
            federal_postings,
            private_median,
            private_postings,
            sector_gap_dollars,
            sector_gap_pct
        FROM JOBSEEKERS.GOLD.vw_salary_gap_federal_vs_private
        WHERE federal_postings >= {min_postings}
          AND private_postings >= {min_postings}
        ORDER BY ABS(sector_gap_dollars) DESC NULLS LAST
        LIMIT 30
    """)


# ─────────────────────────────────────────────────────────────────────────────
# 2.  SOC distribution of our postings — what kinds of jobs we have
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=600)
def soc_major_distribution() -> pd.DataFrame:
    """Posting count by SOC major group (e.g. 'Computer and Mathematical')
    split by sector. Drives the stacked-bar overview at the top of the page.
    """
    return _df("""
        SELECT
            d.soc_major_title,
            f.sector,
            COUNT(*) AS posting_count
        FROM JOBSEEKERS.GOLD.fact_job_postings f
        JOIN JOBSEEKERS.GOLD.dim_occupation d
          ON d.occupation_key = f.occupation_key
        WHERE d.soc_code <> '99-9999'
        GROUP BY d.soc_major_title, f.sector
        ORDER BY posting_count DESC
    """)


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Salary distribution by occupation (boxplot data)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=600)
def salary_by_occupation_top(n: int = 12) -> pd.DataFrame:
    """Per-posting salary rows for the N most-posted occupations. We return
    one row per posting so the UI can render a boxplot with full distribution
    info; n=12 keeps the plot legible.
    """
    return _df(f"""
        WITH top_socs AS (
            SELECT d.soc_code
            FROM JOBSEEKERS.GOLD.fact_job_postings f
            JOIN JOBSEEKERS.GOLD.dim_occupation d
              ON d.occupation_key = f.occupation_key
            WHERE d.soc_code <> '99-9999'
              AND f.salary_midpoint IS NOT NULL
              AND f.salary_midpoint BETWEEN 15000 AND 500000
            GROUP BY d.soc_code
            ORDER BY COUNT(*) DESC
            LIMIT {n}
        )
        SELECT
            d.soc_code,
            d.soc_title,
            f.sector,
            f.salary_midpoint
        FROM JOBSEEKERS.GOLD.fact_job_postings f
        JOIN JOBSEEKERS.GOLD.dim_occupation d
          ON d.occupation_key = f.occupation_key
        WHERE d.soc_code IN (SELECT soc_code FROM top_socs)
          AND f.salary_midpoint IS NOT NULL
          AND f.salary_midpoint BETWEEN 15000 AND 500000
    """)


# ─────────────────────────────────────────────────────────────────────────────
# 4.  QCEW long-history employment trend by state
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=1800)        # 30 min — QCEW barely changes day-to-day
def qcew_trend(state_fips: str = "06", own_label: str = "Private") -> pd.DataFrame:
    """One row per quarter, 1990-present, for the requested state and ownership.
    Used by the long-history line chart. Default to California Private since
    that's the most data-rich combination."""
    return _df(f"""
        SELECT
            year,
            qtr,
            DATE_FROM_PARTS(year, (qtr - 1) * 3 + 1, 1)  AS quarter_date,
            SUM(avg_monthly_emplvl)                       AS state_employment,
            AVG(avg_wkly_wage)                            AS state_avg_wkly_wage
        FROM JOBSEEKERS.GOLD.fact_qcew
        WHERE area_type = 'state'
          AND industry_code = '10'
          AND LEFT(area_fips, 2) = '{state_fips}'
          AND own_label = '{own_label}'
        GROUP BY year, qtr
        ORDER BY year, qtr
    """)


@st.cache_data(ttl=3600)
def qcew_state_options() -> pd.DataFrame:
    """Distinct (state_fips, state_name) pairs for the trend chart's selector.
    State names are hard-coded since QCEW doesn't ship them inline."""
    # FIPS → state name mapping. Keeps a single source of truth in the SQL.
    return _df("""
        WITH fips_names AS (
            SELECT * FROM (VALUES
                ('01','Alabama'),('02','Alaska'),('04','Arizona'),('05','Arkansas'),
                ('06','California'),('08','Colorado'),('09','Connecticut'),('10','Delaware'),
                ('11','D.C.'),('12','Florida'),('13','Georgia'),('15','Hawaii'),
                ('16','Idaho'),('17','Illinois'),('18','Indiana'),('19','Iowa'),
                ('20','Kansas'),('21','Kentucky'),('22','Louisiana'),('23','Maine'),
                ('24','Maryland'),('25','Massachusetts'),('26','Michigan'),('27','Minnesota'),
                ('28','Mississippi'),('29','Missouri'),('30','Montana'),('31','Nebraska'),
                ('32','Nevada'),('33','New Hampshire'),('34','New Jersey'),('35','New Mexico'),
                ('36','New York'),('37','North Carolina'),('38','North Dakota'),('39','Ohio'),
                ('40','Oklahoma'),('41','Oregon'),('42','Pennsylvania'),('44','Rhode Island'),
                ('45','South Carolina'),('46','South Dakota'),('47','Tennessee'),('48','Texas'),
                ('49','Utah'),('50','Vermont'),('51','Virginia'),('53','Washington'),
                ('54','West Virginia'),('55','Wisconsin'),('56','Wyoming')
            ) AS t(fips, name)
        )
        SELECT n.fips AS state_fips, n.name AS state_name
        FROM fips_names n
        WHERE EXISTS (
            SELECT 1 FROM JOBSEEKERS.GOLD.fact_qcew f
            WHERE f.area_type = 'state'
              AND LEFT(f.area_fips, 2) = n.fips
        )
        ORDER BY n.name
    """)


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Top-line counters for the page header
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def headline_counters() -> dict:
    """Fast aggregate counts that anchor the dashboard header."""
    df = _df("""
        SELECT
            (SELECT COUNT(*) FROM JOBSEEKERS.GOLD.fact_job_postings)        AS total_postings,
            (SELECT COUNT(*) FROM JOBSEEKERS.GOLD.fact_qcew)                AS qcew_rows,
            (SELECT COUNT(*) FROM JOBSEEKERS.GOLD.dim_occupation
              WHERE soc_code <> '99-9999')                                  AS soc_codes_mapped,
            (SELECT MAX(year) FROM JOBSEEKERS.GOLD.fact_qcew)               AS latest_qcew_year,
            (SELECT MIN(year) FROM JOBSEEKERS.GOLD.fact_qcew)               AS earliest_qcew_year
    """)
    return df.iloc[0].to_dict()
