"""
Snowflake query helpers for the Streamlit dashboard.

All public functions return pandas DataFrames and are decorated with
@st.cache_data so repeated renders don't hit Snowflake on every rerun.
The TTL values are tuned to balance freshness vs. query cost:
  - static filter lists  → 10 min
  - trend / feed data    →  5 min
  - search results       → no cache (user-driven, always fresh)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st
import snowflake.connector

# Allow running from both project root and analysis/
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


def _df(sql: str, params: list | None = None) -> pd.DataFrame:
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        cols = [d[0].lower() for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ── static filter options ────────────────────────────────────────────────────

@st.cache_data(ttl=600)
def get_filter_options() -> dict[str, list]:
    states = _df(
        "SELECT DISTINCT location_state AS v FROM SILVER.v_jobs_clean"
        " WHERE location_state IS NOT NULL ORDER BY 1"
    )["v"].tolist()
    categories = _df(
        "SELECT DISTINCT job_category AS v FROM SILVER.v_jobs_clean"
        " WHERE job_category IS NOT NULL ORDER BY 1"
    )["v"].tolist()
    return {"states": states, "categories": categories}


@st.cache_data(ttl=600)
def get_cities_for_states(states: tuple[str, ...]) -> list[str]:
    if not states:
        return []
    placeholders = ",".join(f"'{s.replace(chr(39), chr(39)*2)}'" for s in states)
    return _df(
        f"SELECT DISTINCT location_city AS v FROM SILVER.v_jobs_clean"
        f" WHERE location_state IN ({placeholders}) AND location_city IS NOT NULL ORDER BY 1"
    )["v"].tolist()


# ── dashboard data ────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_summary_stats() -> dict:
    row = _df("""
        SELECT
            COUNT(*)                                                    AS total_jobs,
            COUNT(CASE WHEN sector = 'federal'   THEN 1 END)           AS federal_jobs,
            COUNT(CASE WHEN sector = 'private'   THEN 1 END)           AS private_jobs,
            COUNT(CASE WHEN is_entry_level       THEN 1 END)           AS entry_level_jobs,
            COUNT(CASE WHEN posted_date >= DATEADD('day',-7,CURRENT_DATE()) THEN 1 END)
                                                                        AS jobs_last_7d,
            COUNT(DISTINCT location_state)                              AS states_covered
        FROM SILVER.v_jobs_clean
    """).iloc[0]
    return row.to_dict()


@st.cache_data(ttl=300)
def get_posting_trend(days: int = 30) -> pd.DataFrame:
    return _df(f"""
        SELECT
            DATE_TRUNC('day', posted_date) AS day,
            sector,
            COUNT(*)                       AS job_count
        FROM SILVER.v_jobs_clean
        WHERE posted_date >= DATEADD('day', -{days}, CURRENT_DATE())
          AND posted_date IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)


@st.cache_data(ttl=120)
def get_latest_jobs(
    experience_levels: tuple[str, ...] = (),
    sectors: tuple[str, ...] = (),
    location_states: tuple[str, ...] = (),
    remote_types: tuple[str, ...] = (),
    days: int = 7,
    limit: int = 30,
) -> pd.DataFrame:
    filters = [f"ju.posted_date >= DATEADD('day', -{days}, CURRENT_DATE())"]
    if experience_levels:
        vals = ",".join(f"'{v}'" for v in experience_levels)
        filters.append(f"ju.experience_level IN ({vals})")
    if sectors:
        vals = ",".join(f"'{v}'" for v in sectors)
        filters.append(f"ju.sector IN ({vals})")
    if location_states:
        vals = ",".join(f"'{v}'" for v in location_states)
        filters.append(f"ju.location_state IN ({vals})")
    if remote_types:
        vals = ",".join(f"'{v}'" for v in remote_types)
        filters.append(f"ju.remote_type IN ({vals})")

    where = " AND ".join(filters)
    return _df(f"""
        SELECT
            ju.job_id, ju.source, ju.sector,
            ju.job_title, ju.company_or_agency,
            ju.location_city, ju.location_state,
            ju.salary_min, ju.salary_max,
            ju.experience_level, ju.employment_type, ju.remote_type,
            ju.posted_date, ju.apply_url, ju.job_category,
            COALESCE(dcat.category_group, 'Other') AS category_group
        FROM SILVER.v_jobs_clean ju
        LEFT JOIN GOLD.dim_category dcat ON dcat.category_name = ju.job_category
        WHERE {where}
          AND ju.posted_date IS NOT NULL
        ORDER BY ju.posted_date DESC, ju.ingestion_timestamp DESC
        LIMIT {limit}
    """)


# ── search ─────────────────────────────────────────────────────────────────────

def search_jobs(
    states: list[str],
    cities: list[str],
    categories: list[str],
    experience_levels: list[str],
    sectors: list[str],
    remote_types: list[str],
    date_from: str | None,
    date_to: str | None,
    limit: int = 200,
) -> pd.DataFrame:
    """No cache — results depend on user inputs that change per request."""
    filters = ["1=1"]

    def _in(col: str, vals: list[str]) -> None:
        if vals:
            safe = ",".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in vals)
            filters.append(f"{col} IN ({safe})")

    _in("ju.location_state",   states)
    _in("ju.location_city",    cities)
    _in("ju.job_category",     categories)
    _in("ju.experience_level", experience_levels)
    _in("ju.sector",           sectors)
    _in("ju.remote_type",      remote_types)

    if date_from:
        filters.append(f"ju.posted_date >= DATE '{date_from}'")
    if date_to:
        filters.append(f"ju.posted_date <= DATE '{date_to}'")

    return _df(f"""
        SELECT
            ju.job_id, ju.source, ju.sector,
            ju.job_title, ju.company_or_agency,
            ju.location_city, ju.location_state,
            ju.salary_min, ju.salary_max,
            ju.experience_level, ju.employment_type, ju.remote_type,
            ju.posted_date, ju.close_date, ju.apply_url, ju.job_category,
            COALESCE(dcat.category_group, 'Other') AS category_group
        FROM SILVER.v_jobs_clean ju
        LEFT JOIN GOLD.dim_category dcat ON dcat.category_name = ju.job_category
        WHERE {" AND ".join(filters)}
        ORDER BY ju.posted_date DESC
        LIMIT {limit}
    """)
