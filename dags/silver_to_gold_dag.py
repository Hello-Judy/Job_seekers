"""
Silver → Gold DAG:

    SILVER.jobs_unified  →  GOLD star schema
        dim_date  ─┐
        dim_location ─┤
        dim_company  ─┼──► fact_job_postings
        dim_category ─┘

The four dimension tasks run in parallel; the fact task waits for all of them.
This DAG is meant to run after both Bronze→Silver DAGs have completed.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.transformers.silver_to_gold import SilverToGoldTransformer  # noqa: E402

logger = logging.getLogger(__name__)


# ---- Task functions --------------------------------------------------------

def _push(context, key: str, value: int) -> int:
    context["ti"].xcom_push(key=key, value=value)
    return value


def populate_dim_date(**context) -> int:
    rows = SilverToGoldTransformer().populate_dim_date()
    return _push(context, "dim_date_rows", rows)


def populate_dim_location(**context) -> int:
    rows = SilverToGoldTransformer().populate_dim_location()
    return _push(context, "dim_location_rows", rows)


def populate_dim_company(**context) -> int:
    rows = SilverToGoldTransformer().populate_dim_company()
    return _push(context, "dim_company_rows", rows)


def populate_dim_category(**context) -> int:
    rows = SilverToGoldTransformer().populate_dim_category()
    return _push(context, "dim_category_rows", rows)


def populate_fact_job_postings(**context) -> int:
    rows = SilverToGoldTransformer().populate_fact_job_postings()
    return _push(context, "fact_rows", rows)


# ---- DAG definition --------------------------------------------------------

default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="silver_to_gold_star_schema",
    description="Refresh GOLD star schema from SILVER.jobs_unified",
    default_args=default_args,
    start_date=datetime(2026, 4, 23),
    schedule=None,      # trigger manually or chain after both B→S DAGs
    catchup=False,
    tags=["jobseekers", "gold", "star-schema"],
) as dag:

    t_dim_date = PythonOperator(
        task_id="populate_dim_date",
        python_callable=populate_dim_date,
    )

    t_dim_location = PythonOperator(
        task_id="populate_dim_location",
        python_callable=populate_dim_location,
    )

    t_dim_company = PythonOperator(
        task_id="populate_dim_company",
        python_callable=populate_dim_company,
    )

    t_dim_category = PythonOperator(
        task_id="populate_dim_category",
        python_callable=populate_dim_category,
    )

    t_fact = PythonOperator(
        task_id="populate_fact_job_postings",
        python_callable=populate_fact_job_postings,
    )

    # Four dims in parallel, then fact depends on all four.
    [t_dim_date, t_dim_location, t_dim_company, t_dim_category] >> t_fact
