"""
Silver → Gold DAG:

    SILVER.jobs_unified  →  GOLD star schema
        dim_date     ─┐
        dim_location  ─┤
        dim_company   ─┼──► fact_job_postings
        dim_category  ─┘

Triggered automatically by Airflow's Dataset mechanism whenever either
adzuna_bronze_silver_e2e or usajobs_bronze_silver_e2e writes new rows to
SILVER.jobs_unified — no fixed schedule needed.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.transformers.silver_to_gold import SilverToGoldTransformer  # noqa: E402

logger = logging.getLogger(__name__)

SILVER_DATASET = Dataset("snowflake://SILVER/jobs_unified")


def _push(context, key: str, value: int) -> int:
    context["ti"].xcom_push(key=key, value=value)
    return value


def populate_dim_date(**context) -> int:
    return _push(context, "dim_date_rows", SilverToGoldTransformer().populate_dim_date())


def populate_dim_location(**context) -> int:
    return _push(context, "dim_location_rows", SilverToGoldTransformer().populate_dim_location())


def populate_dim_company(**context) -> int:
    return _push(context, "dim_company_rows", SilverToGoldTransformer().populate_dim_company())


def populate_dim_category(**context) -> int:
    return _push(context, "dim_category_rows", SilverToGoldTransformer().populate_dim_category())


def populate_fact_job_postings(**context) -> int:
    return _push(context, "fact_rows", SilverToGoldTransformer().populate_fact_job_postings())


default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_to_gold_star_schema",
    description="Refresh GOLD star schema — triggered by Silver Dataset updates",
    default_args=default_args,
    start_date=datetime(2026, 4, 23),
    schedule=[SILVER_DATASET],   # runs whenever any upstream DAG emits this dataset
    catchup=False,
    tags=["jobseekers", "gold", "star-schema"],
) as dag:

    t_dim_date     = PythonOperator(task_id="populate_dim_date",          python_callable=populate_dim_date)
    t_dim_location = PythonOperator(task_id="populate_dim_location",      python_callable=populate_dim_location)
    t_dim_company  = PythonOperator(task_id="populate_dim_company",       python_callable=populate_dim_company)
    t_dim_category = PythonOperator(task_id="populate_dim_category",      python_callable=populate_dim_category)
    t_fact         = PythonOperator(task_id="populate_fact_job_postings",  python_callable=populate_fact_job_postings)

    [t_dim_date, t_dim_location, t_dim_company, t_dim_category] >> t_fact
