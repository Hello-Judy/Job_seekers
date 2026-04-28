"""
BLS silver → gold DAG:

    SILVER.dim_occupation_soc  ─►  GOLD.dim_occupation
    SILVER.bls_qcew_quarterly  ─►  GOLD.fact_qcew
    SILVER.bls_oews_annual     ─►  GOLD.fact_oews_wages
                                   GOLD.fact_job_postings.occupation_key  (back-fill FK)

Triggered automatically when bls_bronze_to_silver completes (Dataset-driven).
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.transformers.bls_silver_to_gold import (        # noqa: E402
    BLSSilverToGoldTransformer,
)

logger = logging.getLogger(__name__)

SILVER_BLS_DATASET = Dataset("snowflake://SILVER/bls_silver_layer")


def populate_dim_occupation(**context) -> int:
    return BLSSilverToGoldTransformer().populate_dim_occupation()


def populate_fact_qcew(**context) -> int:
    return BLSSilverToGoldTransformer().populate_fact_qcew()


def populate_fact_oews(**context) -> int:
    return BLSSilverToGoldTransformer().populate_fact_oews()


def update_postings_soc(**context) -> int:
    return BLSSilverToGoldTransformer().update_postings_soc()


default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bls_silver_to_gold",
    description="BLS Silver → Gold star schema — Dataset-triggered",
    default_args=default_args,
    start_date=datetime(2026, 4, 26),
    schedule=[SILVER_BLS_DATASET],
    catchup=False,
    tags=["jobseekers", "bls", "gold"],
) as dag:

    t_dim  = PythonOperator(task_id="populate_dim_occupation",
                            python_callable=populate_dim_occupation)
    t_qcew = PythonOperator(task_id="populate_fact_qcew",
                            python_callable=populate_fact_qcew)
    t_oews = PythonOperator(task_id="populate_fact_oews",
                            python_callable=populate_fact_oews)
    t_fk   = PythonOperator(task_id="update_postings_occupation_key",
                            python_callable=update_postings_soc)

    # dim_occupation must exist before the OEWS fact (which has FK to it)
    # and before the postings FK update (same reason).
    t_dim >> [t_qcew, t_oews, t_fk]
