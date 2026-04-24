"""
USAJobs end-to-end DAG:

    USAJobs API  →  BRONZE.raw_usajobs_current  →  SILVER.jobs_unified

Runs every 4 hours.  The final task declares SILVER_DATASET as an outlet,
which automatically triggers silver_to_gold_star_schema whenever this DAG
produces new Silver rows (Airflow 2.4+ Dataset-aware scheduling).
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.extractors.usajobs import USAJobsExtractor                      # noqa: E402
from src.loaders.snowflake_loader import SnowflakeLoader                  # noqa: E402
from src.transformers.bronze_to_silver import BronzeToSilverTransformer  # noqa: E402

logger = logging.getLogger(__name__)

SILVER_DATASET = Dataset("snowflake://SILVER/jobs_unified")

USAJOBS_QUERIES = [
    "software engineer",
    "data analyst",
    "IT specialist",
    "program analyst",
]


def extract_and_load_usajobs(**context) -> int:
    extractor = USAJobsExtractor()
    loader    = SnowflakeLoader()
    total = 0
    for keyword in USAJOBS_QUERIES:
        jobs   = extractor.search(keyword, max_pages=1, days_posted=7)
        loaded = loader.load_usajobs_current_raw(jobs)
        total += loaded
        logger.info("Query %r → %d jobs loaded to Bronze", keyword, loaded)
    context["ti"].xcom_push(key="bronze_rows", value=total)
    return total


def transform_usajobs_to_silver(**context) -> int:
    transformer = BronzeToSilverTransformer()
    affected    = transformer.transform_usajobs()
    context["ti"].xcom_push(key="silver_rows", value=affected)
    return affected


default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="usajobs_bronze_silver_e2e",
    description="USAJobs API → Bronze → Silver  (every 30 min)",
    default_args=default_args,
    start_date=datetime(2026, 4, 23),
    schedule="*/30 * * * *",  # every 30 minutes
    catchup=False,
    tags=["jobseekers", "usajobs", "federal"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_load_usajobs_to_bronze",
        python_callable=extract_and_load_usajobs,
    )

    t2 = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_usajobs_to_silver,
        outlets=[SILVER_DATASET],   # signals Silver was updated → triggers Gold DAG
    )

    t1 >> t2
