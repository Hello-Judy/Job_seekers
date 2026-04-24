"""
USAJobs end-to-end DAG:

    USAJobs API  →  BRONZE.raw_usajobs_current  →  SILVER.jobs_unified
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.extractors.usajobs import USAJobsExtractor          # noqa: E402
from src.loaders.snowflake_loader import SnowflakeLoader      # noqa: E402
from src.transformers.bronze_to_silver import BronzeToSilverTransformer  # noqa: E402

logger = logging.getLogger(__name__)


# Entry-level / new-grad focused queries matching the project thesis.
USAJOBS_QUERIES = [
    "software engineer",
    "data analyst",
    "IT specialist",
    "program analyst",
]


# ---- Task functions --------------------------------------------------------

def extract_and_load_usajobs(**context) -> int:
    """Pull each query from USAJobs and land raw JSON in Bronze."""
    extractor = USAJobsExtractor()
    loader = SnowflakeLoader()

    total = 0
    for keyword in USAJOBS_QUERIES:
        jobs = extractor.search(keyword, max_pages=1, days_posted=7)
        loaded = loader.load_usajobs_current_raw(jobs)
        total += loaded
        logger.info("Query %r → %d jobs loaded to Bronze", keyword, loaded)

    context["ti"].xcom_push(key="bronze_rows", value=total)
    return total


def transform_usajobs_to_silver(**context) -> int:
    """Run the MERGE from BRONZE.raw_usajobs_current into SILVER.jobs_unified."""
    transformer = BronzeToSilverTransformer()
    affected = transformer.transform_usajobs()
    context["ti"].xcom_push(key="silver_rows", value=affected)
    return affected


# ---- DAG definition --------------------------------------------------------

default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="usajobs_bronze_silver_e2e",
    description="USAJobs API → Bronze → Silver",
    default_args=default_args,
    start_date=datetime(2026, 4, 23),
    schedule=None,      # manual trigger; set to "0 6 * * *" for daily 6 AM
    catchup=False,
    tags=["jobseekers", "usajobs", "federal"],
) as dag:

    t1_extract_load = PythonOperator(
        task_id="extract_load_usajobs_to_bronze",
        python_callable=extract_and_load_usajobs,
    )

    t2_transform = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_usajobs_to_silver,
    )

    t1_extract_load >> t2_transform
