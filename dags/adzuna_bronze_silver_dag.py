"""
Day 1 end-to-end DAG:

    Adzuna API  →  BRONZE.raw_adzuna  →  SILVER.jobs_unified

Goal for the first run: prove the full plumbing works with a single query.
Once this runs green, everything else is "add more sources and more queries."
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make our /opt/airflow/src importable inside the Airflow container.
# (docker-compose.yml mounts ./src -> /opt/airflow/src)
sys.path.insert(0, "/opt/airflow")

from src.extractors.adzuna import AdzunaExtractor       # noqa: E402
from src.loaders.snowflake_loader import SnowflakeLoader  # noqa: E402
from src.transformers.bronze_to_silver import BronzeToSilverTransformer  # noqa: E402

logger = logging.getLogger(__name__)


# ---- Task functions ------------------------------------------------------

# The set of queries we pull. Entry-level focus per the project thesis.
# Each call ≈ 1-2 Adzuna pages, staying well under free-tier quota.
ADZUNA_QUERIES = [
    "software engineer intern",
    "data analyst entry level",
    "junior developer",
    "new grad software",
]


def extract_and_load_adzuna(**context) -> int:
    """Pull each query from Adzuna and land raw JSON in Bronze."""
    extractor = AdzunaExtractor()
    loader = SnowflakeLoader()

    total = 0
    for query in ADZUNA_QUERIES:
        jobs = extractor.search(query, max_pages=1, max_days_old=7)
        loaded = loader.load_adzuna_raw(jobs, search_query=query)
        total += loaded
        logger.info("Query %r → %d jobs loaded to Bronze", query, loaded)

    # Push to XCom so downstream task and UI both see how much we ingested.
    context["ti"].xcom_push(key="bronze_rows", value=total)
    return total


def transform_adzuna_to_silver(**context) -> int:
    """Run the MERGE from BRONZE.raw_adzuna into SILVER.jobs_unified."""
    transformer = BronzeToSilverTransformer()
    affected = transformer.transform_adzuna()
    context["ti"].xcom_push(key="silver_rows", value=affected)
    return affected


# ---- DAG definition ------------------------------------------------------

default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="adzuna_bronze_silver_e2e",
    description="Day 1 end-to-end: Adzuna API → Bronze → Silver",
    default_args=default_args,
    start_date=datetime(2026, 4, 21),
    schedule=None,       # manual trigger for now
    catchup=False,
    tags=["jobseekers", "day-1", "adzuna"],
) as dag:

    t1_extract_load = PythonOperator(
        task_id="extract_load_adzuna_to_bronze",
        python_callable=extract_and_load_adzuna,
    )

    t2_transform = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_adzuna_to_silver,
    )

    t1_extract_load >> t2_transform
