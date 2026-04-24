"""
Adzuna end-to-end DAG:

    Adzuna API  →  BRONZE.raw_adzuna  →  SILVER.jobs_unified

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

from src.extractors.adzuna import AdzunaExtractor                       # noqa: E402
from src.loaders.snowflake_loader import SnowflakeLoader                 # noqa: E402
from src.transformers.bronze_to_silver import BronzeToSilverTransformer  # noqa: E402

logger = logging.getLogger(__name__)

# Shared logical dataset — Silver jobs_unified table.
# silver_to_gold_dag listens on this; it runs whenever either B→S DAG updates it.
SILVER_DATASET = Dataset("snowflake://SILVER/jobs_unified")

ADZUNA_QUERIES = [
    "software engineer intern",
    "data analyst entry level",
    "junior developer",
    "new grad software",
]


def extract_and_load_adzuna(**context) -> int:
    extractor = AdzunaExtractor()
    loader    = SnowflakeLoader()
    total = 0
    for query in ADZUNA_QUERIES:
        jobs   = extractor.search(query, max_pages=1, max_days_old=7)
        loaded = loader.load_adzuna_raw(jobs, search_query=query)
        total += loaded
        logger.info("Query %r → %d jobs loaded to Bronze", query, loaded)
    context["ti"].xcom_push(key="bronze_rows", value=total)
    return total


def transform_adzuna_to_silver(**context) -> int:
    transformer = BronzeToSilverTransformer()
    affected    = transformer.transform_adzuna()
    context["ti"].xcom_push(key="silver_rows", value=affected)
    return affected


default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="adzuna_bronze_silver_e2e",
    description="Adzuna API → Bronze → Silver  (every 30 min)",
    default_args=default_args,
    start_date=datetime(2026, 4, 21),
    schedule="*/30 * * * *",  # every 30 minutes
    catchup=False,
    tags=["jobseekers", "adzuna"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_load_adzuna_to_bronze",
        python_callable=extract_and_load_adzuna,
    )

    t2 = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_adzuna_to_silver,
        outlets=[SILVER_DATASET],   # signals Silver was updated → triggers Gold DAG
    )

    t1 >> t2
