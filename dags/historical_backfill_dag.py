"""
Historical USAJobs backfill DAG — one-shot, runs manually via Airflow trigger.

Scope: records posted within the past 30 days from the abigailhaddad/usajobs_historical
parquet dataset.  Chunked processing (5 000 rows/chunk) keeps memory usage low
and Snowflake stage sizes manageable.

Trigger manually:
    airflow dags trigger usajobs_historical_backfill

The DAG is paused by default (schedule=None) so it never runs automatically.
"""
from __future__ import annotations

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

LOOKBACK_DAYS = 30
CHUNK_SIZE    = 5_000

default_args = {
    "owner": "data-eng",
    "retries": 2,
}


# ── task functions ────────────────────────────────────────────────────────────

def extract_and_load_historical(**ctx) -> int:
    """Download parquet, filter to last LOOKBACK_DAYS, bulk-load into Bronze."""
    from src.extractors.usajobs_historical import USAJobsHistoricalExtractor
    from src.loaders.snowflake_loader import SnowflakeLoader

    extractor = USAJobsHistoricalExtractor(lookback_days=LOOKBACK_DAYS)
    loader    = SnowflakeLoader()

    total = 0
    for i, batch in enumerate(extractor.iter_batches(chunk_size=CHUNK_SIZE)):
        n = loader.load_usajobs_historical_raw(batch, source_year=extractor.target_year)
        log.info("Chunk %d: loaded %d rows into Bronze", i + 1, n)
        total += n

    log.info("Historical Bronze load complete — %d total rows", total)
    return total


def transform_historical_to_silver(**ctx) -> int:
    """Run the idempotent MERGE from Bronze historical → Silver."""
    from src.transformers.bronze_to_silver import BronzeToSilverTransformer

    transformer = BronzeToSilverTransformer()
    affected    = transformer.transform_usajobs_historical()
    log.info("Historical Bronze→Silver MERGE affected %d rows", affected)
    return affected


def refresh_gold(**ctx) -> dict[str, int]:
    """Re-run Silver→Gold to incorporate the newly backfilled rows."""
    from src.transformers.silver_to_gold import SilverToGoldTransformer

    results = SilverToGoldTransformer().run_all()
    log.info("Silver→Gold refresh results: %s", results)
    return results


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="usajobs_historical_backfill",
    default_args=default_args,
    description="One-shot backfill: last 30 days from usajobs_historical parquet → Bronze → Silver → Gold",
    schedule=None,          # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["backfill", "usajobs", "historical"],
) as dag:

    t_load = PythonOperator(
        task_id="extract_and_load_to_bronze",
        python_callable=extract_and_load_historical,
    )

    t_silver = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_historical_to_silver,
    )

    t_gold = PythonOperator(
        task_id="refresh_gold",
        python_callable=refresh_gold,
    )

    t_load >> t_silver >> t_gold
