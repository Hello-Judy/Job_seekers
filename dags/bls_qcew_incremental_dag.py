"""
QCEW incremental DAG.

After the one-shot backfill completes, this DAG keeps the Bronze layer in
sync with new BLS releases. It runs daily and is cheap when there is nothing
new — `QCEWExtractor.fetch_year` is a no-op when the local CSV is already
present and the loader skips files that have already been COPY'd within
Snowflake's 64-day load history window.

Release cadence (BLS 'County Employment and Wages' calendar):
    Q1 → released early September of the same year
    Q2 → early December
    Q3 → early March of the following year
    Q4 → early June

We always look at the latest two calendar years on each run so revisions
to the previous year are picked up automatically (BLS routinely revises
recent quarters when establishment-level data is reconciled).
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

import requests
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.extractors.bls_qcew import QCEWExtractor       # noqa: E402
from src.loaders.bls_loader import BLSLoader            # noqa: E402

logger = logging.getLogger(__name__)

# Downstream Silver/Gold layers listen on this dataset.
QCEW_BRONZE_DATASET = Dataset("snowflake://BRONZE/raw_bls_qcew")


def load_recent_qcew(**context) -> dict[int, int]:
    """Load the current and previous calendar year from QCEW.

    Returns a {year: rows_loaded} dict for XCom debugging.
    """
    today    = context.get("logical_date") or datetime.utcnow()
    years    = [today.year - 1, today.year]
    ext      = QCEWExtractor()
    loader   = BLSLoader()
    results: dict[int, int] = {}

    for year in years:
        try:
            f = ext.fetch_year(year)
        except requests.HTTPError as exc:
            # Year not yet published — common for the current calendar year
            # in Q1/Q2 before the September release.
            if exc.response is not None and exc.response.status_code == 404:
                logger.warning("QCEW %d not yet published; skipping", year)
                results[year] = 0
                continue
            raise

        rows = loader.load_qcew_year(f.csv_path, year=year)
        f.csv_path.unlink(missing_ok=True)
        results[year] = rows

    context["ti"].xcom_push(key="qcew_incremental_rows", value=results)
    return results


default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="bls_qcew_incremental",
    description="Daily check-and-load for the current/previous QCEW year",
    default_args=default_args,
    start_date=datetime(2026, 4, 26),
    schedule="0 6 * * *",                   # 06:00 UTC daily, after BLS US-East mornings
    catchup=False,
    tags=["jobseekers", "bls", "incremental"],
) as dag:

    PythonOperator(
        task_id="load_recent_qcew_years",
        python_callable=load_recent_qcew,
        outlets=[QCEW_BRONZE_DATASET],      # signals downstream Silver build
    )
