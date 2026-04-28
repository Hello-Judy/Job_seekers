"""
QCEW backfill DAG: bring in the entire 1990–present QCEW archive.

This DAG is a one-shot. You trigger it manually, it walks year by year,
and after roughly 4–8 hours (network-dependent) the BRONZE layer holds
~50 GB / ~600 M rows of NAICS-classified employment and wage data.

Why year-by-year as separate tasks?
    Each task downloads ~150 MB, unzips to ~1.5 GB, PUTs to a Snowflake
    stage, COPYs into Bronze, then deletes the local file. Failure of
    any one year retries independently — we never have to redo the
    whole thing. Airflow's task-level retries make this a much more
    forgiving pattern than a single 50 GB monolithic job.

Stage cleanup:
    The loader removes its own stage directory after a successful COPY,
    so this DAG carries no separate cleanup task.

After this finishes, switch on `bls_qcew_incremental_dag` for ongoing
quarterly updates — that one runs on the BLS release calendar instead
of as a backfill.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.extractors.bls_qcew import QCEWExtractor       # noqa: E402
from src.loaders.bls_loader import BLSLoader            # noqa: E402

logger = logging.getLogger(__name__)

# Years to backfill. NAICS coverage starts at 1990; we cap at 2024 because
# 2025 data drops in pieces through mid-2026 — the incremental DAG will
# pick up new releases once they're available.
BACKFILL_START = 1990
BACKFILL_END = 2024


def load_one_year(year: int, **context) -> int:
    """Download + load a single QCEW year. XCom-pushes the row count."""
    extractor = QCEWExtractor()
    loader    = BLSLoader()

    qcew_file = extractor.fetch_year(year)
    rows = loader.load_qcew_year(qcew_file.csv_path, year=year)

    # Reclaim the ~1.5 GB of local disk so subsequent years have headroom.
    qcew_file.csv_path.unlink(missing_ok=True)

    context["ti"].xcom_push(key=f"qcew_{year}_rows", value=rows)
    return rows


default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 2,                          # network blips are common on long pulls
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="bls_qcew_backfill",
    description="One-shot historical backfill of QCEW NAICS singlefiles, 1990–2024",
    default_args=default_args,
    start_date=datetime(2026, 4, 26),
    schedule=None,                          # manual trigger
    catchup=False,
    max_active_tasks=2,                     # cap parallel disk + PUT bandwidth
    tags=["jobseekers", "bls", "backfill"],
) as dag:

    # One task per year — independent retry, easy to inspect in the UI.
    previous = None
    for year in range(BACKFILL_START, BACKFILL_END + 1):
        task = PythonOperator(
            task_id=f"load_qcew_{year}",
            python_callable=load_one_year,
            op_kwargs={"year": year},
        )
        # Run sequentially: many years in parallel would saturate disk and
        # generate hundreds of GB of staged compressed data simultaneously.
        if previous is not None:
            previous >> task
        previous = task
