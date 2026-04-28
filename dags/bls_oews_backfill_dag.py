"""
OEWS backfill DAG: load 2019-2024 May survey panels into Bronze.

Why 2019+?  The 2019 panel was the first to use the 2018 SOC structure,
which is what our resolver (and 2024 OEWS) speaks. Mixing in 2018-and-earlier
data would require an SOC vintage cross-walk we don't need for course scope.

Each survey year is one task. They run sequentially because the loader needs
to claim disk space for XLSX→CSV conversion before the upload.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.extractors.bls_oews import OEWSExtractor      # noqa: E402
from src.loaders.bls_loader import BLSLoader           # noqa: E402

logger = logging.getLogger(__name__)

BACKFILL_START = 2019
BACKFILL_END = 2024


def load_one_oews_year(year: int, **context) -> int:
    extractor = OEWSExtractor()
    loader    = BLSLoader()

    panel = extractor.fetch_year(year)
    rows  = loader.load_oews_panel(panel.extract_dir, year=year)

    context["ti"].xcom_push(key=f"oews_{year}_rows", value=rows)
    return rows


default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="bls_oews_backfill",
    description="One-shot historical backfill of OEWS annual panels, 2019–2024",
    default_args=default_args,
    start_date=datetime(2026, 4, 26),
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    tags=["jobseekers", "bls", "backfill"],
) as dag:

    previous = None
    for year in range(BACKFILL_START, BACKFILL_END + 1):
        task = PythonOperator(
            task_id=f"load_oews_{year}",
            python_callable=load_one_oews_year,
            op_kwargs={"year": year},
        )
        if previous is not None:
            previous >> task
        previous = task
