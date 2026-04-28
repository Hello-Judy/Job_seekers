"""
BLS bronze → silver DAG:

    BRONZE.raw_bls_qcew  ─┐
                          ├─► SILVER.bls_qcew_quarterly
                          │     SILVER.bls_oews_annual
    BRONZE.raw_bls_oews  ─┘     SILVER.dim_occupation_soc

    SILVER.jobs_unified  ───►  SILVER.dim_occupation_soc  (SOC resolver)

Triggered by either Bronze dataset emitting an update, OR by the existing
Adzuna/USAJobs DAGs updating jobs_unified — the resolver re-runs whenever
new postings arrive so its coverage stays current.

Outlets the SILVER_BLS dataset, which the next DAG (silver→gold) listens on.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src.transformers.bls_bronze_to_silver import (    # noqa: E402
    BLSBronzeToSilverTransformer,
)

logger = logging.getLogger(__name__)


# Inbound: triggered by either Bronze source.
QCEW_BRONZE  = Dataset("snowflake://BRONZE/raw_bls_qcew")
OEWS_BRONZE  = Dataset("snowflake://BRONZE/raw_bls_oews")
JOBS_SILVER  = Dataset("snowflake://SILVER/jobs_unified")

# Outbound: BLS Silver layer is updated.
SILVER_BLS_DATASET = Dataset("snowflake://SILVER/bls_silver_layer")


def transform_qcew(**context) -> int:
    rows = BLSBronzeToSilverTransformer().transform_qcew()
    context["ti"].xcom_push(key="qcew_silver_rows", value=rows)
    return rows


def transform_oews(**context) -> int:
    rows = BLSBronzeToSilverTransformer().transform_oews()
    context["ti"].xcom_push(key="oews_silver_rows", value=rows)
    return rows


def resolve_soc(**context) -> int:
    rows = BLSBronzeToSilverTransformer().resolve_soc()
    context["ti"].xcom_push(key="soc_resolver_rows", value=rows)
    return rows


default_args = {
    "owner": "jobseekers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="bls_bronze_to_silver",
    description="QCEW/OEWS Bronze → Silver + SOC resolver — Dataset-triggered",
    default_args=default_args,
    start_date=datetime(2026, 4, 26),
    schedule=[QCEW_BRONZE, OEWS_BRONZE, JOBS_SILVER],   # any of the three updates
    catchup=False,
    tags=["jobseekers", "bls", "silver"],
) as dag:

    t_qcew = PythonOperator(
        task_id="transform_qcew_to_silver",
        python_callable=transform_qcew,
    )
    t_oews = PythonOperator(
        task_id="transform_oews_to_silver",
        python_callable=transform_oews,
    )
    t_soc = PythonOperator(
        task_id="resolve_soc_codes",
        python_callable=resolve_soc,
        outlets=[SILVER_BLS_DATASET],     # signals the Gold DAG
    )

    # SOC resolver depends only on jobs_unified, but we serialize behind the
    # two BLS transforms so a single DAG run is a coherent "Silver refresh".
    [t_qcew, t_oews] >> t_soc
