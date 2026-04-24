"""
Snowflake loader.

Uses the official snowflake-connector-python with its pandas helper for fast bulk
loads. For small batches (< a few thousand rows) we use INSERT ... SELECT from
PARSE_JSON; for larger ones we use write_pandas.

Usage:
    from src.loaders.snowflake_loader import SnowflakeLoader
    loader = SnowflakeLoader()
    loader.load_adzuna_raw(jobs, search_query="data analyst")
"""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import date
from typing import Iterator

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.utils.config import settings, require

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Typed wrapper around the bronze-layer inserts we need."""

    def __init__(self) -> None:
        require(
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD",
        )

    # ---------- connection ----------

    @contextmanager
    def _connect(self) -> Iterator[SnowflakeConnection]:
        conn = snowflake.connector.connect(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            role=settings.SNOWFLAKE_ROLE,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA_BRONZE,
        )
        try:
            yield conn
        finally:
            conn.close()

    # ---------- bronze loaders ----------

    def load_adzuna_raw(
        self,
        jobs: list[dict],
        *,
        search_query: str,
        search_country: str | None = None,
    ) -> int:
        """Insert raw Adzuna jobs into BRONZE.RAW_ADZUNA. Returns row count."""
        if not jobs:
            logger.warning("load_adzuna_raw called with empty jobs list")
            return 0

        country = search_country or settings.ADZUNA_COUNTRY
        pull_date = date.today().isoformat()

        # Snowflake trick: VARIANT columns accept JSON via PARSE_JSON.
        # We pass each payload as a string, then let the server parse it.
        rows = [
            (
                str(job.get("id")),
                json.dumps(job),
                search_query,
                country,
                pull_date,
            )
            for job in jobs
        ]

        # MERGE instead of INSERT so repeated DAG runs and overlapping search
        # queries don't produce duplicate bronze rows for the same job_id.
        # WHEN NOT MATCHED only: bronze is append-safe but not append-only.
        sql = """
            MERGE INTO raw_adzuna AS tgt
            USING (
                SELECT column1 AS job_id, PARSE_JSON(column2) AS raw_data,
                       column3 AS search_query, column4 AS search_country,
                       TO_DATE(column5) AS api_pull_date
                FROM VALUES (%s, %s, %s, %s, %s)
            ) AS src
            ON tgt.job_id = src.job_id
            WHEN NOT MATCHED THEN INSERT
                (job_id, raw_data, search_query, search_country, api_pull_date)
            VALUES
                (src.job_id, src.raw_data, src.search_query, src.search_country, src.api_pull_date)
        """
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, rows)
                inserted = cursor.rowcount or 0
                conn.commit()
            finally:
                cursor.close()

        logger.info(
            "Upserted %d new Adzuna rows into BRONZE.RAW_ADZUNA (%d skipped as duplicates)",
            inserted, len(rows) - inserted,
        )
        return inserted

    def load_usajobs_current_raw(self, jobs: list[dict]) -> int:
        """Insert raw USAJobs Current API results into BRONZE.RAW_USAJOBS_CURRENT."""
        if not jobs:
            return 0
        pull_date = date.today().isoformat()

        rows = []
        for job in jobs:
            # USAJobs wraps real data inside MatchedObjectDescriptor; control
            # number lives at the top level as 'MatchedObjectId'.
            control_number = str(job.get("MatchedObjectId", ""))
            rows.append(
                (control_number, json.dumps(job), pull_date)
            )

        sql = """
            MERGE INTO raw_usajobs_current AS tgt
            USING (
                SELECT column1 AS control_number, PARSE_JSON(column2) AS raw_data,
                       TO_DATE(column3) AS api_pull_date
                FROM VALUES (%s, %s, %s)
            ) AS src
            ON tgt.control_number = src.control_number
            WHEN NOT MATCHED THEN INSERT (control_number, raw_data, api_pull_date)
            VALUES (src.control_number, src.raw_data, src.api_pull_date)
        """
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.executemany(sql, rows)
                inserted = cur.rowcount or 0
                conn.commit()
            finally:
                cur.close()

        logger.info(
            "Upserted %d new USAJobs rows into BRONZE.RAW_USAJOBS_CURRENT (%d skipped as duplicates)",
            inserted, len(rows) - inserted,
        )
        return inserted

    # ---------- util ----------

    def execute_sql_file(self, path: str) -> None:
        """Run every statement in a .sql file (split on `;`).

        Good for initial DDL. Not a full SQL parser — don't put stored
        procedures or $$-quoted blocks in files you call this on.
        """
        with open(path, "r", encoding="utf-8") as f:
            script = f.read()

        statements = [s.strip() for s in script.split(";") if s.strip()]
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                for stmt in statements:
                    logger.info("Executing: %s", stmt[:80].replace("\n", " "))
                    cur.execute(stmt)
                conn.commit()
            finally:
                cur.close()
