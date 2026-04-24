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

        sql = """
            INSERT INTO raw_adzuna
                (job_id, raw_data, search_query, search_country, api_pull_date)
            SELECT
                column1,
                PARSE_JSON(column2),
                column3,
                column4,
                TO_DATE(column5)
            FROM VALUES (%s, %s, %s, %s, %s)
        """
        # Snowflake connector's executemany handles the batching.
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, rows)
                inserted = cursor.rowcount or len(rows)
                conn.commit()
            finally:
                cursor.close()

        logger.info("Loaded %d Adzuna rows into BRONZE.RAW_ADZUNA", inserted)
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
            INSERT INTO raw_usajobs_current
                (control_number, raw_data, api_pull_date)
            SELECT column1, PARSE_JSON(column2), TO_DATE(column3)
            FROM VALUES (%s, %s, %s)
        """
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.executemany(sql, rows)
                conn.commit()
            finally:
                cur.close()

        logger.info(
            "Loaded %d USAJobs Current rows into BRONZE.RAW_USAJOBS_CURRENT",
            len(rows),
        )
        return len(rows)

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
