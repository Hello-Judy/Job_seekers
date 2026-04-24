"""
Silver data-quality and deduplication checks.

Run before building the Streamlit dashboard (or as a DAG task after
Silver updates) to surface data problems early.

Usage:
    from src.analysis.quality import QualityChecker
    qc = QualityChecker()
    report = qc.run()
    qc.assert_no_pk_violations()   # raises if Silver PK is broken
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.utils.config import settings, require

logger = logging.getLogger(__name__)


class QualityChecker:
    """Queries Silver dedup/quality views and returns structured reports."""

    def __init__(self) -> None:
        require("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")

    @contextmanager
    def _connect(self) -> Iterator[SnowflakeConnection]:
        conn = snowflake.connector.connect(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            role=settings.SNOWFLAKE_ROLE,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
        )
        try:
            yield conn
        finally:
            conn.close()

    def _query(self, sql: str) -> list[dict]:
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                cols = [d[0].lower() for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            finally:
                cur.close()

    # ---------- individual checks ----------

    def pk_violations(self) -> list[dict]:
        """Return rows in Silver where (source, job_id) is not unique.
        An empty list means the MERGE PK constraint is holding."""
        rows = self._query("SELECT * FROM SILVER.v_pk_violations")
        if rows:
            logger.warning("PK violations found: %d groups", len(rows))
        else:
            logger.info("PK check passed: no violations")
        return rows

    def near_duplicates(self, limit: int = 50) -> list[dict]:
        """Return top near-duplicate candidates (same title+company+state+date)."""
        rows = self._query(
            f"SELECT * FROM SILVER.v_near_duplicates LIMIT {limit}"
        )
        logger.info("Near-duplicate check: %d groups found (showing up to %d)", len(rows), limit)
        return rows

    def quality_summary(self) -> list[dict]:
        """Return null-rate and classification stats per source."""
        rows = self._query("SELECT * FROM SILVER.v_quality_summary ORDER BY source")
        for r in rows:
            logger.info(
                "Source %-20s  total=%6d  null_salary=%.0f%%  null_state=%.0f%%  unknown_remote=%.0f%%",
                r.get("source", "?"),
                r.get("total_rows", 0),
                (r.get("null_salary_min", 0) or 0) * 100 / max(r.get("total_rows", 1), 1),
                (r.get("null_state", 0) or 0) * 100 / max(r.get("total_rows", 1), 1),
                (r.get("unknown_remote", 0) or 0) * 100 / max(r.get("total_rows", 1), 1),
            )
        return rows

    def silver_row_counts(self) -> dict[str, int]:
        """Total rows in Silver by source."""
        rows = self._query(
            "SELECT source, COUNT(*) AS n FROM SILVER.jobs_unified GROUP BY source ORDER BY source"
        )
        counts = {r["source"]: r["n"] for r in rows}
        logger.info("Silver row counts: %s", counts)
        return counts

    # ---------- composite ----------

    def run(self) -> dict:
        """Run all checks and return a summary dict."""
        return {
            "row_counts":       self.silver_row_counts(),
            "pk_violations":    self.pk_violations(),
            "near_duplicates":  self.near_duplicates(),
            "quality_summary":  self.quality_summary(),
        }

    def assert_no_pk_violations(self) -> None:
        """Raise RuntimeError if Silver has any (source, job_id) duplicates."""
        violations = self.pk_violations()
        if violations:
            raise RuntimeError(
                f"Silver PK violations detected ({len(violations)} groups). "
                "Run SELECT * FROM SILVER.v_pk_violations for details."
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    report = QualityChecker().run()
    print(f"\nRow counts: {report['row_counts']}")
    print(f"PK violations: {len(report['pk_violations'])}")
    print(f"Near-duplicate groups: {len(report['near_duplicates'])}")
