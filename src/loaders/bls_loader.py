"""
BLS loader — bulk-loads QCEW and OEWS files into Snowflake's BRONZE schema.

Why a separate loader from `snowflake_loader.SnowflakeLoader`?
    SnowflakeLoader handles small JSON payloads (one Adzuna posting per row)
    via PARSE_JSON inside MERGE statements. That pattern is convenient but
    slow at scale: each row carries a ~3 KB payload and the parse runs in
    the Python client's transaction.

    QCEW singlefiles are 1–2 GB each; we need PUT + COPY INTO so the file
    is uploaded *once* and parsed in parallel by Snowflake's compute.

Pattern
-------
    1. PUT file://... @stg_bls_qcew/{year}/                   (client uploads)
    2. COPY INTO raw_bls_qcew FROM @stg_bls_qcew/{year}/      (server parses)
    3. REMOVE @stg_bls_qcew/{year}/                           (free stage space)

Idempotency is enforced at the COPY step via `ON_ERROR = ABORT_STATEMENT` and
a load-history check: if the same staged file has already been COPY'd in the
last 64 days, Snowflake silently skips it. For backfills older than that
window, we add an explicit `force = TRUE` knob on the loader.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import pandas as pd
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.utils.config import settings, require

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# COPY templates
# ---------------------------------------------------------------------------
# QCEW singlefile column order from the BLS layout doc:
#   area_fips, own_code, industry_code, agglvl_code, size_code, year, qtr,
#   disclosure_code, qtrly_estabs, month1_emplvl, month2_emplvl, month3_emplvl,
#   total_qtrly_wages, taxable_qtrly_wages, qtrly_contributions, avg_wkly_wage,
#   ... [over-the-year change cols we don't store typed] ...
#
# We materialize raw_record as OBJECT_CONSTRUCT() of the typed columns so the
# Bronze contract still says "every row carries its full payload".

COPY_QCEW_SQL = """
COPY INTO raw_bls_qcew (
    area_fips, own_code, industry_code, agglvl_code, size_code,
    year, qtr, disclosure_code,
    qtrly_estabs, month1_emplvl, month2_emplvl, month3_emplvl,
    total_qtrly_wages, taxable_qtrly_wages, qtrly_contributions, avg_wkly_wage,
    raw_record, source_file
)
FROM (
    SELECT
        $1, $2, $3, $4, $5,
        $6, $7, $8,
        TRY_TO_NUMBER($9),  TRY_TO_NUMBER($10), TRY_TO_NUMBER($11), TRY_TO_NUMBER($12),
        TRY_TO_DOUBLE($13), TRY_TO_DOUBLE($14), TRY_TO_DOUBLE($15), TRY_TO_DOUBLE($16),
        OBJECT_CONSTRUCT(
            'area_fips', $1, 'own_code', $2, 'industry_code', $3,
            'agglvl_code', $4, 'size_code', $5, 'year', $6, 'qtr', $7,
            'disclosure_code', $8, 'qtrly_estabs', $9,
            'month1_emplvl', $10, 'month2_emplvl', $11, 'month3_emplvl', $12,
            'total_qtrly_wages', $13, 'taxable_qtrly_wages', $14,
            'qtrly_contributions', $15, 'avg_wkly_wage', $16
        ),
        METADATA$FILENAME
    FROM @stg_bls_qcew/{year}/
)
FILE_FORMAT = (FORMAT_NAME = ff_csv_bls_qcew)
ON_ERROR    = ABORT_STATEMENT
"""

# OEWS bronze loading.
#
# Strategy: convert each XLSX to NDJSON in Python (one JSON object per row,
# with header names as keys), upload the NDJSON, COPY into a VARIANT column.
# This sidesteps Snowflake's restriction that MATCH_BY_COLUMN_NAME can't be
# combined with column transforms (we need to also populate survey_year),
# and is fully robust to BLS reordering or renaming columns in future panels.
COPY_OEWS_SQL = """
COPY INTO raw_bls_oews (record, survey_year, source_file)
FROM (
    SELECT
        $1,
        {year},
        METADATA$FILENAME
    FROM @stg_bls_oews/{year}/
)
FILE_FORMAT = (FORMAT_NAME = ff_json_bls_oews)
ON_ERROR    = CONTINUE
"""

# Kept as a no-op so the loader's call site stays unchanged after this fix.
OEWS_POSTLOAD_FILL_SQL = """SELECT 1"""


class BLSLoader:
    """PUT + COPY INTO for the BLS Bronze tables."""

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
            schema=settings.SNOWFLAKE_SCHEMA_BRONZE,
        )
        try:
            yield conn
        finally:
            conn.close()

    # ---------- QCEW ----------

    def load_qcew_year(self, csv_path: Path, *, year: int) -> int:
        """PUT the singlefile CSV to the year-namespaced stage path, then COPY.

        Returns the number of rows loaded (according to Snowflake's load
        metadata). For the typical 2024 file this is on the order of 12–15 M.
        """
        if not csv_path.exists():
            raise FileNotFoundError(csv_path)

        size_mb = csv_path.stat().st_size / (1 << 20)
        logger.info("Loading QCEW %d into Snowflake (%.1f MB)", year, size_mb)

        put_sql = (
            f"PUT file://{csv_path.resolve()} @stg_bls_qcew/{year}/"
            f" AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        copy_sql = COPY_QCEW_SQL.format(year=year)

        with self._connect() as conn:
            cur = conn.cursor()
            try:
                logger.info("PUT → @stg_bls_qcew/%d/", year)
                cur.execute(put_sql)

                logger.info("COPY → raw_bls_qcew (year=%d)", year)
                cur.execute(copy_sql)
                # last query's row count = rows loaded
                rows_loaded = cur.rowcount or 0

                # Tidy up so we don't pay to keep the staged copy around.
                cur.execute(f"REMOVE @stg_bls_qcew/{year}/")
                conn.commit()
            finally:
                cur.close()

        logger.info("QCEW %d loaded: %s rows into BRONZE.raw_bls_qcew",
                    year, f"{rows_loaded:,}")
        return rows_loaded

    # ---------- OEWS ----------

    def load_oews_panel(self, panel_dir: Path, *, year: int) -> int:
        """Convert each XLSX in `panel_dir` to CSV, PUT, COPY.

        We only load the *area-level* workbooks (national, state, MSA) — those
        are the ones whose columns are stable across years. Industry-specific
        workbooks have variable column layouts and are out of scope for Bronze.
        """
        target_xlsx = self._select_oews_workbooks(panel_dir)
        if not target_xlsx:
            logger.warning("No OEWS workbooks found in %s — skipping load", panel_dir)
            return 0

        # Convert XLSX → NDJSON in a sibling dir so we can stage the entire dir.
        ndjson_dir = panel_dir / "_ndjson"
        ndjson_dir.mkdir(exist_ok=True)
        for xlsx in target_xlsx:
            self._xlsx_to_ndjson(xlsx, ndjson_dir)

        copy_sql = COPY_OEWS_SQL.format(year=year)
        rows_loaded = 0
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                # PUT one shot — wildcards are supported.
                cur.execute(
                    f"PUT 'file://{ndjson_dir.resolve()}/*.ndjson' @stg_bls_oews/{year}/"
                    f" AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                )
                cur.execute(copy_sql)
                rows_loaded = cur.rowcount or 0
                cur.execute(f"REMOVE @stg_bls_oews/{year}/")
                conn.commit()
            finally:
                cur.close()

        logger.info("OEWS %d loaded: %s rows into BRONZE.raw_bls_oews",
                    year, f"{rows_loaded:,}")
        return rows_loaded

    # ---------- helpers ----------

    @staticmethod
    def _select_oews_workbooks(panel_dir: Path) -> list[Path]:
        # OEWS naming has shifted across panels. Known shapes:
        #   pre-2024: 'oesm{YY}nat.xlsx', 'oesm{YY}st.xlsx',
        #             'MSA_M{YYYY}_dl.xlsx', 'BOS_M{YYYY}_dl.xlsx', 'natsector_M*'
        #   2024+   : 'all_data_M_{YYYY}.xlsx' (single consolidated workbook)
        # Whitelist all of these so future panels keep working as BLS evolves
        # the layout.
        wanted_prefixes = (
            "oesm", "MSA_M", "BOS_M", "natsector", "national_M",
            "all_data_M",
        )
        return [
            p for p in panel_dir.glob("*.xlsx")
            if p.name.startswith(wanted_prefixes) and not p.name.startswith("~")
        ]

    @staticmethod
    def _xlsx_to_ndjson(xlsx_path: Path, out_dir: Path) -> Path:
        """Convert one OEWS workbook to NDJSON (one JSON object per row,
        with header names as keys). Suppression markers and BLS-specific
        sentinels stay as strings — silver-layer SQL parses them.

        Why NDJSON instead of CSV:
            Snowflake's MATCH_BY_COLUMN_NAME (the cleanest way to load CSVs
            by header name) cannot be combined with column transforms, and
            we need to set survey_year and source_file at COPY time.
            NDJSON sidesteps this entirely — each row IS a VARIANT object,
            so no header-to-column mapping is needed.
        """
        out = out_dir / (xlsx_path.stem + ".ndjson")
        if out.exists():
            return out
        df = pd.read_excel(xlsx_path, sheet_name=0, dtype=str, engine="openpyxl")
        # to_json with lines=True writes one JSON object per line — exactly
        # the NDJSON shape Snowflake's JSON file format expects.
        df.to_json(out, orient="records", lines=True, force_ascii=False)
        logger.info("Converted %s → %s (%d rows, NDJSON)",
                    xlsx_path.name, out.name, len(df))
        return out


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    if len(sys.argv) < 3 or sys.argv[1] not in ("qcew", "oews"):
        print("Usage: python -m src.loaders.bls_loader qcew /path/to/file.csv 2024")
        print("       python -m src.loaders.bls_loader oews /path/to/panel_dir 2024")
        sys.exit(2)
    kind, path, year = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3])
    if kind == "qcew":
        BLSLoader().load_qcew_year(path, year=year)
    else:
        BLSLoader().load_oews_panel(path, year=year)
