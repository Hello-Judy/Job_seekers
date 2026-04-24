"""
USAJobs historical dataset extractor.

Source: abigailhaddad/usajobs_historical on GitHub
  — parquet files published per year at:
    https://github.com/abigailhaddad/usajobs_historical/raw/main/data/<year>.parquet

Only records posted within `lookback_days` of today are yielded, keeping the
backfill window small (default 30 days) to avoid loading millions of stale rows.

Usage:
    extractor = USAJobsHistoricalExtractor(lookback_days=30)
    for batch in extractor.iter_batches(chunk_size=5000):
        loader.load_usajobs_historical_raw(batch, source_year=extractor.target_year)
"""
from __future__ import annotations

import io
import logging
from datetime import date, timedelta
from typing import Iterator

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Base URL template; fill in the year.
_BASE_URL = (
    "https://github.com/abigailhaddad/usajobs_historical"
    "/raw/main/data/{year}.parquet"
)

# Column names as they appear in the dataset (most common variants).
# We normalise to our internal names after loading.
_COL_ALIASES: dict[str, list[str]] = {
    "position_id":    ["PositionID", "position_id", "job_id"],
    "job_title":      ["PositionTitle", "position_title", "job_title"],
    "agency":         ["OrganizationName", "organization_name", "AgencyName"],
    "department":     ["DepartmentName", "department_name"],
    "job_category":   ["JobFamily", "JobSeries", "job_category", "JobCategoryCode"],
    "location":       ["PositionLocationDisplay", "PositionLocation", "location"],
    "state":          ["StateAbbrv", "state_abbr", "LocationStateAbbreviation"],
    "city":           ["CityDisplay", "city", "LocationCity"],
    "country":        ["CountryCode", "country_code"],
    "salary_min":     ["SalaryMin", "salary_min", "MinimumRange"],
    "salary_max":     ["SalaryMax", "salary_max", "MaximumRange"],
    "salary_uom":     ["SalaryUOM", "salary_uom", "RateIntervalCode"],
    "posted_date":    ["PositionStartDate", "position_start_date", "PublicationStartDate"],
    "close_date":     ["PositionEndDate", "position_end_date", "ApplicationCloseDate"],
    "schedule":       ["WorkSchedule", "work_schedule", "PositionSchedule"],
    "remote":         ["RemoteIndicator", "remote_indicator", "TeleworkEligible"],
    "apply_url":      ["ApplyOnlineURL", "apply_url", "ApplyURI"],
}


def _pick_col(df: pd.DataFrame, aliases: list[str]) -> str | None:
    """Return the first alias that exists as a column in df, or None."""
    for a in aliases:
        if a in df.columns:
            return a
    return None


class USAJobsHistoricalExtractor:
    """
    Downloads a single year's parquet file and yields records posted within
    the specified lookback window.

    Args:
        lookback_days:  Only keep records posted this many days ago or later.
                        Default 30 limits the backfill to roughly one month.
        year:           Which year's parquet to load. Defaults to current year.
        parquet_url:    Override the auto-constructed URL (e.g. for a local path).
    """

    def __init__(
        self,
        lookback_days: int = 30,
        year: int | None = None,
        parquet_url: str | None = None,
    ) -> None:
        self.lookback_days = lookback_days
        self.target_year   = year or date.today().year
        self._url          = parquet_url or _BASE_URL.format(year=self.target_year)
        self._cutoff        = date.today() - timedelta(days=lookback_days)

    # ── download ────────────────────────────────────────────────────────────

    def _download(self) -> pd.DataFrame:
        logger.info("Downloading historical parquet from %s", self._url)
        resp = requests.get(self._url, timeout=120)
        resp.raise_for_status()
        df = pd.read_parquet(io.BytesIO(resp.content))
        logger.info("Downloaded %d rows (year=%d)", len(df), self.target_year)
        return df

    # ── normalise ───────────────────────────────────────────────────────────

    def _normalise(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename dataset-specific column names to our internal schema."""
        rename: dict[str, str] = {}
        for internal, aliases in _COL_ALIASES.items():
            src = _pick_col(df, aliases)
            if src and src != internal:
                rename[src] = internal
        df = df.rename(columns=rename)

        # Parse posted_date; drop rows with no date or before cutoff.
        if "posted_date" in df.columns:
            df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce").dt.date
            df = df[df["posted_date"].notna()]
            df = df[df["posted_date"] >= self._cutoff]
        else:
            logger.warning("No posted_date column found; keeping all rows")

        # Annualise salary when a rate interval is available.
        if "salary_min" in df.columns and "salary_uom" in df.columns:
            df["salary_min"] = df.apply(
                lambda r: _annualise(r["salary_min"], r.get("salary_uom")), axis=1
            )
        if "salary_max" in df.columns and "salary_uom" in df.columns:
            df["salary_max"] = df.apply(
                lambda r: _annualise(r["salary_max"], r.get("salary_uom")), axis=1
            )

        # Ensure position_id is a string (some years have int IDs).
        if "position_id" in df.columns:
            df["position_id"] = df["position_id"].astype(str)

        return df.reset_index(drop=True)

    # ── public interface ────────────────────────────────────────────────────

    def load(self) -> pd.DataFrame:
        """Download and return the normalised, date-filtered DataFrame."""
        df = self._download()
        df = self._normalise(df)
        logger.info(
            "After filtering to last %d days: %d rows remain",
            self.lookback_days, len(df),
        )
        return df

    def iter_batches(self, chunk_size: int = 5_000) -> Iterator[list[dict]]:
        """Yield row dicts in batches of chunk_size for bulk loading."""
        df = self.load()
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            yield chunk.where(chunk.notna(), other=None).to_dict(orient="records")


# ── helpers ──────────────────────────────────────────────────────────────────

def _annualise(value: float | None, uom: str | None) -> float | None:
    """Convert salary to annual equivalent given a rate interval code."""
    if value is None or pd.isna(value):
        return None
    uom = (uom or "PA").upper().strip()
    if uom == "PH":
        return value * 2080
    if uom == "PB":   # bi-weekly
        return value * 26
    return value      # PA (per annum) or unknown → keep as-is
