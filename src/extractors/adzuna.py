"""
Adzuna API extractor.

Docs: https://developer.adzuna.com/docs/search

Adzuna returns up to 50 results per page. Free-tier limit is 1000 calls/month,
so we plan queries carefully.

Usage:
    from src.extractors.adzuna import AdzunaExtractor
    ext = AdzunaExtractor()
    jobs = ext.search("data analyst", max_pages=2)   # up to 100 jobs
"""
from __future__ import annotations

import logging
import time
from typing import Iterator

import requests

from src.utils.config import settings, require

logger = logging.getLogger(__name__)

BASE_URL = "https://api.adzuna.com/v1/api/jobs"
RESULTS_PER_PAGE = 50          # Adzuna's max
REQUEST_TIMEOUT_SECONDS = 30


class AdzunaExtractor:
    """Thin wrapper around the Adzuna search endpoint."""

    def __init__(self, country: str | None = None) -> None:
        require("ADZUNA_APP_ID", "ADZUNA_APP_KEY")
        self.country = country or settings.ADZUNA_COUNTRY
        self.app_id = settings.ADZUNA_APP_ID
        self.app_key = settings.ADZUNA_APP_KEY

    # ---------- public ----------

    def search(
        self,
        what: str,
        *,
        max_pages: int = 1,
        max_days_old: int = 7,
        where: str | None = None,
    ) -> list[dict]:
        """Return a flat list of job dicts (raw Adzuna payload)."""
        return list(self.iter_search(
            what, max_pages=max_pages, max_days_old=max_days_old, where=where
        ))

    def iter_search(
        self,
        what: str,
        *,
        max_pages: int = 1,
        max_days_old: int = 7,
        where: str | None = None,
    ) -> Iterator[dict]:
        """Yield individual job dicts one at a time so callers can stream."""
        for page in range(1, max_pages + 1):
            batch = self._fetch_page(
                page=page, what=what, max_days_old=max_days_old, where=where,
            )
            if not batch:
                logger.info("Adzuna: no more results at page %d", page)
                break
            yield from batch
            # Polite pause so we don't slam the API.
            time.sleep(0.5)

    # ---------- internals ----------

    def _fetch_page(
        self,
        *,
        page: int,
        what: str,
        max_days_old: int,
        where: str | None,
    ) -> list[dict]:
        url = f"{BASE_URL}/{self.country}/search/{page}"
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": RESULTS_PER_PAGE,
            "what": what,
            "max_days_old": max_days_old,
            "content-type": "application/json",
        }
        if where:
            params["where"] = where

        logger.info("Adzuna GET %s page=%d what=%r", self.country, page, what)
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)

        if resp.status_code != 200:
            logger.error(
                "Adzuna error %d: %s", resp.status_code, resp.text[:300]
            )
            resp.raise_for_status()

        payload = resp.json()
        results = payload.get("results", [])
        logger.info("Adzuna returned %d jobs on page %d", len(results), page)
        return results


# Convenience entrypoint when the module is run directly.
# Useful for quick manual testing without Airflow.
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ext = AdzunaExtractor()
    sample = ext.search("software engineer intern", max_pages=1)
    print(f"Got {len(sample)} jobs")
    if sample:
        first = sample[0]
        print("First job title:", first.get("title"))
        print("First job company:", first.get("company", {}).get("display_name"))
        print("First job location:", first.get("location", {}).get("display_name"))
