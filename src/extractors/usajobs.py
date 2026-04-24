"""
USAJobs API extractor.

Docs: https://developer.usajobs.gov/

Authentication requires two headers:
    Authorization-Key  — API key from usajobs.gov developer account
    User-Agent         — your registered email address

Each SearchResultItem contains a top-level MatchedObjectId (control number)
and a MatchedObjectDescriptor sub-object with all position details.
The loader stores the full item dict as VARIANT, so both keys are reachable
in the Bronze→Silver MERGE.

Usage:
    from src.extractors.usajobs import USAJobsExtractor
    ext = USAJobsExtractor()
    jobs = ext.search("software engineer", max_pages=2)
"""
from __future__ import annotations

import logging
import time
from typing import Iterator

import requests

from src.utils.config import settings, require

logger = logging.getLogger(__name__)

BASE_URL = "https://data.usajobs.gov/api/search"
RESULTS_PER_PAGE = 500          # USAJobs max per page
REQUEST_TIMEOUT_SECONDS = 30


class USAJobsExtractor:
    """Thin wrapper around the USAJobs search endpoint."""

    def __init__(self) -> None:
        require("USAJOBS_API_KEY", "USAJOBS_USER_AGENT")
        self.api_key = settings.USAJOBS_API_KEY
        self.user_agent = settings.USAJOBS_USER_AGENT

    @property
    def _headers(self) -> dict[str, str]:
        return {
            "Host": "data.usajobs.gov",
            "User-Agent": self.user_agent,
            "Authorization-Key": self.api_key,
        }

    # ---------- public ----------

    def search(
        self,
        keyword: str,
        *,
        max_pages: int = 1,
        days_posted: int = 7,
        location: str | None = None,
    ) -> list[dict]:
        """Return a flat list of job dicts (raw USAJobs SearchResultItem payload)."""
        return list(self.iter_search(
            keyword, max_pages=max_pages, days_posted=days_posted, location=location
        ))

    def iter_search(
        self,
        keyword: str,
        *,
        max_pages: int = 1,
        days_posted: int = 7,
        location: str | None = None,
    ) -> Iterator[dict]:
        """Yield individual job dicts one at a time so callers can stream."""
        for page in range(1, max_pages + 1):
            batch = self._fetch_page(
                page=page, keyword=keyword, days_posted=days_posted, location=location,
            )
            if not batch:
                logger.info("USAJobs: no more results at page %d", page)
                break
            yield from batch
            time.sleep(0.5)

    # ---------- internals ----------

    def _fetch_page(
        self,
        *,
        page: int,
        keyword: str,
        days_posted: int,
        location: str | None,
    ) -> list[dict]:
        params: dict[str, str | int] = {
            "Keyword": keyword,
            "ResultsPerPage": RESULTS_PER_PAGE,
            "Page": page,
            "DatePosted": days_posted,
        }
        if location:
            params["LocationName"] = location

        logger.info("USAJobs GET page=%d keyword=%r", page, keyword)
        resp = requests.get(
            BASE_URL,
            headers=self._headers,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        if resp.status_code != 200:
            logger.error("USAJobs error %d: %s", resp.status_code, resp.text[:300])
            resp.raise_for_status()

        payload = resp.json()
        results = payload.get("SearchResult", {}).get("SearchResultItems", [])
        logger.info("USAJobs returned %d jobs on page %d", len(results), page)
        return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ext = USAJobsExtractor()
    sample = ext.search("software engineer", max_pages=1)
    print(f"Got {len(sample)} jobs")
    if sample:
        desc = sample[0].get("MatchedObjectDescriptor", {})
        print("First job title:", desc.get("PositionTitle"))
        print("First job agency:", desc.get("OrganizationName"))
        locs = desc.get("PositionLocation", [])
        if locs:
            print("First job location:", locs[0].get("LocationName"))
