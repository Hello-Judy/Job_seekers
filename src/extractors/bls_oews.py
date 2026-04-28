"""
BLS OEWS (Occupational Employment and Wage Statistics) extractor.

Source page  : https://www.bls.gov/oes/tables.htm
Bulk archive : https://www.bls.gov/oes/special-requests/oesm{YY}all.zip
                (one zip per May survey, contains national/state/MSA workbooks)

OEWS publishes annually with a May reference date. The "all" zip bundles every
geography in one file (~30–60 MB), which is a much friendlier shape for our
pipeline than scraping each state's individual .xlsx.

The XLSX files inside use suppression markers ('*', '**', '#') for cells that
fail BLS disclosure rules. We do NOT clean those here — the loader passes them
through to Snowflake's NULL_IF list at COPY time.
"""
from __future__ import annotations

import logging
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

OEWS_URL_TEMPLATE = "https://www.bls.gov/oes/special-requests/oesm{yy}all.zip"
EARLIEST_YEAR = 2019                # 2019+ uses 2018-SOC, our target schema
DOWNLOAD_CHUNK_BYTES = 1 << 20
REQUEST_TIMEOUT_SECONDS = 60

# BLS rejects anonymous requests intermittently; a real-looking UA fixes it.
USER_AGENT = "JobSeekers-Coursework/1.0 (CSE 5114; mailto:noreply@example.org)"


@dataclass(frozen=True)
class OEWSPanel:
    """One survey year's OEWS workbooks, unpacked to disk."""
    survey_year: int
    extract_dir: Path

    def xlsx_files(self) -> list[Path]:
        return sorted(p for p in self.extract_dir.glob("*.xlsx") if not p.name.startswith("~"))


class OEWSExtractor:
    """Download and unzip OEWS annual archives.

    XLSX → CSV conversion is left to the loader so the extractor stays
    free of pandas/openpyxl as a hard dependency at acquisition time.
    """

    def __init__(self, work_dir: str | Path = "/tmp/bls_oews") -> None:
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

    def fetch_year(self, survey_year: int, *, force: bool = False) -> OEWSPanel:
        if survey_year < EARLIEST_YEAR:
            raise ValueError(
                f"OEWSExtractor targets 2018-SOC panels (>= {EARLIEST_YEAR}); "
                f"got {survey_year}"
            )

        extract_dir = self.work_dir / f"oesm{survey_year:04d}"
        if extract_dir.exists() and any(extract_dir.glob("*.xlsx")) and not force:
            logger.info("OEWS %d already extracted at %s", survey_year, extract_dir)
            return OEWSPanel(survey_year=survey_year, extract_dir=extract_dir)

        zip_path = self.work_dir / f"oesm{survey_year % 100:02d}all.zip"
        self._download(survey_year=survey_year, dest=zip_path)
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)
        zip_path.unlink(missing_ok=True)

        n = sum(1 for _ in extract_dir.rglob("*.xlsx"))
        logger.info("OEWS %d ready: %d workbooks under %s", survey_year, n, extract_dir)

        # The zip occasionally nests one extra directory ('oesm24all/'). Normalize.
        self._flatten(extract_dir)
        return OEWSPanel(survey_year=survey_year, extract_dir=extract_dir)

    def fetch_range(self, start_year: int, end_year: int) -> Iterator[OEWSPanel]:
        for year in range(start_year, end_year + 1):
            try:
                yield self.fetch_year(year)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    logger.warning("OEWS %d not yet published — skipping", year)
                    continue
                raise

    def cleanup(self) -> None:
        if self.work_dir.exists():
            shutil.rmtree(self.work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

    # ---------- internals ----------

    def _download(self, *, survey_year: int, dest: Path) -> None:
        url = OEWS_URL_TEMPLATE.format(yy=f"{survey_year % 100:02d}")
        logger.info("OEWS GET %s", url)
        with requests.get(
            url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": USER_AGENT},
        ) as resp:
            resp.raise_for_status()
            with dest.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                    if chunk:
                        f.write(chunk)
        logger.info("OEWS %d zip downloaded: %d bytes", survey_year, dest.stat().st_size)

    @staticmethod
    def _flatten(extract_dir: Path) -> None:
        """Some OEWS zips nest workbooks in a single subdirectory; promote them."""
        subdirs = [p for p in extract_dir.iterdir() if p.is_dir()]
        if len(subdirs) == 1 and not any(extract_dir.glob("*.xlsx")):
            inner = subdirs[0]
            for child in inner.iterdir():
                child.rename(extract_dir / child.name)
            inner.rmdir()


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2024
    panel = OEWSExtractor().fetch_year(year)
    print(f"OK: {panel.extract_dir}  ({len(panel.xlsx_files())} workbooks)")
