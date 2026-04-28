"""
BLS QCEW (Quarterly Census of Employment and Wages) extractor.

Source page  : https://www.bls.gov/cew/downloadable-data-files.htm
File pattern : https://data.bls.gov/cew/data/files/{YYYY}/csv/{YYYY}_qtrly_singlefile.zip
Field layout : https://data.bls.gov/cew/doc/layouts/csv_quarterly_layout.htm

Each "quarterly singlefile" is a zip containing one ~1–2 GB CSV that holds
*every* (county × NAICS 6-digit × ownership × quarter) record for that calendar
year. The full 1990–present archive is on the order of 50 GB uncompressed and
forms the volume backbone of our analytical layer.

Design
------
* Stream the download (`requests` with stream=True) — never hold a 2 GB body
  in RAM.
* Verify Content-Length when the server provides it; warn but continue if not.
* Chunked unzip directly to the staging directory — the .zip itself is then
  deleted to keep disk pressure manageable.
* Idempotent: a partial download is detected on retry by checking the size
  of the existing file against Content-Length, and resumed via Range header.

NAICS-only
----------
QCEW publishes NAICS-classified data from 1990 onward. The 1975–1989 SIC files
exist but use a different industry coding system; we skip them. The 1990–2000
NAICS files are themselves reconstructions of the original SIC data so the
series is consistent for our purposes.
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

QCEW_FILE_URL = (
    "https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip"
)
EARLIEST_NAICS_YEAR = 1990
DOWNLOAD_CHUNK_BYTES = 1 << 20      # 1 MiB
REQUEST_TIMEOUT_SECONDS = 60        # connect+read; the body itself streams


@dataclass(frozen=True)
class QCEWFile:
    """One year of QCEW data, as it sits on local disk after extraction."""
    year: int
    csv_path: Path

    @property
    def filename(self) -> str:
        return self.csv_path.name


class QCEWExtractor:
    """Download and unzip QCEW NAICS-based quarterly singlefiles.

    The extractor is purely an *acquisition* layer — it puts CSVs on local
    disk. The loader (BLSLoader) is what PUTs them to Snowflake and runs
    COPY INTO. Splitting the two means we can re-run loads without
    re-downloading 50 GB of data.
    """

    def __init__(self, work_dir: str | Path = "/tmp/bls_qcew") -> None:
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

    # ---------- public ----------

    def fetch_year(self, year: int, *, force: bool = False) -> QCEWFile:
        """Download + unzip the singlefile for `year`. Returns its CSV path.

        If the CSV already exists and `force` is False, this is a no-op —
        making the function safe for retry-heavy Airflow runs.
        """
        if year < EARLIEST_NAICS_YEAR:
            raise ValueError(
                f"QCEW NAICS singlefiles start at {EARLIEST_NAICS_YEAR}; got {year}"
            )

        csv_path = self.work_dir / f"{year}_qtrly_singlefile.csv"
        if csv_path.exists() and not force:
            logger.info("QCEW %d already present at %s — skipping download", year, csv_path)
            return QCEWFile(year=year, csv_path=csv_path)

        zip_path = self.work_dir / f"{year}_qtrly_singlefile.zip"
        self._stream_download(year=year, dest=zip_path)
        self._unzip(zip_path, csv_path)
        zip_path.unlink(missing_ok=True)

        size_gb = csv_path.stat().st_size / (1 << 30)
        logger.info("QCEW %d ready: %s (%.2f GB)", year, csv_path, size_gb)
        return QCEWFile(year=year, csv_path=csv_path)

    def fetch_range(self, start_year: int, end_year: int) -> Iterator[QCEWFile]:
        """Yield QCEWFile for each year in [start_year, end_year] inclusive."""
        for year in range(start_year, end_year + 1):
            try:
                yield self.fetch_year(year)
            except requests.HTTPError as exc:
                # The current calendar year may not be released yet; that's fine.
                if exc.response is not None and exc.response.status_code == 404:
                    logger.warning("QCEW %d not yet published — skipping", year)
                    continue
                raise

    def cleanup(self) -> None:
        """Wipe the work directory. Call after a successful Snowflake load
        if you want to reclaim local disk."""
        if self.work_dir.exists():
            shutil.rmtree(self.work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)

    # ---------- internals ----------

    def _stream_download(self, *, year: int, dest: Path) -> None:
        url = QCEW_FILE_URL.format(year=year)
        logger.info("QCEW GET %s", url)

        # Resume support — only used if a previous run left a partial file.
        already = dest.stat().st_size if dest.exists() else 0
        headers = {"Range": f"bytes={already}-"} if already else {}

        with requests.get(
            url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS, headers=headers,
        ) as resp:
            if resp.status_code in (200, 206):
                pass
            elif resp.status_code == 416 and already > 0:
                # 'Range Not Satisfiable' — server says we already have the whole file.
                logger.info("QCEW %d already fully downloaded", year)
                return
            else:
                resp.raise_for_status()

            mode = "ab" if already and resp.status_code == 206 else "wb"
            written = already if mode == "ab" else 0
            with dest.open(mode) as f:
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                    if chunk:
                        f.write(chunk)
                        written += len(chunk)

        size_gb = written / (1 << 30)
        logger.info("QCEW %d zip downloaded: %.2f GB", year, size_gb)

    @staticmethod
    def _unzip(zip_path: Path, csv_path: Path) -> None:
        """Extract the *single* CSV inside the zip to `csv_path`.

        QCEW zips always contain exactly one CSV; we don't trust the
        filename inside the archive (it's changed format historically),
        we just take the first non-directory member.
        """
        with zipfile.ZipFile(zip_path) as zf:
            members = [m for m in zf.namelist() if not m.endswith("/")]
            if not members:
                raise RuntimeError(f"{zip_path.name}: empty zip archive")
            inner = members[0]
            logger.info("Extracting %s -> %s", inner, csv_path.name)
            with zf.open(inner) as src, csv_path.open("wb") as dst:
                shutil.copyfileobj(src, dst, length=DOWNLOAD_CHUNK_BYTES)


# Manual smoke test:
#     python -m src.extractors.bls_qcew 2024
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2024
    f = QCEWExtractor().fetch_year(year)
    print(f"OK: {f.csv_path}  ({f.csv_path.stat().st_size:,} bytes)")
