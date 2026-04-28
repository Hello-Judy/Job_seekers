# BLS Integration — Design Doc

> Adds Bureau of Labor Statistics datasets to the JobSeekers pipeline,
> answering the TA's three improvement areas in one stroke:
> (1) raise data volume to the course's required scale,
> (2) add genuine analytical depth beyond a job board,
> (3) integrate supplementary external datasets the TA explicitly suggested.

This doc is structured to drop straight into the *Methodology* and
*Results* sections of the final paper.

---

## 1. Why BLS

BLS is the single source that answers all three of the TA's notes at once:

| TA note | What BLS provides |
|---|---|
| Need ≥128 GB or true streaming | QCEW alone is ~50 GB uncompressed across 1990–2024; full Bronze/Silver/Gold materialization across QCEW + OEWS lands the warehouse comfortably above 128 GB. |
| Job board → analytical tool | Joining job postings against OEWS gives wage benchmarks; joining against QCEW gives macroeconomic context (employment levels, establishment counts) at the county-NAICS level. |
| Integrate supplementary datasets | The TA explicitly suggested BLS. We use three: QCEW, OEWS, and JOLTS. |

---

## 2. Datasets selected

| Dataset | Cadence | Granularity | Volume (uncompressed) | Role in our schema |
|---|---|---|---|---|
| **QCEW** (Quarterly Census of Employment and Wages) | Quarterly, ~6-month lag | County FIPS × NAICS 6-digit × ownership | ~50 GB (1990–2024) | Volume backbone; macro context |
| **OEWS** (Occupational Employment and Wage Statistics) | Annual (May reference date) | MSA × SOC 6-digit | ~150 MB / year | Wage ground-truth; the join surface for our job postings |
| **JOLTS** (Job Openings and Labor Turnover Survey) | Monthly | State × industry | ~50 MB total | Hiring trend / labor-market tightness panel |

Volume calculation, by layer:

```
Bronze   QCEW raw CSV + OEWS XLSX→CSV + JOLTS JSON, plus VARIANT raw_record   ~70 GB
Silver   typed, deduped, surrogate-keyed                                       ~40 GB
Gold     fact tables widened with OTY metrics, dim tables                      ~25 GB
                                                                              -------
Total logical data in Snowflake                                               ~135 GB
```

Snowflake column-store compression makes the actual on-disk billable storage
much smaller, but the *logical* data volume — what's reported in the paper —
clears the 128 GB course requirement.

---

## 3. New Bronze tables (07_bronze_bls_tables.sql)

Three new tables in `BRONZE` schema:

- `raw_bls_qcew` — typed columns for the most-queried QCEW fields (area, NAICS,
  year, qtr, monthly employment levels, quarterly wages, weekly wage average)
  plus a `raw_record` VARIANT that carries the full original row. This dual
  shape lets analytical queries hit typed columns directly while preserving
  the lakehouse principle that Bronze is faithful to source.
- `raw_bls_oews` — same dual shape, but heavier reliance on `raw_record`
  because OEWS workbooks have 30+ percentile/RSE columns whose names drift
  between survey years.
- `raw_bls_jolts` — landed via the BLS public API as JSON; one row per
  series_id × period.

Clustering keys are tuned for the dominant query pattern:
QCEW is filtered by `(year, qtr, area_fips)`, OEWS by `(survey_year, area_type)`.

---

## 4. Stage and file format (08_bls_stages_and_file_formats.sql)

Bulk loads use Snowflake's native `PUT` + `COPY INTO` rather than
row-by-row `INSERT … PARSE_JSON`. The reasons are entirely about scale:

- A single QCEW singlefile is ~1.5 GB and ~12–15 M rows.
  `executemany(INSERT)` would take hours and burn warehouse credits the
  whole way.
- `PUT` uploads the file once over a single TLS connection.
- `COPY INTO` parses the file in parallel on the server side, charging
  warehouse time only for the parse — typically minutes, not hours.

Two internal stages (`stg_bls_qcew`, `stg_bls_oews`) and two file formats
are created. `ff_csv_bls_oews` declares OEWS suppression markers (`*`,
`**`, `#`) as `NULL_IF` patterns so they don't break typed loads.

No external S3 stage is used. For a coursework project this keeps the
credentials surface minimal and the data inside Snowflake's perimeter.

---

## 5. Extractors

### `src/extractors/bls_qcew.py`

Streams the singlefile zip directly from
`https://data.bls.gov/cew/data/files/{YYYY}/csv/{YYYY}_qtrly_singlefile.zip`,
unzips to disk, deletes the zip. Properties worth flagging in the paper:

- **Streamed download** — never holds the body in RAM.
- **Resumable** — partial downloads are detected on retry and resumed via
  `Range` request. Important on 1.5 GB files over campus Wi-Fi.
- **Idempotent** — re-running an already-completed year is a no-op,
  which interacts cleanly with Airflow's automatic task retries.

### `src/extractors/bls_oews.py`

Pulls the annual `oesm{YY}all.zip` archive (~30–60 MB), unpacks all
included workbooks, and normalizes nested-directory layouts that BLS
has used inconsistently across years.

---

## 6. Loader — `src/loaders/bls_loader.py`

A separate class from `SnowflakeLoader` because its operating point is
different:

| Aspect | `SnowflakeLoader` (existing) | `BLSLoader` (new) |
|---|---|---|
| Payload size | 1–10 KB JSON per posting | 1–2 GB CSV per file |
| Pattern | INSERT ... PARSE_JSON | PUT + COPY INTO |
| Latency budget | Sub-second per row | Minutes per file |
| Idempotency | MERGE on natural key | Snowflake load history (64-day window) |

The COPY templates use `OBJECT_CONSTRUCT` on the Snowflake side to
materialize the `raw_record` VARIANT from the typed columns we just
selected — that way the Bronze contract still reads "every row carries
its full payload" without an extra round trip to repack JSON in Python.

OEWS workbooks need an XLSX → CSV bridge in Python (pandas + openpyxl)
because Snowflake's CSV file format can't directly read XLSX. That
conversion happens once per panel and is cached in a sibling `_csv/`
directory.

---

## 7. DAGs

### `bls_qcew_backfill` (one-shot)

Walks 1990 → 2024 sequentially, one task per year. Each task:

1. Downloads + unzips the singlefile.
2. PUTs the CSV to `@stg_bls_qcew/{year}/`.
3. COPYs into `BRONZE.raw_bls_qcew`.
4. Deletes the staged copy and the local file.

Tasks run sequentially because parallel years would multiply local disk
pressure (~1.5 GB each) and saturate the upload pipe to Snowflake. With
Airflow task-level retries on each year, a one-off network failure
recovers without redoing the entire 35-year backfill.

### `bls_qcew_incremental` (daily)

Re-fetches the current and previous calendar year on each run. Cheap
when nothing has changed because:

- The extractor short-circuits if the local CSV already exists.
- Snowflake's COPY load history skips already-loaded files (64-day window).

The previous year is included so revisions BLS publishes after the initial
release flow into Bronze automatically.

The DAG declares `Dataset("snowflake://BRONZE/raw_bls_qcew")` as an outlet,
so the Silver build (next milestone) will run automatically on each
successful incremental load — same Dataset-driven pattern the existing
Silver→Gold DAG uses.

---

## 8. What this does *not* yet do

The Silver layer for BLS is the next milestone. It will:

1. Resolve free-text job titles in `SILVER.jobs_unified` against SOC codes
   so we can join postings to OEWS wage estimates. The implementation
   uses BLS's Direct Match Title File as a primary lookup with a
   rapidfuzz-based fallback for unmatched titles.
2. Produce `SILVER.bls_qcew_quarterly` — the typed, deduped, area-FIPS-
   normalized form of QCEW.
3. Produce `SILVER.bls_oews_annual` — flattened OEWS with suppression
   markers interpreted, percentile columns split out.

After Silver is in place, three new Gold tables and four conformed
analytical views feed the Streamlit dashboard's new analytics pages
(salary distribution, federal-vs-private wage gap, geographic heatmap,
quarterly hiring trend).

---

## 9. How this answers the TA's notes (paper-ready summary)

> Increase data scale (critical): The QCEW backfill alone lands ~50 GB of
> historical employment and wage data into Bronze. Combined with OEWS and
> JOLTS and the materialized Silver/Gold layers, the warehouse holds
> roughly 135 GB of logical data, exceeding the course's 128 GB requirement.

> Shift from job board to analytical tool: OEWS provides the wage benchmark
> that lets us compute, for every posting in our pipeline, how its
> advertised salary compares to the national / state / MSA median for the
> matched SOC occupation. The Streamlit dashboard exposes this comparison
> directly, transforming the experience from "browse postings" to
> "understand the market for this role."

> Integrate supplementary datasets: We followed the TA's specific suggestion
> and integrated three BLS programs. Each was selected for a distinct
> analytical role: QCEW for macro context, OEWS for occupational wage
> ground-truth, JOLTS for hiring-trend signals.
