# JobSeekers — A Data-Driven Lens on the U.S. Job Market

> **CSE 5114 Final Project, Spring 2026**
> An end-to-end medallion lakehouse that ingests federal (USAJobs) and
> private-sector (Adzuna) job postings, conforms them against the U.S.
> Bureau of Labor Statistics datasets, and exposes the result through both
> a job-board UI and an analytical dashboard.

---

## TL;DR

| | |
|---|---|
| **Total rows materialized** | ~1.5 billion across Bronze / Silver / Gold |
| **Largest single table** | `GOLD.fact_qcew` — **696 million rows, 12.34 GB** |
| **Snowflake compressed storage** | ~30 GB |
| **Logical (uncompressed) data volume** | 128+ GB |
| **Time coverage** | 1990 Q1 – 2024 Q4 (35 years) |
| **Data sources** | 3 (Adzuna, USAJobs, BLS) |
| **DAGs** | 8, Dataset-driven dependencies |
| **SOC mapping coverage** | 95.4% of 1,221 unified postings |

---

## Quickstart

```bash
# 1. Credentials
cp .env.example .env
# Edit .env with your Snowflake, Adzuna, and USAJobs credentials.

# 2. Bring up Airflow + Streamlit (local Docker)
docker compose up -d

# 3. Initialize Snowflake — run these in order in a Worksheet:
#    sql/01_create_databases.sql
#    sql/02_bronze_tables.sql              ─┐
#    sql/03_silver_tables.sql              ─┤  postings layer
#    sql/04_gold_star_schema.sql           ─┤
#    sql/05_analytics_views.sql            ─┘
#    sql/06_schema_updates.sql
#    sql/07_bronze_bls_tables.sql          ─┐
#    sql/13_bronze_oews_reload.sql         ─┤
#    sql/08_bls_stages_and_file_formats.sql─┤  BLS layer
#    sql/14_json_file_format_oews.sql      ─┤
#    sql/09_silver_bls_tables.sql          ─┤
#    sql/10_gold_bls_extensions.sql        ─┤
#    sql/11_bls_analytics_views.sql        ─┘

# 4. Open the UIs
#    Airflow:    http://localhost:8080  (login: airflow / airflow)
#    Streamlit:  http://localhost:8501

# 5. In Airflow, unpause and trigger DAGs in this order:
#    adzuna_bronze_silver_e2e
#    usajobs_bronze_silver_e2e
#    bls_qcew_backfill            ← long-running (~6 hrs for 1990–2024)
#    bls_oews_backfill            ← ~10 minutes for 6 years
#    Downstream DAGs trigger automatically via Datasets.
```

When the pipeline is live, this query confirms the analytical layer is
populated:

```sql
SELECT
  (SELECT COUNT(*) FROM JOBSEEKERS.GOLD.fact_job_postings)  AS postings,
  (SELECT COUNT(*) FROM JOBSEEKERS.GOLD.fact_qcew)          AS qcew_rows,
  (SELECT COUNT(*) FROM JOBSEEKERS.GOLD.dim_occupation)     AS soc_codes;
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                │
├─────────────────────────────────────────────────────────────────────┤
│  [1] USAJobs API           → federal postings, polled 30-min         │
│  [2] Adzuna API            → private postings, polled 30-min         │
│  [3] BLS QCEW singlefiles  → 1990–2024, county × NAICS × quarter     │
│  [4] BLS OEWS workbooks    → May 2024, MSA × SOC occupational wages  │
└─────────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│                Airflow 2.9  (LocalExecutor, 8 DAGs)                  │
│  • adzuna_bronze_silver_e2e        (every 30 min)                    │
│  • usajobs_bronze_silver_e2e       (every 30 min)                    │
│  • silver_to_gold_star_schema      (Dataset-triggered)               │
│  • bls_qcew_backfill               (manual, one-shot 1990–2024)      │
│  • bls_qcew_incremental            (daily)                           │
│  • bls_oews_backfill               (manual)                          │
│  • bls_bronze_to_silver            (Dataset-triggered)               │
│  • bls_silver_to_gold              (Dataset-triggered)               │
└─────────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│            Snowflake — Medallion Lakehouse (16 tables)               │
│                                                                      │
│  🥉 BRONZE   raw_adzuna · raw_usajobs_current · raw_usajobs_hist     │
│              raw_bls_qcew (414M rows, 11 GB)                         │
│              raw_bls_oews (NDJSON, header-name keyed VARIANT)        │
│                                                                      │
│  🥈 SILVER   jobs_unified · bls_qcew_quarterly (348M)                │
│              bls_oews_annual · dim_occupation_soc (SOC resolver)     │
│                                                                      │
│  🥇 GOLD     fact_job_postings · fact_qcew (696M, 12.3 GB)           │
│              fact_oews_wages · dim_occupation                        │
│              dim_date · dim_location · dim_company · dim_category    │
│              + 4 analytical views                                    │
└─────────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────────┐
│                       Streamlit dashboard                            │
│   Job board:  Search · Recommendations · Saved jobs · Preferences    │
│   Analytics:  Federal vs Private wage gap · Occupation mix           │
│               · Salary boxplots · QCEW 35-year employment trend      │
└─────────────────────────────────────────────────────────────────────┘
```

See [docs/BLS_INTEGRATION.md](docs/BLS_INTEGRATION.md) for the BLS layer
design and [docs/DAY_2_CHECKLIST.md](docs/DAY_2_CHECKLIST.md) for the
deployment runbook.

---

## Repository Layout

```
jobseekers/
├── dags/                                Airflow DAGs (8)
│   ├── adzuna_bronze_silver_dag.py
│   ├── usajobs_bronze_silver_dag.py
│   ├── silver_to_gold_dag.py
│   ├── bls_qcew_backfill_dag.py
│   ├── bls_qcew_incremental_dag.py
│   ├── bls_oews_backfill_dag.py
│   ├── bls_bronze_to_silver_dag.py
│   └── bls_silver_to_gold_dag.py
├── src/
│   ├── extractors/                      API + bulk-file clients
│   │   ├── adzuna.py
│   │   ├── usajobs.py
│   │   ├── bls_qcew.py                  streamed download, resumable
│   │   └── bls_oews.py
│   ├── transformers/                    Bronze → Silver → Gold logic
│   │   ├── bronze_to_silver.py
│   │   ├── silver_to_gold.py
│   │   ├── bls_bronze_to_silver.py      includes the SOC resolver
│   │   └── bls_silver_to_gold.py
│   ├── loaders/                         Snowflake writes
│   │   ├── snowflake_loader.py          row-level INSERT for postings
│   │   └── bls_loader.py                PUT + COPY INTO for bulk BLS
│   └── utils/
├── sql/                                 14 DDL/DML files, run in order
├── analysis/
│   ├── app.py                           Streamlit entry
│   ├── analytics_page.py                4-panel analytics tab
│   └── db/
│       ├── snowflake_reader.py          job-board reads
│       ├── analytics_queries.py         analytical aggregations
│       └── user_db.py                   SQLite user/saved-jobs store
├── docs/
│   ├── BLS_INTEGRATION.md               design doc, paper-ready
│   └── DAY_2_CHECKLIST.md               deployment runbook
├── tests/
└── docker-compose.yml
```

---

## SOC Resolver — Three-Stage Title → Occupation Mapping

The single most analytically-important component is the resolver that maps
free-text job titles ("Senior Data Analyst — Remote") to the BLS Standard
Occupational Classification 6-digit code (`15-2041`). Without it, postings
and BLS reference data cannot be joined.

The resolver is implemented entirely in Snowflake SQL (no Python rapidfuzz
dependency) and runs in three stages, each handling what the previous one
missed:

| Stage      | Method                                          | Postings covered |
|------------|-------------------------------------------------|------------------|
| 1. exact   | dictionary lookup against ~85 curated titles    | 16% (189)        |
| 2. fuzzy   | EDITDISTANCE-based, threshold = 15% of length   |  3% (34)         |
| 3. keyword | substring match against ~200 patterns           | 77% (942)        |
| —          | unmapped, sentinel SOC `99-9999`                | 4.6% (56)        |
|            | **total coverage**                              | **95.4%**        |

The keyword layer carries most of the load because USAJobs federal titles
("Supervisory Management and Program Analyst", "Health System Specialist")
do not appear in any standard SOC dictionary but contain unambiguous role
keywords ("program analyst" → 13-1111, "health system" → 11-9111).

---

## Headline Analytical Insight

After running the BLS integration, the dashboard's *Federal vs Private salary
gap* view shows that for occupations with postings on both sides:

| SOC     | Title                | Federal Median | Private Median |     Gap |
|---------|----------------------|---------------:|---------------:|--------:|
| 13-2011 | Accountants          |       $117,778 |        $34,960 | **+237%** |
| 15-1252 | Software Developers  |       $172,035 |        $82,388 | **+109%** |
| 13-1111 | Management Analysts  |       $117,980 |        $59,343 | **+99%**  |
| 15-2051 | Data Scientists      |       $165,503 |        $99,154 |   +67%    |
| 17-2199 | Engineers, All Other |       $116,288 |       $154,221 | **−25%**  |

Federal pays substantially more in nine of ten compared occupations.
Engineering is the sole exception, consistent with private-sector
competition for engineering talent. This insight is unobtainable from any
single source — it requires the conformed `dim_occupation` that the BLS
integration introduces.

---

## Prerequisites

| Tool        | Version |
|-------------|---------|
| Docker      | ≥ 24    |
| Python      | 3.11 (only for ad-hoc local scripts; the pipeline itself runs in containers) |
| Snowflake   | Any tier (free trial works) |

Plus **free** API accounts:
- Adzuna: https://developer.adzuna.com/
- USAJobs: https://developer.usajobs.gov/apirequest/

BLS QCEW and OEWS require no credentials — files are publicly downloadable.

---

## Operational Notes

**QCEW backfill is the long pole.** The 1990–2024 backfill ingests ~50 GB
of compressed source data. On a typical residential connection it takes
4–8 hours. Each year is an independent Airflow task, so a one-off network
failure recovers without redoing the whole 35-year run.

**Snowflake compute.** Bulk QCEW COPY operations run on an X-Small warehouse
in roughly 1–2 minutes per year. The full backfill costs only a few credits
because each parsing step is short and the warehouse auto-suspends between
tasks.

**OEWS column robustness.** BLS reorganized the May-2024 OEWS workbook
layout (consolidated all areas into `all_data_M_2024.xlsx`). Our loader
sidesteps positional CSV parsing entirely by converting each XLSX to NDJSON
in Python, then COPY-ing into a single VARIANT column keyed by header name.
Future column reorderings by BLS will not require code changes.

---

## What's Implemented

- [x] Adzuna + USAJobs ingestion → Bronze → Silver
- [x] Silver → Gold star schema (postings)
- [x] BLS QCEW historical backfill, 1990–2024 (~700M rows in Gold)
- [x] BLS OEWS May-2024 ingestion via NDJSON
- [x] SOC resolver with 95.4% coverage
- [x] Conformed `dim_occupation`, joinable across postings and BLS facts
- [x] 4 analytical Gold views (federal-vs-private, postings-vs-OEWS,
      occupation mix, QCEW employment trend)
- [x] Streamlit job board (search, recommendations, saved jobs, preferences)
- [x] Streamlit Analytics tab — 4 interactive plots reading the Gold views
- [x] Dataset-driven DAG dependencies across all 8 DAGs

---

## Related Work

Architecture influenced by:
- [`kazarmax/adzuna_etl_airflow`](https://github.com/kazarmax/adzuna_etl_airflow) — early reference for the Adzuna ingestion pattern.
- [`abigailhaddad/usajobs_historical`](https://github.com/abigailhaddad/usajobs_historical) — historical USAJobs data source.
- [`astronomer/etl-elt-airflow-snowflake`](https://github.com/astronomer/etl-elt-airflow-snowflake) — Airflow + Snowflake reference architecture.

The BLS integration, SOC resolver, and Dataset-driven multi-DAG layout are
project-specific designs.

---

## Team

- **Xuyang Zheng** — project owner.
  Original partner Yuyang Wu withdrew from the course mid-semester; the
  full pipeline, BLS integration, and dashboard work were completed solo.
