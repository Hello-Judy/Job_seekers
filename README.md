# JobSeekers — Data-Driven Lens on the U.S. Job Market for New Graduates

> CSE 5114 Data Project, Spring 2026
> An end-to-end data engineering pipeline that ingests federal (USAJobs) and
> private-sector (Adzuna) job postings, unifies them into a single schema,
> and surfaces insights through an interactive dashboard.

---

## TL;DR for the Teaching Team

```bash
# 1. Fill in credentials
cp .env.example .env && $EDITOR .env

# 2. Bring up Airflow + Postgres (local Docker)
docker compose up -d

# 3. Open http://localhost:8080  (login: airflow / airflow)
#    Unpause and trigger the DAG:  adzuna_bronze_silver_e2e

# 4. In your Snowflake Worksheet:
#    SELECT COUNT(*) FROM JOBSEEKERS.SILVER.JOBS_UNIFIED;
```

If the count is greater than zero, the end-to-end pipeline is working. See
[docs/architecture.md](docs/architecture.md) for the full design.

---

## Project Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                          │
├──────────────────────────────────────────────────────────────┤
│   [1] USAJobs Historical API  → ~3M records (backfill)       │
│   [2] USAJobs Current API     → daily incremental            │
│   [3] Adzuna API              → daily incremental            │
└──────────────────────────────────────────────────────────────┘
                               ↓
┌──────────────────────────────────────────────────────────────┐
│                 Airflow (orchestration, local)                │
│   • historical_backfill_dag  (one-shot)                      │
│   • daily_incremental_dag    (scheduled)                     │
└──────────────────────────────────────────────────────────────┘
                               ↓
┌──────────────────────────────────────────────────────────────┐
│                Snowflake (medallion architecture)             │
│   🥉 BRONZE   raw_usajobs_hist / raw_usajobs_cur / raw_adzuna│
│   🥈 SILVER   jobs_unified       (one row, one job)          │
│   🥇 GOLD     fact_job_postings + dim_* (star schema)        │
└──────────────────────────────────────────────────────────────┘
                               ↓
┌──────────────────────────────────────────────────────────────┐
│                   Streamlit dashboard                         │
│   Federal vs. private · salary · geography · trends          │
└──────────────────────────────────────────────────────────────┘
```

## Repository Layout

```
jobseekers/
├── dags/                        # Airflow DAGs
├── src/
│   ├── extractors/              # API + parquet clients
│   ├── transformers/            # Bronze → Silver → Gold
│   ├── loaders/                 # Snowflake writes
│   └── utils/                   # Config, logging
├── sql/                         # Snowflake DDL
├── analysis/                    # Streamlit app + notebooks
├── tests/
├── docs/
└── docker-compose.yml
```

## Prerequisites

| Tool        | Version |
|-------------|---------|
| Docker      | ≥ 24    |
| Python      | 3.11    |
| Snowflake   | Any tier (free trial works) |

Plus **free** API accounts:
- Adzuna: https://developer.adzuna.com/
- USAJobs: https://developer.usajobs.gov/apirequest/

## Setup (fresh machine)

```bash
git clone <this-repo>
cd jobseekers

# 1. Credentials
cp .env.example .env
# Edit .env with your Snowflake / Adzuna / USAJobs credentials.

# 2. Initialize Snowflake (one time, from the Snowflake UI)
#    Run sql/01_create_databases.sql then 02_, 03_, 04_ in order.

# 3. Start Airflow
docker compose up -d

# 4. Trigger the DAG from the Airflow UI (http://localhost:8080).
```

## What's Implemented So Far

- [x] Day 1: Adzuna API → Bronze → Silver end-to-end
- [ ] Day 2: USAJobs Current + Historical ingestion
- [ ] Day 3: Silver → Gold star schema
- [ ] Day 4: Historical backfill (≈3M records) for scale
- [ ] Day 5: Streamlit analytics dashboard
- [ ] Day 6: Paper + presentation

## Related Work

Architecture influenced by:
- [`kazarmax/adzuna_etl_airflow`](https://github.com/kazarmax/adzuna_etl_airflow)
- [`abigailhaddad/usajobs_historical`](https://github.com/abigailhaddad/usajobs_historical) (data source for backfill)
- [`astronomer/etl-elt-airflow-snowflake`](https://github.com/astronomer/etl-elt-airflow-snowflake)

See [docs/related_work.md](docs/related_work.md) for a detailed comparison.

## Team

- **Xuyang Zheng** — project owner (original partner Yuyang Wu withdrew from
  the course mid-semester).
