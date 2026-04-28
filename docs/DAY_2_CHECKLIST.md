# Day 2 — Silver, Gold extensions, SOC resolver

> Day 2 turns the QCEW Bronze data we loaded yesterday into analytical
> assets the Streamlit dashboard can chart. This file is the operations
> manual; design rationale lives in BLS_INTEGRATION.md.

## What's new in this drop

### SQL (3 new files, run in order)
| file | what it does |
|---|---|
| `sql/09_silver_bls_tables.sql` | creates `silver.bls_qcew_quarterly`, `silver.bls_oews_annual`, `silver.dim_occupation_soc` |
| `sql/10_gold_bls_extensions.sql` | creates `gold.dim_occupation`, `gold.fact_qcew`, `gold.fact_oews_wages`; adds `occupation_key` to `fact_job_postings` |
| `sql/11_bls_analytics_views.sql` | three analytical views the dashboard reads |

### Python (2 transformers + 1 extractor was already there)
| file | what it does |
|---|---|
| `src/transformers/bls_bronze_to_silver.py` | QCEW + OEWS bronze→silver; **SOC resolver** with seeded exact-match + EDITDISTANCE fuzzy fallback, all in one MERGE |
| `src/transformers/bls_silver_to_gold.py` | dim_occupation, fact_qcew, fact_oews_wages, plus the back-fill of `fact_job_postings.occupation_key` |

### DAGs (3 new)
| file | trigger |
|---|---|
| `dags/bls_oews_backfill_dag.py` | manual |
| `dags/bls_bronze_to_silver_dag.py` | Dataset (Bronze QCEW/OEWS or Silver jobs_unified) |
| `dags/bls_silver_to_gold_dag.py` | Dataset (Silver BLS layer) |

---

## Deployment steps

### 1. Drop the new code in
Same as Day 1: unzip the bundle on top of your project, overwrite. No
docker-compose changes this time, so no container restart needed yet.

### 2. Run the three SQL files in Snowflake (in order)
In a Worksheet, paste each file's contents and Run All:

1. `sql/09_silver_bls_tables.sql`
2. `sql/10_gold_bls_extensions.sql`
3. `sql/11_bls_analytics_views.sql`

After file 1 you should see three new SILVER tables.
After file 2, three new GOLD tables and the `occupation_key` column on the
existing `fact_job_postings`.
After file 3, three views in GOLD.

If any file errors, paste the error and stop — don't run the next.

### 3. Reload Airflow so it sees the new DAGs
Just wait ~30 seconds, the scheduler picks up new files automatically.
Confirm in the UI you now see:

- `bls_oews_backfill`
- `bls_bronze_to_silver`
- `bls_silver_to_gold`

### 4. Pull OEWS data into Bronze
This one is small (~150 MB total across 6 years) and fast (~10 minutes).

In the UI: unpause `bls_oews_backfill`, then trigger it. Watch the Graph
view as `load_oews_2019` → `load_oews_2024` go green one by one.

Verify in Snowflake:
```sql
SELECT survey_year, COUNT(*) FROM JOBSEEKERS.BRONZE.RAW_BLS_OEWS
GROUP BY survey_year ORDER BY survey_year;
```
Expected: roughly 800k–1M rows per year, six rows in the result.

### 5. Run the bronze→silver transform once manually
The Dataset trigger will fire automatically on future Bronze updates, but
for the first run we trigger it by hand:

```bash
docker compose exec airflow-scheduler airflow dags trigger bls_bronze_to_silver
```

Or in the UI: trigger `bls_bronze_to_silver`.

The three tasks run in this order:
- `transform_qcew_to_silver` — converts ~14M (or ~500M after backfill) QCEW rows
- `transform_oews_to_silver` — converts ~5M OEWS rows
- `resolve_soc_codes` — runs the resolver and prints the breakdown to logs

When `resolve_soc_codes` finishes, look at its log. You should see lines like:
```
SOC resolver — exact:    187 distinct titles, 8,341 postings
SOC resolver — fuzzy:    312 distinct titles, 2,108 postings
SOC resolver — unmapped:  44 distinct titles,   210 postings
```

The exact + fuzzy fraction should cover **>90%** of postings. If not,
send the breakdown back to me — usually means a few high-volume titles
need to be added to `SOC_SEED_SQL` in `bls_bronze_to_silver.py`.

### 6. Run silver→gold (auto-triggered)
The previous DAG outlets the SILVER_BLS dataset, so `bls_silver_to_gold`
should kick off automatically. If it doesn't, trigger it manually too.

After it finishes, verify the views work end-to-end:

```sql
-- Should show top SOC codes where federal pays more (or less) than private
SELECT * FROM JOBSEEKERS.GOLD.vw_salary_gap_federal_vs_private LIMIT 20;

-- Per-posting comparison to BLS national wage benchmark
SELECT * FROM JOBSEEKERS.GOLD.vw_postings_vs_oews LIMIT 20;

-- Long-history employment trend (one row per state × ownership × quarter)
SELECT * FROM JOBSEEKERS.GOLD.vw_qcew_employment_trend
WHERE state_fips = '06' AND own_label = 'Private'           -- California, private sector
ORDER BY year, qtr LIMIT 50;
```

If those return data, the dashboard is unblocked.

---

## Common issues

### "EDITDISTANCE function not recognized"
Snowflake's `EDITDISTANCE` is in the standard function library and should be
available in any account. If for some reason it isn't, the alternative is
`JAROWINKLER_SIMILARITY` — let me know and I'll swap it.

### "Resolver coverage is below 80%"
Send me a sample of unmapped titles:
```sql
SELECT title_raw, posting_count
FROM SILVER.dim_occupation_soc
WHERE match_method = 'unmapped'
ORDER BY posting_count DESC LIMIT 30;
```
I'll seed those into `SOC_SEED_SQL` and we re-run the resolver.

### "fact_qcew is empty after silver→gold"
Means `transform_qcew_to_silver` produced 0 rows. Check that QCEW Bronze
actually has data:
```sql
SELECT COUNT(*) FROM JOBSEEKERS.BRONZE.RAW_BLS_QCEW;
```
If that's 0, your Day-1 backfill didn't actually land data — start by
re-running `load_qcew_2024` from Day-1's playbook.

---

## What's still missing (Day 3 territory)

- **OEWS in Streamlit**: the `analysis/app.py` still queries `fact_job_postings`
  only. We add 3 new dashboard pages that read the views above. Cheap work,
  ~3-4 hours.
- **State FIPS → state name dim**: `vw_qcew_employment_trend` returns FIPS
  codes; the dashboard needs human-readable names. Tiny lookup table or a
  CASE expression. Day 3.
- **JOLTS extractor**: optional, mostly for the paper's "we have monthly
  hiring signals too" story. Can ship after the main analytical dashboard.
