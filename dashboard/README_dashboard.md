# NYC Taxi Lakehouse

A medallion-architecture (bronze → silver → gold) data pipeline built on
Databricks Free Edition, using NYC TLC trip data (Yellow, Green, FHV, FHVHV)
covering the most recent 12 published months, orchestrated end to end and
served through a local Streamlit dashboard.

Built deliberately around Databricks Free Edition's constraints —
serverless-only compute, restricted outbound internet, daily compute quotas
— rather than discovering them mid-project. See `docs/` for phase-by-phase
writeups of the reasoning behind each design choice.

## Architecture

| Stage | Where it runs | What happens |
|---|---|---|
| Download | Local | Python script pulls parquet files from TLC's public CDN |
| Upload | Local -> Databricks | Databricks CLI pushes files into a Unity Catalog volume |
| Bronze | Databricks (serverless) | Raw parquet -> Delta, partitioned, explicit/inferred schema |
| Silver | Databricks (serverless) | Dedup, bounds checks, quarantine for failed rows |
| Gold | Databricks (serverless) | Business aggregates: revenue, demand patterns, durations |
| Orchestration | Databricks Jobs | 12-task DAG chaining bronze -> silver -> gold |
| Data quality | Plain PySpark checks | Bounds/dedup logic in the silver notebooks themselves |
| Consumption | Local Streamlit | Dashboard querying gold tables via SQL connector |

Data quality was originally planned around Great Expectations, but GX has
an open compatibility issue with Databricks serverless compute
(`PERSIST TABLE is not supported on serverless compute`), so validation is
implemented as explicit PySpark boolean checks instead. See
`docs/phase2_lesson_guide.md` for the full reasoning.

## Project phases

- [x] **Phase 0** — Local download + Unity Catalog volume upload
- [x] **Phase 1** — Bronze ingestion notebooks (yellow, green, fhvhv, fhv)
- [x] **Phase 2** — Silver transforms: dedup, bounds checks, quarantine tables
- [x] **Phase 3** — Gold aggregates: trips_unified, demand_patterns,
      revenue_by_zone_hour, trip_duration_stats
- [x] **Phase 4** — Job orchestration: 12-task DAG in Databricks Jobs
- [x] **Phase 5** — Streamlit dashboard, 4 tabs over the gold tables

Each phase was committed separately once verified working end to end.

## Repo layout

```
scripts/      local download + upload scripts (Phase 0)
notebooks/    bronze, silver, gold notebooks + schema setup SQL
dashboard/    Streamlit app + its own requirements.txt and README
docs/         phase-by-phase lesson guides and the job orchestration spec
data/         local raw parquet landing zone (gitignored)
```

## Setup

### Phase 0 — data acquisition

```powershell
pip install -r requirements.txt
python scripts/download_tlc_data.py
databricks auth login --host https://your-workspace-url.cloud.databricks.com
python scripts/upload_to_databricks.py --profile your-profile-name
```

Prerequisite (one-time, run in Databricks SQL editor):

```sql
CREATE CATALOG IF NOT EXISTS nyc_taxi;
USE CATALOG nyc_taxi;
CREATE SCHEMA IF NOT EXISTS raw;
USE SCHEMA raw;
CREATE VOLUME IF NOT EXISTS trip_data COMMENT 'Raw TLC parquet files, landing zone';
```

### Phases 1-3 — bronze, silver, gold

Upload the contents of `notebooks/` into your Databricks workspace. Run the
three `setup_*_schema.sql` notebooks once each (bronze, silver, gold), then
run the bronze notebooks, then silver, then gold. See
`docs/phase1_lesson_guide.md` and `docs/phase2_lesson_guide.md` for the
reasoning behind the schema and validation choices in these notebooks.

### Phase 4 — orchestration

The DAG is a Databricks Jobs config, not a notebook — see
`docs/phase4_job_orchestration.md` for the full task dependency spec and
UI click-path.

### Phase 5 — dashboard

```powershell
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

Needs three environment variables set first (server hostname, HTTP path,
and a personal access token) — see `dashboard/README.md` for where to find
each value and how to generate the token.

## Data source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — published monthly, ~2 month lag.