# NYC Taxi Lakehouse

A medallion-architecture (bronze → silver → gold) data pipeline built on
Databricks Free Edition, using NYC TLC trip data (Yellow, Green, FHV, FHVHV)
covering the most recent 12 published months.

Built deliberately around Databricks Free Edition's constraints — serverless-only
compute, restricted outbound internet, daily compute quotas — rather than
discovering them mid-project. See `docs/architecture.md` for the reasoning
behind each design choice.

## Architecture

| Stage | Where it runs | What happens |
|---|---|---|
| Download | Local | Python script pulls parquet files from TLC's public CDN |
| Upload | Local -> Databricks | Databricks CLI pushes files into a Unity Catalog volume |
| Bronze | Databricks (serverless) | Raw parquet -> Delta, partitioned, schema-on-read |
| Silver | Databricks (serverless) | Dedup, type enforcement, outlier/bounds checks |
| Gold | Databricks (serverless) | Business aggregates: revenue, demand patterns, durations |
| Orchestration | Databricks Jobs | DAG chaining bronze -> silver -> gold |
| Data quality | Great Expectations | Validation checks at silver/gold boundaries |
| Consumption | Local Streamlit | Dashboard querying gold tables via SQL connector |

## Project phases

- [x] **Phase 0** — Local download + Unity Catalog volume upload
- [ ] **Phase 1** — Bronze ingestion notebooks
- [ ] **Phase 2** — Silver transforms + data quality checks
- [ ] **Phase 3** — Gold aggregates
- [ ] **Phase 4** — Job orchestration (DAG)
- [ ] **Phase 5** — Streamlit dashboard

Each phase is committed separately once verified working end to end.

## Setup

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

## Data source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — published monthly, ~2 month lag.
