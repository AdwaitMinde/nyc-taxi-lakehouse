# Deploying to Streamlit Community Cloud

## Prerequisites

- This `dashboard/` folder pushed to GitHub (already done, it's part of
  the main repo)
- A Streamlit Community Cloud account (sign in with GitHub at
  share.streamlit.io)
- A `dashboard/snapshot/` folder committed with the four gold-table
  Parquet files and `metadata.json` (see "Refreshing dashboard data"
  below) - the app reads from these instead of connecting to Databricks
  live, so there are no secrets to configure for this deploy

## Deploy steps

1. Go to share.streamlit.io, click "Create app"
2. Choose "Yup, I have an app", select this repo
3. Branch: `main`
4. Main file path: `dashboard/app.py` (not just `app.py` - it needs the
   subfolder path since that's where it actually lives in the repo)
5. Click Deploy

No secrets are needed for this deploy - `app.py` only reads local Parquet
files out of `dashboard/snapshot/`, which are committed to the repo.

## Refreshing dashboard data

The dashboard doesn't query Databricks live. It reads a static snapshot of
the four gold tables committed to `dashboard/snapshot/`, refreshed
manually alongside the DAG runs (see `docs/phase4_job_orchestration.md`).
This was a deliberate move away from live queries - visitors were hitting
SQL warehouse cold starts and, occasionally, Free Edition's compute quota,
neither of which anyone but the pipeline owner should have to wait out.

After each manual DAG run:

```powershell
pip install -r scripts/requirements.txt
python scripts/export_gold_snapshot.py
```

This needs the same three env vars the live connector used to need
(`DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`)
- see `dashboard/README.md` for where to find each value. It queries the
four gold tables and writes `dashboard/snapshot/*.parquet` plus
`metadata.json` (used for the "Data as of" caption in the app).

Then commit and push the refreshed snapshot:

```powershell
git add dashboard/snapshot
git commit -m "Refresh dashboard snapshot"
git push
```

Streamlit Community Cloud redeploys automatically on push, so the live
dashboard picks up the new snapshot within a minute or two - no
Community Cloud secrets or settings to touch.
