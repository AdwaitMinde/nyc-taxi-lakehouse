"""
export_gold_snapshot.py - snapshot the gold tables for the dashboard

Queries the 4 gold tables via the Databricks SQL connector (same PAT-based
auth as the dashboard used to use directly) and writes each one to
dashboard/snapshot/<table_name>.parquet, plus a metadata.json with the
export timestamp. The deployed Streamlit app reads these files instead of
querying Databricks live - this script is the manual step that refreshes
them.

Run manually after each DAG run:

    python scripts/export_gold_snapshot.py

Needs the same three env vars as the old dashboard connector (PowerShell):
    $env:DATABRICKS_SERVER_HOSTNAME = "dbc-0b78111c-44a0.cloud.databricks.com"
    $env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/cd14778cab65ff40"
    $env:DATABRICKS_TOKEN = "dapi..."
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from databricks import sql

CATALOG = "nyc_taxi"
SNAPSHOT_DIR = Path(__file__).resolve().parent.parent / "dashboard" / "snapshot"

TABLES = {
    "gold_trips_unified": "trips_unified",
    "gold_demand_patterns": "demand_patterns",
    "gold_revenue_by_zone_hour": "revenue_by_zone_hour",
    "gold_trip_duration_stats": "trip_duration_stats",
}


def get_connection():
    server_hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=token,
    )


def run_query(conn, query: str) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def main():
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection()

    row_counts = {}
    for file_stem, table_name in TABLES.items():
        df = run_query(conn, f"SELECT * FROM {CATALOG}.gold.{table_name}")
        out_path = SNAPSHOT_DIR / f"{file_stem}.parquet"
        df.to_parquet(out_path, index=False)
        row_counts[file_stem] = len(df)

    exported_at = datetime.now(timezone.utc).isoformat()
    metadata = {"exported_at": exported_at, "row_counts": row_counts}
    with open(SNAPSHOT_DIR / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"Snapshot exported at {exported_at}")
    for file_stem, count in row_counts.items():
        print(f"  {file_stem}: {count:,} rows")


if __name__ == "__main__":
    main()
