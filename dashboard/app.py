"""
app.py - NYC Taxi Lakehouse dashboard

Local streamlit app, queries the 4 gold tables via the databricks SQL
connector. Needs a SQL warehouse, not jobs/notebook compute - the connector
doesn't support connecting to jobs compute, so this points at the
auto-created "Starter Warehouse" rather than anything from the job DAG.

Auth is a personal access token, set via env vars (never hardcoded, never
committed). Database connector docs recommend OAuth over PATs generally,
but OAuth's browser-based flow doesn't fit a long-running local script well
- PAT is the right tool here even though it's a step down security-wise.
Kept the token lifetime short (90 days) for that reason.

Run with:
    streamlit run app.py

Needs these env vars set first (PowerShell):
    $env:DATABRICKS_SERVER_HOSTNAME = "dbc-0b78111c-44a0.cloud.databricks.com"
    $env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/cd14778cab65ff40"
    $env:DATABRICKS_TOKEN = "dapi..."
"""

import os

import pandas as pd
import streamlit as st
from databricks import sql

st.set_page_config(page_title="NYC Taxi Lakehouse", layout="wide")

CATALOG = "nyc_taxi"


@st.cache_resource
def get_connection():
    server_hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]
    return sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=token,
    )


@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    # not using fetchall_arrow here - pyarrow is an extra dependency and our
    # gold tables are small aggregates already, no real need for it
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
    return pd.DataFrame(rows, columns=columns)


st.title("NYC Taxi Lakehouse")
st.caption("Yellow, Green, FHV, and FHVHV trips - May 2025 through April 2026")

with st.spinner(
    "Connecting to the Databricks SQL warehouse... this runs on serverless "
    "compute that suspends after ~10 minutes idle, so a cold start can take "
    "a few minutes if nobody's viewed this recently. Hang tight."
):
    try:
        test_df = run_query("SELECT 1")
    except KeyError as e:
        st.error(
            f"Missing environment variable: {e}. Set DATABRICKS_SERVER_HOSTNAME, "
            "DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN before running this app."
        )
        st.stop()
    except Exception as e:
        st.error(f"Could not connect to Databricks: {e}")
        st.stop()

tab_overview, tab_demand, tab_revenue, tab_duration = st.tabs(
    ["Overview", "Demand patterns", "Revenue", "Trip duration"]
)

# ---------- Overview ----------
with tab_overview:
    counts_df = run_query(f"""
        SELECT trip_type, COUNT(*) AS trip_count
        FROM {CATALOG}.gold.trips_unified
        GROUP BY trip_type
        ORDER BY trip_count DESC
    """)

    col1, col2 = st.columns([1, 2])
    with col1:
        st.metric("Total trips", f"{counts_df['trip_count'].sum():,}")
        st.dataframe(counts_df, hide_index=True, use_container_width=True)
    with col2:
        st.bar_chart(counts_df.set_index("trip_type"))

    st.caption(
        "fhv has 11 months instead of 12 - TLC hadn't published April 2026 "
        "for that dataset as of when this was built."
    )

# ---------- Demand patterns ----------
with tab_demand:
    st.subheader("Trips by hour of day")

    hourly_df = run_query(f"""
        SELECT pickup_hour, trip_type, SUM(trip_count) AS trip_count
        FROM {CATALOG}.gold.demand_patterns
        GROUP BY pickup_hour, trip_type
        ORDER BY pickup_hour
    """)

    selected_types = st.multiselect(
        "Trip type",
        options=sorted(hourly_df["trip_type"].unique()),
        default=sorted(hourly_df["trip_type"].unique()),
    )
    filtered = hourly_df[hourly_df["trip_type"].isin(selected_types)]
    pivoted = filtered.pivot(index="pickup_hour", columns="trip_type", values="trip_count")
    st.line_chart(pivoted)

    st.subheader("Top 10 pickup zones (all trip types combined)")
    top_zones_df = run_query(f"""
        SELECT PULocationID, SUM(trip_count) AS trip_count
        FROM {CATALOG}.gold.demand_patterns
        GROUP BY PULocationID
        ORDER BY trip_count DESC
        LIMIT 10
    """)
    st.bar_chart(top_zones_df.set_index("PULocationID"))
    st.caption("Zone IDs match the TLC taxi zone lookup table (1-263), not joined here.")

# ---------- Revenue ----------
with tab_revenue:
    st.subheader("Revenue by trip type")
    st.caption(
        "fhv excluded - no fare data in that dataset. fhvhv's total_revenue "
        "is base fare + tips, not a perfect match to yellow/green's "
        "total_amount, but the closest equivalent available."
    )

    rev_by_type_df = run_query(f"""
        SELECT trip_type, SUM(total_revenue) AS revenue, SUM(trip_count) AS trip_count
        FROM {CATALOG}.gold.revenue_by_zone_hour
        GROUP BY trip_type
        ORDER BY revenue DESC
    """)
    rev_by_type_df["revenue_per_trip"] = (
        rev_by_type_df["revenue"] / rev_by_type_df["trip_count"]
    ).round(2)

    col1, col2 = st.columns(2)
    with col1:
        st.bar_chart(rev_by_type_df.set_index("trip_type")["revenue"])
    with col2:
        st.dataframe(rev_by_type_df, hide_index=True, use_container_width=True)

    st.subheader("Revenue by hour of day")
    rev_by_hour_df = run_query(f"""
        SELECT pickup_hour, trip_type, SUM(total_revenue) AS revenue
        FROM {CATALOG}.gold.revenue_by_zone_hour
        GROUP BY pickup_hour, trip_type
        ORDER BY pickup_hour
    """)
    rev_pivoted = rev_by_hour_df.pivot(index="pickup_hour", columns="trip_type", values="revenue")
    st.line_chart(rev_pivoted)

# ---------- Trip duration ----------
with tab_duration:
    st.subheader("Trip duration distribution")

    duration_df = run_query(f"""
        SELECT trip_type, trip_count, avg_duration_min, median_duration_min,
               p90_duration_min, min_duration_min, max_duration_min
        FROM {CATALOG}.gold.trip_duration_stats
        ORDER BY trip_count DESC
    """)
    st.dataframe(duration_df, hide_index=True, use_container_width=True)

    chart_df = duration_df.set_index("trip_type")[
        ["avg_duration_min", "median_duration_min", "p90_duration_min"]
    ]
    st.bar_chart(chart_df)

    st.caption(
        "min/max are clipped to a 1-180 minute bound during the silver layer "
        "validation step - anything outside that range was quarantined, not "
        "dropped silently."
    )