"""
app.py - NYC Taxi Lakehouse dashboard

Local streamlit app, reads the 4 gold tables from a local Parquet snapshot
(dashboard/snapshot/) instead of querying Databricks live. The snapshot is
refreshed manually by running scripts/export_gold_snapshot.py after each
DAG run - see dashboard/DEPLOYMENT.md for the "Refreshing dashboard data"
section.

Run with:
    streamlit run app.py
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="NYC Taxi Lakehouse", layout="wide")

SNAPSHOT_DIR = Path(__file__).resolve().parent / "snapshot"


@st.cache_data
def load_table(name: str) -> pd.DataFrame:
    path = SNAPSHOT_DIR / f"{name}.parquet"
    return pd.read_parquet(path)


@st.cache_data
def load_metadata() -> dict:
    path = SNAPSHOT_DIR / "metadata.json"
    with open(path) as f:
        return json.load(f)


def snapshot_missing_error(name: str):
    st.error(
        f"Snapshot file `{name}.parquet` is missing from `dashboard/snapshot/`. "
        "Run `python scripts/export_gold_snapshot.py` to generate it, then "
        "commit and push the `dashboard/snapshot/` folder."
    )
    st.stop()


st.title("NYC Taxi Lakehouse")
st.caption("Yellow, Green, FHV, and FHVHV trips - May 2025 through April 2026")

try:
    metadata = load_metadata()
    exported_at = datetime.fromisoformat(metadata["exported_at"])
    data_as_of = f"Data as of {exported_at.strftime('%B %d, %Y')} (UTC)"
except FileNotFoundError:
    st.error(
        "`dashboard/snapshot/metadata.json` is missing. Run "
        "`python scripts/export_gold_snapshot.py` to generate a snapshot, "
        "then commit and push the `dashboard/snapshot/` folder."
    )
    st.stop()

tab_overview, tab_demand, tab_revenue, tab_duration = st.tabs(
    ["Overview", "Demand patterns", "Revenue", "Trip duration"]
)

# ---------- Overview ----------
with tab_overview:
    st.caption(data_as_of)
    try:
        trips_df = load_table("gold_trips_unified")
    except FileNotFoundError:
        snapshot_missing_error("gold_trips_unified")

    counts_df = (
        trips_df.groupby("trip_type")
        .size()
        .reset_index(name="trip_count")
        .sort_values("trip_count", ascending=False)
    )

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
    st.caption(data_as_of)
    st.subheader("Trips by hour of day")

    try:
        hourly_df = load_table("gold_demand_patterns")
    except FileNotFoundError:
        snapshot_missing_error("gold_demand_patterns")

    selected_types = st.multiselect(
        "Trip type",
        options=sorted(hourly_df["trip_type"].unique()),
        default=sorted(hourly_df["trip_type"].unique()),
    )
    filtered = hourly_df[hourly_df["trip_type"].isin(selected_types)]
    grouped = (
        filtered.groupby(["pickup_hour", "trip_type"])["trip_count"]
        .sum()
        .reset_index()
    )
    pivoted = grouped.pivot(index="pickup_hour", columns="trip_type", values="trip_count")
    st.line_chart(pivoted)

    st.subheader("Top 10 pickup zones (all trip types combined)")
    top_zones_df = (
        hourly_df.groupby("PULocationID")["trip_count"]
        .sum()
        .reset_index()
        .sort_values("trip_count", ascending=False)
        .head(10)
    )
    st.bar_chart(top_zones_df.set_index("PULocationID"))
    st.caption("Zone IDs match the TLC taxi zone lookup table (1-263), not joined here.")

# ---------- Revenue ----------
with tab_revenue:
    st.caption(data_as_of)
    st.subheader("Revenue by trip type")
    st.caption(
        "fhv excluded - no fare data in that dataset. fhvhv's total_revenue "
        "is base fare + tips, not a perfect match to yellow/green's "
        "total_amount, but the closest equivalent available."
    )

    try:
        revenue_df = load_table("gold_revenue_by_zone_hour")
    except FileNotFoundError:
        snapshot_missing_error("gold_revenue_by_zone_hour")

    rev_by_type_df = (
        revenue_df.groupby("trip_type")
        .agg(revenue=("total_revenue", "sum"), trip_count=("trip_count", "sum"))
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    rev_by_type_df["revenue_per_trip"] = (
        rev_by_type_df["revenue"] / rev_by_type_df["trip_count"]
    ).round(2)

    col1, col2 = st.columns(2)
    with col1:
        st.bar_chart(rev_by_type_df.set_index("trip_type")["revenue"])
    with col2:
        st.dataframe(rev_by_type_df, hide_index=True, use_container_width=True)

    st.subheader("Revenue by hour of day")
    rev_by_hour_df = (
        revenue_df.groupby(["pickup_hour", "trip_type"])["total_revenue"]
        .sum()
        .reset_index()
        .rename(columns={"total_revenue": "revenue"})
    )
    rev_pivoted = rev_by_hour_df.pivot(index="pickup_hour", columns="trip_type", values="revenue")
    st.line_chart(rev_pivoted)

# ---------- Trip duration ----------
with tab_duration:
    st.caption(data_as_of)
    st.subheader("Trip duration distribution")

    try:
        duration_df = load_table("gold_trip_duration_stats")
    except FileNotFoundError:
        snapshot_missing_error("gold_trip_duration_stats")

    duration_df = duration_df[
        [
            "trip_type",
            "trip_count",
            "avg_duration_min",
            "median_duration_min",
            "p90_duration_min",
            "min_duration_min",
            "max_duration_min",
        ]
    ].sort_values("trip_count", ascending=False)
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
