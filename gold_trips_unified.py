# Databricks notebook source
# MAGIC %md
# MAGIC ### gold - unified trips
# MAGIC pulls just the common fields out of all 4 silver tables (pickup/dropoff time,
# MAGIC pickup/dropoff location, a trip_type label) and stacks them into one table.
# MAGIC this is what makes "compare yellow vs green vs fhvhv vs fhv" possible at all -
# MAGIC the silver tables don't share a schema so you can't query across them directly.
# MAGIC
# MAGIC deliberately NOT using unionByName's allowMissingColumns to merge full schemas
# MAGIC together - that would leave a 40+ column table full of nulls depending on which
# MAGIC dataset a row came from. narrowing to common fields first, then stacking, is
# MAGIC cleaner.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

catalog = "nyc_taxi"
gold_table = f"{catalog}.gold.trips_unified"

# COMMAND ----------

yellow = (
    spark.table(f"{catalog}.silver.yellow")
    .select(
        F.lit("yellow").alias("trip_type"),
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        "PULocationID", "DOLocationID",
        "pickup_year", "pickup_month",
    )
)

green = (
    spark.table(f"{catalog}.silver.green")
    .select(
        F.lit("green").alias("trip_type"),
        F.col("lpep_pickup_datetime").alias("pickup_datetime"),
        F.col("lpep_dropoff_datetime").alias("dropoff_datetime"),
        "PULocationID", "DOLocationID",
        "pickup_year", "pickup_month",
    )
)

fhvhv = (
    spark.table(f"{catalog}.silver.fhvhv")
    .select(
        F.lit("fhvhv").alias("trip_type"),
        "pickup_datetime", "dropoff_datetime",
        F.col("PULocationID").cast("int").alias("PULocationID"),
        F.col("DOLocationID").cast("int").alias("DOLocationID"),
        "pickup_year", "pickup_month",
    )
)

# COMMAND ----------

# fhv's column names vary by year so grab them dynamically same as the bronze/
# silver fhv notebooks do, rather than hardcoding something that might not exist

fhv_raw = spark.table(f"{catalog}.silver.fhv")

pickup_col = next((c for c in fhv_raw.columns if "pickup" in c.lower() and "datetime" in c.lower()), None)
dropoff_col = next((c for c in fhv_raw.columns if "dropoff" in c.lower() and "datetime" in c.lower()), None)
pu_col = next((c for c in fhv_raw.columns if c.lower() == "pulocationid"), None)
do_col = next((c for c in fhv_raw.columns if c.lower() == "dolocationid"), None)

print(pickup_col, dropoff_col, pu_col, do_col)

# COMMAND ----------

if pickup_col and dropoff_col and pu_col and do_col:
    fhv = (
        fhv_raw
        .select(
            F.lit("fhv").alias("trip_type"),
            F.col(pickup_col).alias("pickup_datetime"),
            F.col(dropoff_col).alias("dropoff_datetime"),
            F.col(pu_col).cast("int").alias("PULocationID"),
            F.col(do_col).cast("int").alias("DOLocationID"),
            "pickup_year", "pickup_month",
        )
    )
else:
    fhv = None
    print("fhv missing one of the expected columns, skipping it from the unified table")

# COMMAND ----------

unified_df = yellow.unionByName(green).unionByName(fhvhv)
if fhv is not None:
    unified_df = unified_df.unionByName(fhv)

print(unified_df.count())

# COMMAND ----------

(
    unified_df.write
    .format("delta")
    .partitionBy("pickup_year", "pickup_month")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print("done")

# COMMAND ----------

display(spark.sql(f"""
    select trip_type, count(*) as trip_count
    from {gold_table}
    group by trip_type
    order by trip_count desc
"""))
