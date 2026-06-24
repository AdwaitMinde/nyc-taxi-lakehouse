# Databricks notebook source
# MAGIC %md
# MAGIC ### gold - trip duration distribution
# MAGIC off trips_unified again, duration is just pickup/dropoff time which every
# MAGIC dataset has. using approx_percentile since exact percentiles over millions
# MAGIC of rows is expensive for not much gain in accuracy here.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

catalog = "nyc_taxi"
unified_table = f"{catalog}.gold.trips_unified"
gold_table = f"{catalog}.gold.trip_duration_stats"

# COMMAND ----------

trips = (
    spark.table(unified_table)
    .withColumn(
        "duration_min",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0
    )
)

duration_stats = (
    trips
    .groupBy("trip_type")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.round(F.expr("approx_percentile(duration_min, 0.5)"), 2).alias("median_duration_min"),
        F.round(F.expr("approx_percentile(duration_min, 0.9)"), 2).alias("p90_duration_min"),
        F.round(F.min("duration_min"), 2).alias("min_duration_min"),
        F.round(F.max("duration_min"), 2).alias("max_duration_min"),
    )
)

display(duration_stats)

# COMMAND ----------

(
    duration_stats.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print("done")
