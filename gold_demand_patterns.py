# Databricks notebook source
# MAGIC %md
# MAGIC ### gold - demand patterns
# MAGIC built off trips_unified rather than the individual silver tables, since this
# MAGIC is purely about when/where pickups happen - works the same regardless of
# MAGIC trip_type. depends on gold_trips_unified having already been run.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

catalog = "nyc_taxi"
unified_table = f"{catalog}.gold.trips_unified"
gold_table = f"{catalog}.gold.demand_patterns"

# COMMAND ----------

trips = spark.table(unified_table)

demand_df = (
    trips
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))  # 1=sunday, 7=saturday
    .groupBy("trip_type", "PULocationID", "pickup_dow", "pickup_hour")
    .agg(F.count("*").alias("trip_count"))
)

print(demand_df.count())

# COMMAND ----------

(
    demand_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print("done")

# COMMAND ----------

# quick look - busiest hour overall across all trip types
display(spark.sql(f"""
    select pickup_hour, sum(trip_count) as total_trips
    from {gold_table}
    group by pickup_hour
    order by pickup_hour
"""))
