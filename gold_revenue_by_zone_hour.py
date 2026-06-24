# Databricks notebook source
# MAGIC %md
# MAGIC ### gold - revenue by zone/hour
# MAGIC unlike demand_patterns, this one can't just use trips_unified - revenue
# MAGIC fields are different per dataset (fare_amount for yellow/green vs
# MAGIC base_passenger_fare for fhvhv, fhv has no fare data at all). aggregating each
# MAGIC dataset separately first, then stacking the already-aggregated results
# MAGIC together. fhv is skipped here, not unioned in as zero/null - it has nothing
# MAGIC to contribute to a revenue table.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

catalog = "nyc_taxi"
gold_table = f"{catalog}.gold.revenue_by_zone_hour"

# COMMAND ----------

yellow_rev = (
    spark.table(f"{catalog}.silver.yellow")
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .groupBy(F.lit("yellow").alias("trip_type"), "PULocationID", "pickup_hour")
    .agg(
        F.sum("fare_amount").alias("total_fare"),
        F.sum("tip_amount").alias("total_tips"),
        F.sum("total_amount").alias("total_revenue"),
        F.count("*").alias("trip_count"),
    )
)

green_rev = (
    spark.table(f"{catalog}.silver.green")
    .withColumn("pickup_hour", F.hour("lpep_pickup_datetime"))
    .groupBy(F.lit("green").alias("trip_type"), "PULocationID", "pickup_hour")
    .agg(
        F.sum("fare_amount").alias("total_fare"),
        F.sum("tip_amount").alias("total_tips"),
        F.sum("total_amount").alias("total_revenue"),
        F.count("*").alias("trip_count"),
    )
)

# COMMAND ----------

# fhvhv doesn't have total_amount the way yellow/green do, so total_revenue here
# is base fare + tips, which isn't perfectly apples to apples with the other two
# but it's the closest equivalent available

fhvhv_rev = (
    spark.table(f"{catalog}.silver.fhvhv")
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .withColumn("PULocationID", F.col("PULocationID").cast("int"))
    .groupBy(F.lit("fhvhv").alias("trip_type"), "PULocationID", "pickup_hour")
    .agg(
        F.sum("base_passenger_fare").alias("total_fare"),
        F.sum("tips").alias("total_tips"),
        (F.sum("base_passenger_fare") + F.sum("tips")).alias("total_revenue"),
        F.count("*").alias("trip_count"),
    )
)

# COMMAND ----------

revenue_df = yellow_rev.unionByName(green_rev).unionByName(fhvhv_rev)
print(revenue_df.count())

# COMMAND ----------

(
    revenue_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print("done")

# COMMAND ----------

display(spark.sql(f"""
    select trip_type, sum(total_revenue) as revenue
    from {gold_table}
    group by trip_type
    order by revenue desc
"""))
