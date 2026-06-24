# Databricks notebook source
# MAGIC %md
# MAGIC ### silver - fhvhv
# MAGIC different field set than yellow/green so the bounds checks are different -
# MAGIC no passenger_count or fare_amount here, using trip_miles/base_passenger_fare/
# MAGIC trip_time instead. dedup key also doesn't include vendor since there's no
# MAGIC VendorID column for this dataset, using dispatching_base_num instead.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

catalog = "nyc_taxi"
bronze_table = f"{catalog}.bronze.fhvhv"
silver_table = f"{catalog}.silver.fhvhv"
quarantine_table = f"{catalog}.silver.fhvhv_quarantine"

# COMMAND ----------

bronze_df = spark.table(bronze_table)
print(bronze_df.count())

# COMMAND ----------

dedup_keys = ["dispatching_base_num", "pickup_datetime", "dropoff_datetime",
              "PULocationID", "DOLocationID"]

w = Window.partitionBy(*dedup_keys).orderBy(F.col("_ingest_timestamp").asc())

deduped_df = (
    bronze_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f"removed {bronze_df.count() - deduped_df.count()} dupe rows")

# COMMAND ----------

# MAGIC %md
# MAGIC #### bounds checks
# MAGIC trip_time is already in seconds per the data dictionary, not minutes like the
# MAGIC derived column in yellow/green. duration bound converted to seconds to match
# MAGIC (1 to 180 min -> 60 to 10800 sec). no passenger count field to check here,
# MAGIC fhvhv doesn't report it.

# COMMAND ----------

checked_df = (
    deduped_df
    .withColumn("is_valid_location",
        F.col("PULocationID").between(1, 263) & F.col("DOLocationID").between(1, 263))
    .withColumn("is_valid_distance",
        F.col("trip_miles") > 0)
    .withColumn("is_valid_fare",
        F.col("base_passenger_fare") > 0)
    .withColumn("is_valid_duration",
        F.col("trip_time").between(60, 10800))
)

checked_df = checked_df.withColumn(
    "is_valid_trip",
    F.col("is_valid_location") &
    F.col("is_valid_distance") &
    F.col("is_valid_fare") &
    F.col("is_valid_duration")
)

# COMMAND ----------

checked_df.select(
    F.count("*").alias("total"),
    F.sum((~F.col("is_valid_location")).cast("int")).alias("bad_location"),
    F.sum((~F.col("is_valid_distance")).cast("int")).alias("bad_distance"),
    F.sum((~F.col("is_valid_fare")).cast("int")).alias("bad_fare"),
    F.sum((~F.col("is_valid_duration")).cast("int")).alias("bad_duration"),
).show()

# COMMAND ----------

silver_df = checked_df.filter(F.col("is_valid_trip") == True)
quarantine_df = checked_df.filter(F.col("is_valid_trip") == False)

print(f"silver: {silver_df.count()}, quarantine: {quarantine_df.count()}")

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .partitionBy("pickup_year", "pickup_month")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(quarantine_table)
)

print("done")

# COMMAND ----------

display(spark.sql(f"""
    select pickup_year, pickup_month, count(*) as trip_count
    from {silver_table}
    group by pickup_year, pickup_month
    order by pickup_year, pickup_month
"""))
