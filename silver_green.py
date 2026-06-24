# Databricks notebook source
# MAGIC %md
# MAGIC ### silver - green taxi
# MAGIC same approach as the yellow silver notebook, lpep column names instead of tpep.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

catalog = "nyc_taxi"
bronze_table = f"{catalog}.bronze.green"
silver_table = f"{catalog}.silver.green"
quarantine_table = f"{catalog}.silver.green_quarantine"

# COMMAND ----------

bronze_df = spark.table(bronze_table)
print(bronze_df.count())

# COMMAND ----------

dedup_keys = ["VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
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

checked_df = (
    deduped_df
    .withColumn("trip_duration_min",
        (F.col("lpep_dropoff_datetime").cast("long") - F.col("lpep_pickup_datetime").cast("long")) / 60.0)
    .withColumn("is_valid_location",
        F.col("PULocationID").between(1, 263) & F.col("DOLocationID").between(1, 263))
    .withColumn("is_valid_passenger_count",
        F.col("passenger_count").between(1, 6))
    .withColumn("is_valid_distance",
        F.col("trip_distance") > 0)
    .withColumn("is_valid_fare",
        F.col("fare_amount").between(2.5, 250))
    .withColumn("is_valid_duration",
        F.col("trip_duration_min").between(1, 180))
)

checked_df = checked_df.withColumn(
    "is_valid_trip",
    F.col("is_valid_location") &
    F.col("is_valid_passenger_count") &
    F.col("is_valid_distance") &
    F.col("is_valid_fare") &
    F.col("is_valid_duration")
)

# COMMAND ----------

checked_df.select(
    F.count("*").alias("total"),
    F.sum((~F.col("is_valid_location")).cast("int")).alias("bad_location"),
    F.sum((~F.col("is_valid_passenger_count")).cast("int")).alias("bad_passenger_count"),
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
