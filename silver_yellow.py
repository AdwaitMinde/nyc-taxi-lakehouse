# Databricks notebook source
# MAGIC %md
# MAGIC ### silver - yellow taxi
# MAGIC dedup + bounds checks on top of bronze. flagging bad rows into a separate
# MAGIC quarantine table instead of just dropping them - want to be able to show what
# MAGIC got filtered and why, not just disappear it.
# MAGIC
# MAGIC skipping great expectations here. looked into it and there's an open issue
# MAGIC where GX throws "PERSIST TABLE is not supported on serverless compute" -
# MAGIC seems to be a structural thing with how GX validates on spark dataframes, not
# MAGIC something that's been fixed. plain pyspark checks instead, same effect.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

catalog = "nyc_taxi"
bronze_table = f"{catalog}.bronze.yellow"
silver_table = f"{catalog}.silver.yellow"
quarantine_table = f"{catalog}.silver.yellow_quarantine"

# COMMAND ----------

bronze_df = spark.table(bronze_table)
print(bronze_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### dedup
# MAGIC not using dropDuplicates - it picks an arbitrary surviving row and you can't
# MAGIC say which one without a window function anyway, so just doing it properly.
# MAGIC defining "duplicate" as same vendor + pickup + dropoff + locations, since two
# MAGIC genuinely different trips wouldn't share all of those. keeping the row with the
# MAGIC earliest ingest timestamp as the tiebreak, arbitrary but at least deterministic.

# COMMAND ----------

dedup_keys = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
              "PULocationID", "DOLocationID"]

w = Window.partitionBy(*dedup_keys).orderBy(F.col("_ingest_timestamp").asc())

deduped_df = (
    bronze_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

dupes_removed = bronze_df.count() - deduped_df.count()
print(f"removed {dupes_removed} dupe rows")

# COMMAND ----------

# MAGIC %md
# MAGIC #### bounds checks
# MAGIC thresholds aren't arbitrary - these line up with what's commonly used in TLC
# MAGIC data cleaning writeups (location ids 1-263 is the actual taxi zone range, 6
# MAGIC passengers is the legal max for a yellow cab, etc). a row failing any single
# MAGIC check gets routed to quarantine instead of silver.

# COMMAND ----------

checked_df = (
    deduped_df
    .withColumn("trip_duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
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

# quick look at how much fails each check individually, useful for sanity checking
# whether a threshold is too aggressive before committing to it

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
