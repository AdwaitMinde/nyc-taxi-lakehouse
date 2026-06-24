# Databricks notebook source
# MAGIC %md
# MAGIC ### silver - fhv
# MAGIC fhv bronze has way fewer columns than the other 3 (no fare, no distance, often
# MAGIC no passenger count), so checks are limited to location ids and duration.
# MAGIC dedup key and column names come from the bronze table directly instead of
# MAGIC being hardcoded, same reasoning as the bronze fhv notebook - don't want to
# MAGIC assume a column exists if it might not.
# MAGIC
# MAGIC originally only checked location, no duration check at all - gold layer
# MAGIC surfaced a ~3 year long "trip" that should've been caught here. added now.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

catalog = "nyc_taxi"
bronze_table = f"{catalog}.bronze.fhv"
silver_table = f"{catalog}.silver.fhv"
quarantine_table = f"{catalog}.silver.fhv_quarantine"

# COMMAND ----------

bronze_df = spark.table(bronze_table)
print(bronze_df.columns)
print(bronze_df.count())

# COMMAND ----------

# figure out which columns actually exist before building the dedup key / checks.
# dispatching_base_num + pickup + dropoff + locations is the dedup key if all of
# those are present, otherwise fall back to whatever subset is available

candidate_keys = ["dispatching_base_num", "PUlocationID", "DOlocationID",
                  "PULocationID", "DOLocationID"]
pickup_candidates = [c for c in bronze_df.columns if "pickup" in c.lower() and "datetime" in c.lower()]
dropoff_candidates = [c for c in bronze_df.columns if "dropoff" in c.lower() and "datetime" in c.lower()]

dedup_keys = [c for c in candidate_keys if c in bronze_df.columns]
dedup_keys += pickup_candidates[:1] + dropoff_candidates[:1]
print("dedup keys:", dedup_keys)

# COMMAND ----------

w = Window.partitionBy(*dedup_keys).orderBy(F.col("_ingest_timestamp").asc())

deduped_df = (
    bronze_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

print(f"removed {bronze_df.count() - deduped_df.count()} dupe rows")

# COMMAND ----------

# location id columns are named inconsistently across years for fhv specifically
# (PUlocationID vs PULocationID etc), so find whatever's actually there

pu_col = next((c for c in deduped_df.columns if c.lower() == "pulocationid"), None)
do_col = next((c for c in deduped_df.columns if c.lower() == "dolocationid"), None)
print("pu_col:", pu_col, "do_col:", do_col)

# COMMAND ----------

# duration check was missing here entirely - gold_trip_duration_stats turned up
# a 1,578,280 minute (~3 year) "trip" in this table, which is obviously bad data
# that should've been caught here. using the same pickup/dropoff columns already
# found above for the dedup key, reusing pickup_candidates/dropoff_candidates
# from up there instead of re-detecting

pickup_col = pickup_candidates[0] if pickup_candidates else None
dropoff_col = dropoff_candidates[0] if dropoff_candidates else None
print("pickup_col:", pickup_col, "dropoff_col:", dropoff_col)

if pickup_col and dropoff_col:
    deduped_df = deduped_df.withColumn(
        "duration_sec",
        F.unix_timestamp(dropoff_col) - F.unix_timestamp(pickup_col)
    )
    has_duration_check = True
else:
    has_duration_check = False

# COMMAND ----------

location_check = F.lit(True)
if pu_col and do_col:
    location_check = F.col(pu_col).between(1, 263) & F.col(do_col).between(1, 263)

duration_check = F.lit(True)
if has_duration_check:
    # same 1-180 min bound as the other 3 datasets, in seconds
    duration_check = F.col("duration_sec").between(60, 10800)

checked_df = deduped_df.withColumn("is_valid_location", location_check) \
    .withColumn("is_valid_duration", duration_check) \
    .withColumn("is_valid_trip", location_check & duration_check)

# COMMAND ----------

checked_df.select(
    F.count("*").alias("total"),
    F.sum((~F.col("is_valid_location")).cast("int")).alias("bad_location"),
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
