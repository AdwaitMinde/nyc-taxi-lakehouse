# Databricks notebook source
# MAGIC %md
# MAGIC ### silver - fhv
# MAGIC fhv bronze has way fewer columns than the other 3 (no fare, no distance, often
# MAGIC no passenger count), so the only real check that applies here is the location
# MAGIC id bounds. dedup key and pickup col name come from the bronze table directly
# MAGIC instead of being hardcoded, same reasoning as the bronze fhv notebook - don't
# MAGIC want to assume a column exists if it might not.

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

if pu_col and do_col:
    checked_df = deduped_df.withColumn(
        "is_valid_trip",
        F.col(pu_col).between(1, 263) & F.col(do_col).between(1, 263)
    )
else:
    # no location columns found at all, can't apply the check - everything passes
    # through untouched rather than guessing
    checked_df = deduped_df.withColumn("is_valid_trip", F.lit(True))

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
