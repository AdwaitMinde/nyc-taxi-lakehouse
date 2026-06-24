# Databricks notebook source
# MAGIC %md
# MAGIC ### bronze - fhvhv (uber/lyft/via)
# MAGIC richest of the 4 datasets - driver pay, shared ride flags, wav flags, all
# MAGIC the stuff high volume FHV companies have to report under local law 149.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType, LongType,
    IntegerType
)
from pyspark.sql.functions import year, month, current_timestamp, col

# COMMAND ----------

catalog = "nyc_taxi"
raw_path = f"/Volumes/{catalog}/raw/trip_data/fhvhv"
bronze_table = f"{catalog}.bronze.fhvhv"

# COMMAND ----------

# the Y/N flag fields come through as strings from TLC, not booleans - leaving them
# as-is here and converting in silver if we actually need booleans

# checked this against an actual file too - only real fix needed was PULocationID/
# DOLocationID, which are int here not long. airport_fee is lowercase in this one
# (unlike yellow's "Airport_fee" with the capital A, annoyingly inconsistent)

fhvhv_schema = StructType([
    StructField("hvfhs_license_num", StringType(), True),
    StructField("dispatching_base_num", StringType(), True),
    StructField("originating_base_num", StringType(), True),
    StructField("request_datetime", TimestampType(), True),
    StructField("on_scene_datetime", TimestampType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("trip_miles", DoubleType(), True),
    StructField("trip_time", LongType(), True),
    StructField("base_passenger_fare", DoubleType(), True),
    StructField("tolls", DoubleType(), True),
    StructField("bcf", DoubleType(), True),
    StructField("sales_tax", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("tips", DoubleType(), True),
    StructField("driver_pay", DoubleType(), True),
    StructField("shared_request_flag", StringType(), True),
    StructField("shared_match_flag", StringType(), True),
    StructField("access_a_ride_flag", StringType(), True),
    StructField("wav_request_flag", StringType(), True),
    StructField("wav_match_flag", StringType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
])

# COMMAND ----------

raw_df = (
    spark.read
    .format("parquet")
    .schema(fhvhv_schema)
    .load(raw_path)
)

raw_df.printSchema()
print(raw_df.count())

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("pickup_year", year(col("pickup_datetime")))
    .withColumn("pickup_month", month(col("pickup_datetime")))
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

# COMMAND ----------

(
    bronze_df.write
    .format("delta")
    .partitionBy("pickup_year", "pickup_month")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"wrote {bronze_df.count()} rows -> {bronze_table}")

# COMMAND ----------

display(spark.sql(f"""
    select pickup_year, pickup_month, count(*) as trip_count
    from {bronze_table}
    group by pickup_year, pickup_month
    order by pickup_year, pickup_month
"""))
