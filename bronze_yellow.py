# Databricks notebook source
# MAGIC %md
# MAGIC ### bronze - yellow taxi
# MAGIC raw parquet from the volume -> delta table, partitioned by pickup month.
# MAGIC
# MAGIC using an explicit schema here instead of letting spark infer it. TLC files
# MAGIC are inconsistent about types across months (passenger_count shows up as both
# MAGIC int64 and double depending on the month) and mergeSchema doesn't fix that,
# MAGIC it only handles missing columns. so just nailing down the schema up front.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, DoubleType,
    StringType, TimestampType
)
from pyspark.sql.functions import year, month, current_timestamp, col

# COMMAND ----------

catalog = "nyc_taxi"
raw_path = f"/Volumes/{catalog}/raw/trip_data/yellow"
bronze_table = f"{catalog}.bronze.yellow"

# COMMAND ----------

# schema pulled from the TLC data dictionary (mar 2025 rev - has cbd_congestion_fee,
# which applies to us since we're pulling may 2025 onward anyway). had to fix a
# couple things after actually checking a file's real schema though - passenger_count
# and RatecodeID come through as long not double, and it's "Airport_fee" (capital A),
# not "airport_fee" like the data dictionary pdf has it. case matters here.

yellow_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
])

# COMMAND ----------

raw_df = (
    spark.read
    .format("parquet")
    .schema(yellow_schema)
    .load(raw_path)
)

raw_df.printSchema()
print(raw_df.count())

# COMMAND ----------

# adding ingest metadata + the partition columns. _source_file is handy if a bad
# row ever needs tracing back to a specific month's file. using _metadata.file_path
# instead of input_file_name() since UC blocks that function outright

bronze_df = (
    raw_df
    .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
    .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

# COMMAND ----------

# overwrite for the initial load. once this is appending new months instead of
# a full reload, switch to append mode (see the silver notebook for that pattern)

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

# quick check that partitioning came out right
display(spark.sql(f"""
    select pickup_year, pickup_month, count(*) as trip_count
    from {bronze_table}
    group by pickup_year, pickup_month
    order by pickup_year, pickup_month
"""))
