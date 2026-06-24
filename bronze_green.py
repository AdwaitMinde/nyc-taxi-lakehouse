# Databricks notebook source
# MAGIC %md
# MAGIC ### bronze - green taxi
# MAGIC same deal as the yellow notebook, just green's column names. lpep instead of
# MAGIC tpep (livery vs taxi passenger enhancement program), and there's a trip_type
# MAGIC column (street hail vs dispatch) that yellow doesn't have.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, DoubleType,
    StringType, TimestampType
)
from pyspark.sql.functions import year, month, current_timestamp, col

# COMMAND ----------

catalog = "nyc_taxi"
raw_path = f"/Volumes/{catalog}/raw/trip_data/green"
bronze_table = f"{catalog}.bronze.green"

# COMMAND ----------

# schema checked against an actual file's real types rather than trusting the data
# dictionary pdf - RatecodeID, passenger_count, and trip_type are all long here,
# not double like the yellow notebook originally assumed before that got fixed too

green_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", LongType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", LongType(), True),
    StructField("trip_type", LongType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("cbd_congestion_fee", DoubleType(), True),
])

# COMMAND ----------

raw_df = (
    spark.read
    .format("parquet")
    .schema(green_schema)
    .load(raw_path)
)

raw_df.printSchema()
print(raw_df.count())

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("pickup_year", year(col("lpep_pickup_datetime")))
    .withColumn("pickup_month", month(col("lpep_pickup_datetime")))
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
