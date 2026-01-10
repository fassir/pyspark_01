from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CryptoBronzeLayer") \
    .getOrCreate()

# Paths (internal to container)
LANDING_PATH = "/app/data/landing/*.json"
BRONZE_PATH = "/app/data/bronze"

# Define Schema (to enforce structure on raw JSON)
schema = StructType([
    StructField("data", StringType(), True), # It usually comes in a 'data' wrapper
    StructField("timestamp", LongType(), True)
])

print(f"Reading from {LANDING_PATH}")

try:
    # Read raw JSON files
    # multiLine option might be needed if pretty-printed
    raw_df = spark.read.option("multiline", "true").json(LANDING_PATH)
    
    # CRITICAL FIX: Use source 'timestamp' (ms) to distinguish files in batch
    # timestamp is Long (ms). Convert to Timestamp type.
    raw_df_with_meta = raw_df.withColumn("ingestion_time", from_unixtime(col("timestamp") / 1000).cast("timestamp")) \
                             .withColumn("source_file", input_file_name())

    # Write to Bronze
    print(f"Writing to {BRONZE_PATH}")
    raw_df_with_meta.write.mode("append").parquet(BRONZE_PATH)
    print("Bronze Layer processing completed successfully.")

except Exception as e:
    print(f"Error in Bronze Layer: {e}")

spark.stop()
