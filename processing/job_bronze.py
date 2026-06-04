from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, col, from_unixtime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CryptoBronzeLayer") \
    .getOrCreate()

# Paths (internal to container)
LANDING_PATH = "/app/data/landing/*.json"
BRONZE_PATH = "/app/data/bronze"

print(f"Reading from {LANDING_PATH}")

# --- Step 1: Determine which source files have already been ingested ---
try:
    already_ingested_df = spark.read.parquet(BRONZE_PATH).select("source_file").distinct()
    ingested_paths = set(row.source_file for row in already_ingested_df.collect())
    print(f"Found {len(ingested_paths)} already-ingested source file(s) in Bronze.")
except Exception:
    # Bronze doesn't exist yet — first run
    ingested_paths = set()
    print("Bronze layer not found. This is the first run; all landing files will be ingested.")

# --- Step 2: Read all landing files with source path attached ---
try:
    all_df = spark.read.option("multiline", "true").json(LANDING_PATH) \
                  .withColumn("source_file", input_file_name())

    # --- Step 3: Keep only files that have not been processed before ---
    # input_file_name() returns a URI like file:///app/data/landing/crypto_assets_....json
    # The values stored in Bronze come from the same function, so the format is consistent.
    if ingested_paths:
        new_df = all_df.filter(~col("source_file").isin(list(ingested_paths)))
    else:
        new_df = all_df

    new_count = new_df.count()
    if new_count == 0:
        print("No new files to process. Exiting.")
        spark.stop()
        exit(0)

    print(f"Processing {new_count} new record(s) from unprocessed landing file(s).")

    # --- Step 4: Add ingestion metadata and write to Bronze ---
    bronze_df = new_df.withColumn(
        "ingestion_time",
        from_unixtime(col("timestamp") / 1000).cast("timestamp")
    )

    print(f"Writing to {BRONZE_PATH}")
    bronze_df.write.mode("append").parquet(BRONZE_PATH)
    print("Bronze Layer processing completed successfully.")

except Exception as e:
    print(f"Error in Bronze Layer: {e}")
    raise

spark.stop()
