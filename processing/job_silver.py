import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, lit
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("CryptoSilverLayer").getOrCreate()

BRONZE_PATH = "/app/data/bronze"
SILVER_PATH = "/app/data/silver"

# CoinCap / CoinGecko-compatible asset schema (all fields arrive as strings from JSON)
asset_schema = StructType([
    StructField("id", StringType(), True),
    StructField("rank", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("supply", StringType(), True),
    StructField("maxSupply", StringType(), True),
    StructField("marketCapUsd", StringType(), True),
    StructField("volumeUsd24Hr", StringType(), True),
    StructField("priceUsd", StringType(), True),
    StructField("changePercent24Hr", StringType(), True),
    StructField("vwap24Hr", StringType(), True)
])

try:
    bronze_df = spark.read.parquet(BRONZE_PATH)

    if "data" not in bronze_df.columns:
        print("Column 'data' not found in Bronze. Schema might be different.")
        bronze_df.printSchema()
        spark.stop()
        sys.exit(1)

    print("Bronze DataFrame Schema:")
    bronze_df.printSchema()

    # -----------------------------------------------------------------------
    # Incremental watermark: only process Bronze records newer than the latest
    # ingestion_time already present in Silver.  On the very first run Silver
    # does not exist yet, so we fall back to processing everything.
    # -----------------------------------------------------------------------
    latest_silver_time = None
    try:
        existing_silver = spark.read.parquet(SILVER_PATH)
        latest_silver_time = existing_silver.agg({"ingestion_time": "max"}).collect()[0][0]
        print(f"Silver watermark (latest ingestion_time): {latest_silver_time}")
    except Exception:
        print("Silver layer does not exist yet — full Bronze load will be performed.")

    if latest_silver_time is not None:
        bronze_df = bronze_df.filter(col("ingestion_time") > latest_silver_time)
        new_bronze_count = bronze_df.count()
        if new_bronze_count == 0:
            print("No new Bronze records found after watermark. Silver is already up-to-date.")
            spark.stop()
            sys.exit(0)
        print(f"Found {new_bronze_count} new Bronze rows to process (after watermark).")

    # -----------------------------------------------------------------------
    # Dual-schema handling: Bronze 'data' column may be stored as a JSON
    # string (early schema) or as a native array of structs (current schema).
    # -----------------------------------------------------------------------
    dtype = dict(bronze_df.dtypes)["data"]
    print(f"'data' column type: {dtype}")

    if dtype.startswith("string"):
        json_schema = ArrayType(asset_schema)
        exploded_df = (
            bronze_df
            .withColumn("parsed_data", from_json(col("data"), json_schema))
            .select(explode(col("parsed_data")).alias("asset"), "ingestion_time")
        )
    elif dtype.startswith("array"):
        exploded_df = bronze_df.select(
            explode(col("data")).alias("asset"), "ingestion_time"
        )
    else:
        print(f"Unexpected data type for 'data' column: {dtype}")
        spark.stop()
        sys.exit(1)

    # -----------------------------------------------------------------------
    # Flatten struct, cast numeric fields, and include rank for Gold KPIs.
    # -----------------------------------------------------------------------
    # rank field only exists in data ingested after the schema update
    struct_fields = [f.name for f in exploded_df.schema["asset"].dataType.fields]
    rank_col = col("asset.rank").cast("integer").alias("rank") if "rank" in struct_fields \
               else lit(None).cast(IntegerType()).alias("rank")

    silver_df = exploded_df.select(
        col("asset.id"),
        rank_col,
        col("asset.symbol"),
        col("asset.name"),
        col("asset.priceUsd").cast("double").alias("priceUsd"),
        col("asset.marketCapUsd").cast("double").alias("marketCapUsd"),
        col("asset.volumeUsd24Hr").cast("double").alias("volumeUsd24Hr"),
        col("asset.changePercent24Hr").cast("double").alias("changePercent24Hr"),
        col("ingestion_time")
    )

    # -----------------------------------------------------------------------
    # Quality gate: drop rows with null / non-positive price or null symbol.
    # These records cannot contribute to any meaningful KPI downstream.
    # -----------------------------------------------------------------------
    before_filter = silver_df.count()
    silver_df = silver_df.filter(
        col("priceUsd").isNotNull() &
        col("symbol").isNotNull() &
        (col("priceUsd") > 0)
    )
    after_filter = silver_df.count()
    dropped = before_filter - after_filter
    if dropped > 0:
        print(f"Quality filter dropped {dropped} rows (null/zero price or null symbol).")

    # -----------------------------------------------------------------------
    # Deduplication: guard against Bronze containing duplicate snapshots for
    # the same asset at the same ingestion_time (e.g. from reprocessed files).
    # -----------------------------------------------------------------------
    before_dedup = after_filter
    silver_df = silver_df.dropDuplicates(["id", "ingestion_time"])
    after_dedup = silver_df.count()
    dupes_removed = before_dedup - after_dedup
    if dupes_removed > 0:
        print(f"Deduplication removed {dupes_removed} duplicate (id, ingestion_time) rows.")

    # -----------------------------------------------------------------------
    # Append only the new records — historical data is preserved in Silver.
    # -----------------------------------------------------------------------
    silver_df.write.mode("append").parquet(SILVER_PATH)
    print(f"Silver layer updated: {after_dedup} new records appended to {SILVER_PATH}.")
    print("Silver Layer processing completed successfully.")

except Exception as e:
    print(f"Error in Silver Layer: {e}")
    raise

finally:
    spark.stop()
