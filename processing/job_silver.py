import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, to_timestamp
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

spark = SparkSession.builder.appName("CryptoSilverLayer").getOrCreate()

BRONZE_PATH = "/app/data/bronze"
SILVER_PATH = "/app/data/silver"

# CoinCap 'Asset' Schema
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

# The main JSON often has a "data" array field. 
# We need to see how Spark inferred the bronze schema.
# Assuming Bronze has the 'data' column as an Array of Structs (if schema inference worked) 
# OR we might have to clean it up depending on how "job_bronze" saved it.

try:
    bronze_df = spark.read.parquet(BRONZE_PATH)
    
    # Explore the schema. If 'data' is an array, explode it.
    if "data" in bronze_df.columns:
        print("Bronze DataFrame Schema:")
        bronze_df.printSchema()
        
        # Check data type of 'data' column
        dtype = dict(bronze_df.dtypes)["data"]
        print(f"'data' column type: {dtype}")
        
        if dtype.startswith("string"):
             # If String, parse it
             json_schema = ArrayType(asset_schema)
             exploded_df = bronze_df.withColumn("parsed_data", from_json(col("data"), json_schema)) \
                                    .select(explode(col("parsed_data")).alias("asset"), "ingestion_time")
        elif dtype.startswith("array"):
             # If Array, explode directly
             exploded_df = bronze_df.select(explode(col("data")).alias("asset"), "ingestion_time")
        else:
             print(f"Unexpected data type for 'data': {dtype}")
             sys.exit(1)

        # Flatten the struct
        silver_df = exploded_df.select(
            col("asset.id"),
            col("asset.symbol"),
            col("asset.name"),
            col("asset.priceUsd").cast("double").alias("priceUsd"),
            col("asset.marketCapUsd").cast("double").alias("marketCapUsd"),
            col("asset.volumeUsd24Hr").cast("double").alias("volumeUsd24Hr"),
            col("asset.changePercent24Hr").cast("double").alias("changePercent24Hr"),
            col("ingestion_time")
        )
        
        # Deduplication: Keep only the latest entry per ID per ingestion batch
        # silver_df = silver_df.dropDuplicates(["id", "ingestion_time"]) 
        # (Actually, we want time-series, so we keep all ingestion points, but maybe dedup if we re-ran)
        
        print(f"Writing to {SILVER_PATH}")
        silver_df.write.mode("overwrite").parquet(SILVER_PATH) 
        # using overwrite for demo simplicity, usually 'append' with partitionBy date
        print("Silver Layer processing completed successfully.")

    else:
        print("Column 'data' not found in Bronze. Schema might be different.")
        bronze_df.printSchema()

except Exception as e:
    print(f"Error in Silver Layer: {e}")

spark.stop()
