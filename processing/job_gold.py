from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, col, window, desc

spark = SparkSession.builder.appName("CryptoGoldLayer").getOrCreate()

SILVER_PATH = "/app/data/silver"
GOLD_PATH = "/app/data/gold"

try:
    silver_df = spark.read.parquet(SILVER_PATH)
    
    # Example KPI: Volatility Analysis (Max - Min price) over a global window or simple check
    # Since we might stick to batch, let's just do a simple aggregation of "Latest Snapshot" vs "Average"
    
    # 1. Latest Market Overview
    print(f"Silver DF count: {silver_df.count()}")
    
    latest_time_row = silver_df.agg(max("ingestion_time")).collect()
    latest_time = latest_time_row[0][0]
    print(f"Latest time in Silver: {latest_time}")
    
    if latest_time is None:
        print("Silver layer is empty or has no ingestion_time. Exiting Gold job.")
        spark.stop()
        exit(0)

    latest_snapshot = silver_df.filter(col("ingestion_time") == latest_time)
    print(f"Snapshot count: {latest_snapshot.count()}")
    
    print("DEBUG: Silver DF Schema:")
    silver_df.printSchema()
    print("DEBUG: Latest Snapshot Sample:")
    latest_snapshot.show(5)

    # 2. Crypto Volatility (e.g., if we had history)
    # For now, just rank by Market Cap
    top_assets = latest_snapshot.orderBy(col("marketCapUsd").desc())
    top_assets.show()
    
    # 3. Write Data
    # -----------------------------------------------
    # Aggregate Stats (e.g. Average Price of 'Bitcoin' if multiple ingestions exists)
    history_stats = silver_df.groupBy("symbol").agg(
        avg("priceUsd").alias("avg_price"),
        max("priceUsd").alias("max_price"),
        min("priceUsd").alias("min_price")
    )
    
    try:
        print(f"Writing Top 20 assets to {GOLD_PATH}/top_assets")
        # Removing coalesce to avoid shuffle issues.
        top_assets.write.mode("overwrite").parquet(f"{GOLD_PATH}/top_assets")
        print("Success: Top 20 assets written cmd executed.")
        
        # Verify immediately
        import os
        if os.path.exists(f"{GOLD_PATH}/top_assets"):
            print(f"VERIFICATION: Folder {GOLD_PATH}/top_assets exists. Content: {os.listdir(f'{GOLD_PATH}/top_assets')}")
        else:
            print(f"VERIFICATION: Folder {GOLD_PATH}/top_assets DOES NOT EXIST AFTER WRITE!")

        # -----------------------------------------------
        # 4. Write History Data (For Time Series/Candles)
        # -----------------------------------------------
        print(f"Writing Price History to {GOLD_PATH}/price_history")
        history_df = silver_df.select(
            col("ingestion_time"),
            col("symbol"),
            col("priceUsd"),
            col("volumeUsd24Hr"),
            col("marketCapUsd")
        )
        history_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/price_history")
        print("Success: Price History written.")

        print(f"Writing History Stats to {GOLD_PATH}/history_stats")
        history_stats.write.mode("overwrite").parquet(f"{GOLD_PATH}/history_stats")
        print("Success: History Stats written.")
        
    except Exception as w_err:
        print(f"CRITICAL ERROR WRITING GOLD DATA: {w_err}")
        raise w_err
    
    print("Gold Layer processing complete.")

except Exception as e:
    print(f"Error in Gold Layer: {e}")

spark.stop()
