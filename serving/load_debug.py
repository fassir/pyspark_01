import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuration
DB_HOST = "postgres" # Service name in Docker Compose
DB_NAME = "cryptodb"
DB_USER = "admin"
DB_PASS = "password"

GOLD_PATH = "/app/data/gold/top_assets"

def main():
    print("Starting Load to Postgres (Debug Mode)...")
    
    spark = SparkSession.builder \
        .appName("GoldToPostgresDebug") \
        .getOrCreate()
    
    try:
        print(f"Reading from: {GOLD_PATH}")
        df = spark.read.parquet(GOLD_PATH)
        row_count = df.count()
        print(f"Loaded {row_count} rows from Gold Layer.")
        
        # Rename columns
        jdbc_df = df.withColumnRenamed("priceUsd", "price_usd") \
                    .withColumnRenamed("marketCapUsd", "market_cap_usd") \
                    .withColumnRenamed("volumeUsd24Hr", "volume_usd_24hr") \
                    .withColumnRenamed("changePercent24Hr", "change_percent_24hr")
        
        print("DEBUG: Content to be written to JDBC:")
        distinct_syms = jdbc_df.select("symbol").distinct().collect()
        print(f"DISTINCT SYMBOLS: {[r['symbol'] for r in distinct_syms]}")
        
        # JDBC Configuration
        jdbc_url = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"
        properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "org.postgresql.Driver"
        }
        
        print(f"Writing to JDBC: {jdbc_url}")
        
        # jdbc_df.write \
        #     .jdbc(url=jdbc_url, table="crypto_top_assets", mode="overwrite", properties=properties)
            
        print("Data loaded to Postgres successfully via JDBC (SKIPPED).")
        
    except Exception as e:
        print(f"Error loading to Postgres: {e}")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()
