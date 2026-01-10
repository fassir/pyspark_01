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
    print("Starting Load to Postgres (Spark JDBC)...")
    
    # Initialize Spark Session
    # No need to hardcode jars if provided via spark-submit --jars
    spark = SparkSession.builder \
        .appName("GoldToPostgres") \
        .getOrCreate()
    
    try:
        print(f"Reading from: {GOLD_PATH}")
        
        # Read Gold Data (Parquet)
        # Check if path exists logic is tricky with lazy evaluation, 
        # but read will throw AnalysisException if missing.
        try:
            df = spark.read.parquet(GOLD_PATH)
        except Exception as e:
            print(f"WARNING: Gold Path could not be read. Ensure Gold job ran successfully. Error: {e}")
            spark.stop()
            return

        row_count = df.count()
        print(f"Loaded {row_count} rows from Gold Layer.")
        
        if row_count == 0:
            print("Gold layer is empty. Nothing to load.")
            spark.stop()
            return

        # Rename columns to snake_case for Postgres/Grafana compatibility
        jdbc_df = df.withColumnRenamed("priceUsd", "price_usd") \
                    .withColumnRenamed("marketCapUsd", "market_cap_usd") \
                    .withColumnRenamed("volumeUsd24Hr", "volume_usd_24hr") \
                    .withColumnRenamed("changePercent24Hr", "change_percent_24hr")
        
        print("Schema optimized for Postgres:")
        jdbc_df.printSchema()

        # Debug: Check for diversity
        distinct_syms = jdbc_df.select("symbol").distinct().collect()
        sym_list = [r["symbol"] for r in distinct_syms]
        print(f"DEBUG: Distinct Symbols in Dataframe: {sym_list}")

        # JDBC Configuration
        jdbc_url = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"
        properties = {
            "user": DB_USER,
            "password": DB_PASS,
            "driver": "org.postgresql.Driver"
        }
        
        # Explicit Truncate via Spark? 
        # Spark JDBC 'truncate' option only works with Overwrite mode, but we suspect Overwrite is buggy here.
        # Let's try 'overwrite' with 'truncate' option explicitly set to true.
        # If that fails, we can't easily run raw SQL from Spark without a separate driver connection.
        # Given we removed psycopg2 dependency... 
        
        # We will trust Spark but add the option.
        print(f"Writing to JDBC: {jdbc_url} with Truncate")
        
        jdbc_df.write \
            .option("truncate", "true") \
            .jdbc(url=jdbc_url, table="crypto_top_assets", mode="overwrite", properties=properties)
            
        print("Data loaded to 'crypto_top_assets' successfully.")

        # ---------------------------------------------------------
        # Load History Data
        # ---------------------------------------------------------
        HISTORY_PATH = "/app/data/gold/price_history"
        print(f"Reading History from: {HISTORY_PATH}")
        try:
            history_df = spark.read.parquet(HISTORY_PATH)
            
            # Rename for Postgres
            pg_history_df = history_df.withColumnRenamed("priceUsd", "price_usd") \
                                      .withColumnRenamed("volumeUsd24Hr", "volume_usd_24hr") \
                                      .withColumnRenamed("marketCapUsd", "market_cap_usd")
            
            print("History Schema:")
            pg_history_df.printSchema()
            
            # Write to crypto_price_history
            # We use 'overwrite' here for simplicity in this demo (re-loading all history). 
            # In prod, 'append' with deduplication or upsert is better.
            print(f"Writing to JDBC table 'crypto_price_history'")
            pg_history_df.write \
                .jdbc(url=jdbc_url, table="crypto_price_history", mode="overwrite", properties=properties)
            
            print("Data loaded to 'crypto_price_history' successfully.")
            
        except Exception as h_e:
            print(f"Warning: Could not load history data. {h_e}")

    except Exception as e:
        print(f"Error loading to Postgres: {e}")
        # Exit with error code to alert orchestrator/docker
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()
