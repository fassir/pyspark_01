from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("DataDiagnostic").getOrCreate()

# Write results to a file for clean retrieval
with open("/app/data/diagnostic_summary.txt", "w") as f:
    f.write("="*50 + "\n")
    f.write("DIAGNOSTIC REPORT\n")
    f.write("="*50 + "\n")

    # Check Bronze
    try:
        f.write("\n--- BRONZE LAYER ---\n")
        bronze_df = spark.read.parquet("/app/data/bronze")
        count = bronze_df.count()
        f.write(f"Bronze Count: {count}\n")
        # bronze_df.printSchema() # Can't redirect easily, skip or use df.schema.simpleString()
        f.write(f"Schema: {bronze_df.schema.simpleString()}\n")
    except Exception as e:
        f.write(f"Error reading Bronze: {e}\n")

    # Check Silver
    try:
        f.write("\n--- SILVER LAYER ---\n")
        silver_df = spark.read.parquet("/app/data/silver")
        count = silver_df.count()
        f.write(f"Silver Count: {count}\n")
        
        distinct_symbols = silver_df.select("symbol").distinct().collect()
        symbols = [row["symbol"] for row in distinct_symbols]
        f.write(f"Distinct Symbols in Silver: {symbols}\n")
    except Exception as e:
        f.write(f"Error reading Silver: {e}\n")

    # Check Gold
    try:
        f.write("\n--- GOLD LAYER ---\n")
        gold_df = spark.read.parquet("/app/data/gold/top_assets")
        count = gold_df.count()
        f.write(f"Gold Count: {count}\n")
        
        distinct_symbols = gold_df.select("symbol").distinct().collect()
        symbols = [row["symbol"] for row in distinct_symbols]
        f.write(f"Distinct Symbols in Gold: {symbols}\n")
    except Exception as e:
        f.write(f"Error reading Gold: {e}\n")

    f.write("="*50 + "\n")
spark.stop()
