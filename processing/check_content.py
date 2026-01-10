from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ContentCheck").getOrCreate()

print("="*50)
print("CHECKING SILVER CONTENT")
try:
    silver_df = spark.read.parquet("/app/data/silver")
    print(f"Total Rows: {silver_df.count()}")
    silver_df.select("symbol", "name", "priceUsd").show(20, truncate=False)
except Exception as e:
    print(f"Error reading Silver: {e}")

print("="*50)
print("CHECKING GOLD CONTENT")
try:
    gold_df = spark.read.parquet("/app/data/gold/top_assets")
    print(f"Total Rows: {gold_df.count()}")
    gold_df.select("symbol", "name", "priceUsd").show(20, truncate=False) # Note snake_case or CamelCase depending on Gold job
    # Gold job writes top_assets from latest_snapshot which comes from Silver (CamelCase)
    # BUT Gold job REWRITES? No, top_assets IS latest_snapshot (filtered).
    # Gold job does NOT rename columns for top_assets. Loader does.
    # So Gold has CamelCase.
    # Verify cols
    print("Gold Columns:", gold_df.columns)
    gold_df.select("symbol", "name", "priceUsd").show(20, truncate=False)
except Exception as e:
    print(f"Error reading Gold: {e}")

print("="*50)
spark.stop()
