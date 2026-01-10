from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CountCheck").getOrCreate()

try:
    silver_df = spark.read.parquet("/app/data/silver")
    print(f"SILVER_COUNT: {silver_df.count()}")
    silver_df.select("symbol").distinct().show()
except Exception as e:
    print(f"SILVER_ERROR: {e}")

try:
    gold_df = spark.read.parquet("/app/data/gold/top_assets")
    print(f"GOLD_COUNT: {gold_df.count()}")
    gold_df.select("symbol").distinct().show()
except Exception as e:
    print(f"GOLD_ERROR: {e}")

spark.stop()
