from pyspark.sql import SparkSession

GOLD_PATH = "/app/data/gold/top_assets"
EXPORT_PATH = "/app/data/serving/powerbi_export"

# This script can be run by the orchestrator or manually
def main():
    spark = SparkSession.builder.appName("PowerBIExport").getOrCreate()
    
    try:
        print(f"Reading Gold data from {GOLD_PATH}")
        df = spark.read.parquet(GOLD_PATH)
        
        # Coalesce to 1 to get a single CSV file (easier for users)
        print(f"Exporting to CSV at {EXPORT_PATH}")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(EXPORT_PATH)
        
        print("Export successful.")
    except Exception as e:
        print(f"Error exporting for PowerBI: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
