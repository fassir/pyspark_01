import json
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CryptoQualityChecks") \
    .getOrCreate()

SILVER_PATH = "/app/data/silver"
QUALITY_REPORTS_PATH = "/app/data/quality_reports"


def run_quality_checks(spark, df, layer_name):
    """
    Run data quality checks on a DataFrame and write a JSON report.

    Raises ValueError (causing sys.exit(1) in the caller) if any critical
    check fails (null prices, null symbols, or negative prices).
    """
    total = df.count()
    if total == 0:
        raise ValueError(f"CRITICAL: {layer_name} has 0 records")

    nulls_price = df.filter(col("priceUsd").isNull()).count()
    nulls_symbol = df.filter(col("symbol").isNull()).count()
    negative_price = df.filter(col("priceUsd") < 0).count()
    distinct_symbols = df.select("symbol").distinct().count()

    passed = (nulls_price == 0 and negative_price == 0 and nulls_symbol == 0)

    report = {
        "layer": layer_name,
        "run_at": datetime.now().isoformat(),
        "total_records": total,
        "distinct_symbols": distinct_symbols,
        "null_price_count": nulls_price,
        "null_price_pct": round(nulls_price / total * 100, 2),
        "null_symbol_count": nulls_symbol,
        "null_symbol_pct": round(nulls_symbol / total * 100, 2),
        "negative_price_count": negative_price,
        "passed": passed,
    }

    os.makedirs(QUALITY_REPORTS_PATH, exist_ok=True)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(QUALITY_REPORTS_PATH, f"{layer_name}_{timestamp_str}.json")
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Quality report written to: {report_file}")
    print(f"Quality Report for {layer_name}: {report}")

    if not passed:
        raise ValueError(f"Quality check FAILED for {layer_name}: {report}")

    return report


# --- Main ---
try:
    print(f"Reading Silver layer from {SILVER_PATH}")
    silver_df = spark.read.parquet(SILVER_PATH)

    run_quality_checks(spark, silver_df, "silver")

    print("Quality checks PASSED")

except ValueError as e:
    # Critical quality failure — exit with non-zero so the orchestrator can detect it
    print(f"QUALITY CHECK FAILURE: {e}")
    spark.stop()
    sys.exit(1)

except Exception as e:
    print(f"Error in Quality Check job: {e}")
    spark.stop()
    sys.exit(1)

spark.stop()
