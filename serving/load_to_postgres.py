import sys
import os
from pyspark.sql import SparkSession

# Configuration — env vars with safe defaults for local Docker Compose
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/cryptodb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
GOLD_PATH = "/app/data/gold"

JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# (parquet_subpath, pg_table, col_renames_dict, write_mode)
TABLES = [
    (
        "top_assets",
        "crypto_top_assets",
        {
            "priceUsd": "price_usd",
            "marketCapUsd": "market_cap_usd",
            "volumeUsd24Hr": "volume_usd_24hr",
            "changePercent24Hr": "change_percent_24hr",
        },
        "overwrite",
    ),
    (
        "price_history",
        "crypto_price_history",
        {
            "priceUsd": "price_usd",
            "marketCapUsd": "market_cap_usd",
            "volumeUsd24Hr": "volume_usd_24hr",
        },
        "overwrite",  # demo mode; change to "append" in production
    ),
    (
        "volatility",
        "crypto_volatility",
        {},  # already snake_case: symbol, price_stddev, avg_price, price_range_pct, volatility_pct
        "overwrite",
    ),
    (
        "price_changes",
        "crypto_price_changes",
        {
            "priceUsd": "price_usd",
        },
        "overwrite",
    ),
    (
        "moving_averages",
        "crypto_moving_averages",
        {
            "priceUsd": "price_usd",
        },
        "overwrite",
    ),
    (
        "market_dominance",
        "crypto_market_dominance",
        {
            "marketCapUsd": "market_cap_usd",
        },
        "overwrite",
    ),
    (
        "liquidity",
        "crypto_liquidity",
        {
            "volumeUsd24Hr": "volume_usd_24hr",
            "marketCapUsd": "market_cap_usd",
        },
        "overwrite",
    ),
    (
        "anomalies",
        "crypto_anomalies",
        {
            "priceUsd": "price_usd",
        },
        "overwrite",
    ),
    (
        "history_stats",
        "crypto_history_stats",
        {},  # already snake_case: symbol, avg_price, max_price, min_price
        "overwrite",
    ),
]


def load_table(spark, parquet_subpath, table_name, col_renames, mode):
    parquet_path = f"{GOLD_PATH}/{parquet_subpath}"
    try:
        df = spark.read.parquet(parquet_path)

        # Apply column renames (camelCase → snake_case)
        for old_name, new_name in col_renames.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        row_count = df.count()

        df.write \
            .jdbc(
                url=POSTGRES_URL,
                table=table_name,
                mode=mode,
                properties=JDBC_PROPERTIES,
            )

        print(f"  OK  {table_name:<35} {row_count:>6} rows  [{mode}]")
        return True

    except Exception as e:
        print(f"  FAIL {table_name:<34} — {e}")
        return False


def main():
    print("=" * 60)
    print("Starting Load to Postgres (Spark JDBC) — 9 tables")
    print(f"Target: {POSTGRES_URL}")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("GoldToPostgres") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    results = {}
    for parquet_subpath, table_name, col_renames, mode in TABLES:
        success = load_table(spark, parquet_subpath, table_name, col_renames, mode)
        results[table_name] = success

    spark.stop()

    # Summary
    total = len(results)
    passed = sum(1 for ok in results.values() if ok)
    failed_tables = [t for t, ok in results.items() if not ok]

    print("=" * 60)
    if passed == total:
        print(f"SUCCESS: Loaded {passed}/{total} tables successfully.")
    else:
        print(f"WARNING: {passed}/{total} tables loaded. Failed: {failed_tables}")
        sys.exit(1)
    print("=" * 60)


if __name__ == "__main__":
    main()
