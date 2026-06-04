from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, max, min, col, desc, stddev, lag,
    round as spark_round,
    sum as spark_sum,
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("CryptoGoldLayer").getOrCreate()

SILVER_PATH = "/app/data/silver"
GOLD_PATH = "/app/data/gold"

# ---------------------------------------------------------------------------
# Read Silver layer
# ---------------------------------------------------------------------------
try:
    silver_df = spark.read.parquet(SILVER_PATH)
    print(f"Silver record count: {silver_df.count()}")
except Exception as e:
    print(f"CRITICAL: Cannot read Silver layer — {e}")
    spark.stop()
    exit(1)

# ---------------------------------------------------------------------------
# Resolve latest snapshot (used by several KPIs)
# ---------------------------------------------------------------------------
latest_time_row = silver_df.agg(max("ingestion_time")).collect()
latest_time = latest_time_row[0][0]

if latest_time is None:
    print("Silver layer is empty (no ingestion_time). Exiting Gold job.")
    spark.stop()
    exit(0)

print(f"Latest ingestion_time: {latest_time}")
latest_snapshot = silver_df.filter(col("ingestion_time") == latest_time)
print(f"Latest snapshot count: {latest_snapshot.count()}")

# ---------------------------------------------------------------------------
# KPI 1 — top_assets
# Latest snapshot ordered by market cap descending.
# ---------------------------------------------------------------------------
top_assets = latest_snapshot.orderBy(col("marketCapUsd").desc())

try:
    top_assets.write.mode("overwrite").parquet(f"{GOLD_PATH}/top_assets")
    print("SUCCESS: top_assets written.")
except Exception as e:
    print(f"ERROR writing top_assets: {e}")

# ---------------------------------------------------------------------------
# KPI 2 — price_history
# Full time-series: ingestion_time, symbol, price, volume, market cap.
# ---------------------------------------------------------------------------
price_history_df = silver_df.select(
    "ingestion_time", "symbol", "priceUsd", "volumeUsd24Hr", "marketCapUsd"
)

try:
    price_history_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/price_history")
    print("SUCCESS: price_history written.")
except Exception as e:
    print(f"ERROR writing price_history: {e}")

# ---------------------------------------------------------------------------
# KPI 3 — volatility
# Per-symbol: stddev, avg_price, price_range_pct = (max-min)/avg*100,
# and volatility_pct = (stddev/avg)*100 — coefficient of variation.
# ---------------------------------------------------------------------------
volatility_df = silver_df.groupBy("symbol").agg(
    stddev("priceUsd").alias("price_stddev"),
    avg("priceUsd").alias("avg_price"),
    ((max("priceUsd") - min("priceUsd")) / avg("priceUsd") * 100).alias("price_range_pct"),
    spark_round(stddev("priceUsd") / avg("priceUsd") * 100, 4).alias("volatility_pct"),
).orderBy(col("volatility_pct").desc())

try:
    volatility_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/volatility")
    print("SUCCESS: volatility written.")
except Exception as e:
    print(f"ERROR writing volatility: {e}")

# ---------------------------------------------------------------------------
# KPI 4 — price_changes
# Row-by-row price change % using Window lag(1) per symbol ordered by time.
# First row per symbol (no previous price) is excluded.
# ---------------------------------------------------------------------------
w_ordered = Window.partitionBy("symbol").orderBy("ingestion_time")

price_changes_df = (
    silver_df
    .withColumn("prev_price", lag("priceUsd", 1).over(w_ordered))
    .withColumn(
        "price_change_pct",
        spark_round(
            (col("priceUsd") - col("prev_price")) / col("prev_price") * 100,
            4,
        ),
    )
    .filter(col("prev_price").isNotNull())
    .select("ingestion_time", "symbol", "priceUsd", "prev_price", "price_change_pct")
)

try:
    price_changes_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/price_changes")
    print("SUCCESS: price_changes written.")
except Exception as e:
    print(f"ERROR writing price_changes: {e}")

# ---------------------------------------------------------------------------
# KPI 5 — moving_averages
# MA7 = rolling average over last 7 rows (inclusive), MA14 over last 14 rows.
# ---------------------------------------------------------------------------
w7 = Window.partitionBy("symbol").orderBy("ingestion_time").rowsBetween(-6, 0)
w14 = Window.partitionBy("symbol").orderBy("ingestion_time").rowsBetween(-13, 0)

ma_df = (
    silver_df
    .withColumn("ma_7", spark_round(avg("priceUsd").over(w7), 4))
    .withColumn("ma_14", spark_round(avg("priceUsd").over(w14), 4))
    .select("ingestion_time", "symbol", "priceUsd", "ma_7", "ma_14")
)

try:
    ma_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/moving_averages")
    print("SUCCESS: moving_averages written.")
except Exception as e:
    print(f"ERROR writing moving_averages: {e}")

# ---------------------------------------------------------------------------
# KPI 6 — market_dominance
# Each coin's % share of total market cap in the latest snapshot.
# Uses a scalar collect() to avoid a full crossJoin shuffle.
# ---------------------------------------------------------------------------
total_cap_row = latest_snapshot.agg(
    spark_sum("marketCapUsd").alias("total_market_cap")
).collect()[0]
total_cap = total_cap_row["total_market_cap"]

if total_cap and total_cap > 0:
    dominance_df = (
        latest_snapshot
        .withColumn(
            "market_dominance_pct",
            spark_round(col("marketCapUsd") / total_cap * 100, 2),
        )
        .select("symbol", "name", "marketCapUsd", "market_dominance_pct")
        .orderBy(col("market_dominance_pct").desc())
    )
    try:
        dominance_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/market_dominance")
        print("SUCCESS: market_dominance written.")
    except Exception as e:
        print(f"ERROR writing market_dominance: {e}")
else:
    print("SKIP: market_dominance — total market cap is zero or null.")

# ---------------------------------------------------------------------------
# KPI 7 — liquidity
# Volume-to-MarketCap ratio for the latest snapshot.
# High ratio → more liquid asset relative to its size.
# ---------------------------------------------------------------------------
liquidity_df = (
    latest_snapshot
    .withColumn(
        "liquidity_ratio",
        spark_round(col("volumeUsd24Hr") / col("marketCapUsd"), 4),
    )
    .select("symbol", "name", "volumeUsd24Hr", "marketCapUsd", "liquidity_ratio")
    .orderBy(col("liquidity_ratio").desc())
)

try:
    liquidity_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/liquidity")
    print("SUCCESS: liquidity written.")
except Exception as e:
    print(f"ERROR writing liquidity: {e}")

# ---------------------------------------------------------------------------
# KPI 8 — anomalies
# Z-score = (price - symbol_avg) / symbol_stddev using an unbounded window.
# Rows where z_score > 2.0 are flagged as anomalies.
# Written with mode overwrite for idempotency (use append in production).
# ---------------------------------------------------------------------------
w_all = Window.partitionBy("symbol")

anomaly_df = (
    silver_df
    .withColumn("global_avg", avg("priceUsd").over(w_all))
    .withColumn("global_std", stddev("priceUsd").over(w_all))
    .withColumn(
        "z_score",
        spark_round(
            (col("priceUsd") - col("global_avg")) / col("global_std"),
            4,
        ),
    )
    .withColumn("is_anomaly", col("z_score") > 2.0)
    .filter(col("is_anomaly") == True)  # noqa: E712  (Spark column comparison)
    .select("ingestion_time", "symbol", "priceUsd", "global_avg", "z_score", "is_anomaly")
)

try:
    anomaly_df.write.mode("overwrite").parquet(f"{GOLD_PATH}/anomalies")
    print("SUCCESS: anomalies written.")
except Exception as e:
    print(f"ERROR writing anomalies: {e}")

# ---------------------------------------------------------------------------
# KPI 9 — history_stats
# Per-symbol avg / max / min price aggregates across all ingestions.
# ---------------------------------------------------------------------------
history_stats = silver_df.groupBy("symbol").agg(
    avg("priceUsd").alias("avg_price"),
    max("priceUsd").alias("max_price"),
    min("priceUsd").alias("min_price"),
)

try:
    history_stats.write.mode("overwrite").parquet(f"{GOLD_PATH}/history_stats")
    print("SUCCESS: history_stats written.")
except Exception as e:
    print(f"ERROR writing history_stats: {e}")

# ---------------------------------------------------------------------------
print("Gold Layer processing complete.")
spark.stop()
