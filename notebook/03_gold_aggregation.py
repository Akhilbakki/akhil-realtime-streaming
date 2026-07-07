# =============================================================
# 03 — GOLD AGGREGATION
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Reads from Silver Delta table and computes real-time business
# KPIs using 5-minute tumbling windows with 10-minute watermark.
# Single Gold table covering revenue, volume, and order health
# metrics — fast, simple, and dashboard-ready.
#
# Layer    : Gold (business KPIs — dashboard ready)
# Trigger  : 30-second micro-batch
# Window   : 5-minute tumbling window on event_ts
# Watermark: 10 minutes (handles late-arriving events)
# Output   : akhilstream_db.gold_order_kpis (Delta, update mode)
# Tech     : PySpark · Delta Lake · Windowed Aggregations
# Author   : Akhil Bakki
# =============================================================

from pyspark.sql.functions import (
    col, window, count, sum, avg,
    when, round, approx_count_distinct
)

# ------------------------------------------------------------------
# STEP 1 — ADLS Gen2 + Database configuration
# ------------------------------------------------------------------
STORAGE_ACCOUNT = dbutils.secrets.get("shopstream-scope", "adls-account-name")
ACCESS_KEY       = dbutils.secrets.get("shopstream-scope", "adls-access-key")
CONTAINER        = "shopstream-data"

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    ACCESS_KEY
)

BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_CP   = f"{BASE_PATH}/checkpoints/gold_orders"

spark.sql("USE CATALOG hive_metastore")
spark.sql("CREATE DATABASE IF NOT EXISTS akhilstream_db")
spark.sql("USE DATABASE akhilstream_db")

print("✅ ADLS Gen2 configured :", BASE_PATH)
print("✅ Database ready       :", spark.catalog.currentDatabase())

# ------------------------------------------------------------------
# STEP 2 — Spark performance tuning
# ------------------------------------------------------------------
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

print("✅ Spark AQE + performance tuning applied")

# ------------------------------------------------------------------
# STEP 3 — Verify Silver table has data
# ------------------------------------------------------------------
silver_count = spark.table("hive_metastore.akhilstream_db.silver_order_events").count()
print(f"✅ Silver table has {silver_count:,} rows")

if silver_count == 0:
    raise ValueError(
        "❌ Silver table is empty!\n"
        "Run notebook 02_silver_transform first and wait 2 minutes."
    )

# ------------------------------------------------------------------
# STEP 4 — Clear old checkpoint
# ------------------------------------------------------------------
try:
    dbutils.fs.rm(GOLD_CP, recurse=True)
    print(f"✅ Cleared checkpoint: {GOLD_CP}")
except:
    print("ℹ️  No existing checkpoint — fresh start")

dbutils.fs.mkdirs(GOLD_CP)
print("✅ Checkpoint directory created")

# ------------------------------------------------------------------
# STEP 5 — Read stream from Silver Delta table
# ------------------------------------------------------------------
silver_stream = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", "10")
    .table("hive_metastore.akhilstream_db.silver_order_events")
)

print("✅ Silver stream reader created")

# ------------------------------------------------------------------
# STEP 6 — Gold KPIs
#    append mode with watermark — window written once finalised
#    watermark of 10 min means window written ~10 min after it closes
# ------------------------------------------------------------------
gold_df = (
    silver_stream
    .withWatermark("event_ts", "10 minutes")
    .groupBy(
        window(col("event_ts"), "5 minutes"),
        col("region"),
        col("category")
    )
    .agg(
        # Volume
        count("*").alias("total_orders"),
        approx_count_distinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_units_sold"),

        # Revenue
        round(sum("order_value"),  2).alias("total_revenue"),
        round(avg("order_value"),  2).alias("avg_order_value"),

        # Order status counts
        sum(when(col("order_status") == "DELIVERED",  1).otherwise(0)).alias("delivered_count"),
        sum(when(col("order_status") == "CANCELLED",  1).otherwise(0)).alias("cancelled_count"),

        # Returns
        sum(when(col("is_returned") == True, 1).otherwise(0)).alias("returned_count"),
    )
    .withColumn("delivery_rate_pct",
        round((col("delivered_count") / col("total_orders")) * 100, 2))
    .withColumn("cancellation_rate_pct",
        round((col("cancelled_count") / col("total_orders")) * 100, 2))
    .withColumn("return_rate_pct",
        round((col("returned_count")  / col("total_orders")) * 100, 2))
)

print("✅ Gold KPIs defined — append mode with watermark finalisation")

# ------------------------------------------------------------------
# STEP 7 — Write Gold stream
#    append mode — window row written once watermark passes it
#    exactly-once — no duplicates guaranteed by checkpoint
# ------------------------------------------------------------------
print("\n🚀 Starting Gold streaming job...")
print(f"   Checkpoint  : {GOLD_CP}")
print(f"   Table       : akhilstream_db.gold_order_kpis")
print(f"   Trigger     : 30 seconds")
print(f"   Output mode : append (window finalised after watermark)")
print(f"   Window      : 5 minutes with 10-min watermark")

GOLD_DELTA_PATH = f"{BASE_PATH}/delta/gold"

gold_query = (
    gold_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", GOLD_CP)
    .option("mergeSchema", "true")
    .start(GOLD_DELTA_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS hive_metastore.akhilstream_db.gold_order_kpis
    USING DELTA LOCATION '{GOLD_DELTA_PATH}'
""")

# ------------------------------------------------------------------
# STEP 8 — Monitor stream status
# ------------------------------------------------------------------
import time
time.sleep(15)

print("\n📊 Gold stream status:")
print("   Is active:", gold_query.isActive)
print("   Message  :", gold_query.status["message"])

print("\n⏳ Verify after ~5 minutes:")
print("   SELECT * FROM akhilstream_db.gold_order_kpis ORDER BY window DESC LIMIT 10")

# ------------------------------------------------------------------
# Sample output after ~5 minutes:
#
# SELECT * FROM akhilstream_db.gold_order_kpis
# ORDER BY window DESC, total_revenue DESC LIMIT 5;
#
# +---------------------------+--------+-------------+--------------+-----------------+---------------+-----------------+-------------------+------------------+--------------------+
# | window                    | region | category    | total_orders | unique_customers | total_revenue | avg_order_value | delivery_rate_pct | cancel_rate_pct  | return_rate_pct    |
# +---------------------------+--------+-------------+--------------+-----------------+---------------+-----------------+-------------------+------------------+--------------------+
# | 2025-07-07 10:40–10:45   | WEST   | ELECTRONICS | 423          | 389             | 84,231.77     | 199.13          | 78.25             | 3.31             | 4.49               |
# | 2025-07-07 10:40–10:45   | NORTH  | FASHION     | 891          | 812             | 31,540.09     | 35.40           | 72.50             | 5.05             | 6.17               |
# | 2025-07-07 10:40–10:45   | SOUTH  | GROCERY     | 1204         | 1087            | 18,922.40     | 15.72           | 89.12             | 2.16             | 1.91               |
# | 2025-07-07 10:40–10:45   | EAST   | HOME        | 318          | 298             | 12,441.20     | 39.12           | 80.50             | 4.40             | 3.77               |
# +---------------------------+--------+-------------+--------------+-----------------+---------------+-----------------+-------------------+------------------+--------------------+
# ------------------------------------------------------------------
