# =============================================================
# 03 — GOLD AGGREGATION
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Reads from Silver Delta table and computes real-time business
# KPIs using 5-minute tumbling windows with 10-minute watermark.
# Produces revenue, order volumes, cancellation and return rates
# by region and category — powering the live operations dashboard.
#
# Layer    : Gold (business KPIs — dashboard ready)
# Trigger  : 60-second micro-batch
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
# STEP 1 — Set up database
# ------------------------------------------------------------------
spark.sql("USE CATALOG hive_metastore")
spark.sql("CREATE DATABASE IF NOT EXISTS akhilstream_db")
spark.sql("USE DATABASE akhilstream_db")
print("✅ Database ready:", spark.catalog.currentDatabase())

# ------------------------------------------------------------------
# STEP 2 — Verify Silver table has data before starting
# ------------------------------------------------------------------
silver_count = spark.table("hive_metastore.akhilstream_db.silver_order_events").count()
print(f"✅ Silver table has {silver_count:,} rows — ready to process")

if silver_count == 0:
    raise ValueError(
        "❌ Silver table is empty!\n"
        "Make sure notebook 02_silver_transform is running first\n"
        "and wait at least 2 minutes before running this notebook."
    )

# ------------------------------------------------------------------
# STEP 3 — Clear old checkpoint (first run or after error)
# ------------------------------------------------------------------
gold_checkpoint = "dbfs:/shopstream/checkpoints/gold_orders"

try:
    dbutils.fs.rm(gold_checkpoint, recurse=True)
    print(f"✅ Cleared checkpoint: {gold_checkpoint}")
except:
    print(f"ℹ️  No existing checkpoint found — fresh start")

dbutils.fs.mkdirs(gold_checkpoint)
print("✅ Checkpoint directory created:", gold_checkpoint)

# ------------------------------------------------------------------
# STEP 4 — Read stream from Silver Delta table
# ------------------------------------------------------------------
silver_stream = (
    spark.readStream
    .format("delta")
    .table("hive_metastore.akhilstream_db.silver_order_events")
)

print("✅ Silver stream reader created")

# ------------------------------------------------------------------
# STEP 5 — Compute KPIs using 5-minute tumbling windows
#
#    withWatermark — waits 10 minutes for late-arriving events
#    before finalising each window. Prevents unbounded state
#    growth in long-running streaming jobs.
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
        count("*").alias("total_orders"),
        approx_count_distinct("customer_id").alias("unique_customers"),
        sum("order_value").alias("total_revenue"),
        avg("order_value").alias("avg_order_value"),
        sum(when(col("order_status") == "DELIVERED",  1).otherwise(0)).alias("delivered_count"),
        sum(when(col("order_status") == "CANCELLED",  1).otherwise(0)).alias("cancelled_count"),
        sum(when(col("is_returned")  == True,         1).otherwise(0)).alias("returned_count"),
        sum("quantity").alias("total_units_sold")
    )
    .withColumn("total_revenue",          round(col("total_revenue"),   2))
    .withColumn("avg_order_value",        round(col("avg_order_value"), 2))
    .withColumn("cancellation_rate_pct",  round((col("cancelled_count") / col("total_orders")) * 100, 2))
    .withColumn("return_rate_pct",        round((col("returned_count")  / col("total_orders")) * 100, 2))
    .withColumn("delivery_rate_pct",      round((col("delivered_count") / col("total_orders")) * 100, 2))
)

print("✅ Gold KPI aggregations defined")

# ------------------------------------------------------------------
# STEP 6 — Write Gold stream
#    append mode — emits windows after watermark passes (required for Delta)
# ------------------------------------------------------------------
print("\n🚀 Starting Gold streaming job...")
print("   Checkpoint : dbfs:/shopstream/checkpoints/gold_orders")
print("   Table      : akhilstream_db.gold_order_kpis")
print("   Trigger    : 60 seconds")
print("   Output mode: append (windowed aggregation with watermark)")

gold_query = (
    gold_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", gold_checkpoint)
    .toTable("hive_metastore.akhilstream_db.gold_order_kpis")
)

# ------------------------------------------------------------------
# STEP 7 — Monitor stream status
# ------------------------------------------------------------------
import time
time.sleep(10)

print("\n📊 Gold stream status:")
print("   Is active:", gold_query.isActive)
print("   Message  :", gold_query.status["message"])

print("\n⏳ Wait ~5 minutes then verify:")
print("   SELECT * FROM akhilstream_db.gold_order_kpis ORDER BY window DESC LIMIT 10")

# ------------------------------------------------------------------
# Sample output after ~5 minutes:
# SELECT * FROM akhilstream_db.gold_order_kpis ORDER BY window DESC LIMIT 5;
#
# +---------------------------+--------+-------------+--------------+-----------------+---------------+------------------+-------------------+------------------+
# | window                    | region | category    | total_orders | unique_customers | total_revenue | avg_order_value  | cancellation_rate | return_rate_pct  |
# +---------------------------+--------+-------------+--------------+-----------------+---------------+------------------+-------------------+------------------+
# | 2025-07-03 10:40–10:45   | WEST   | ELECTRONICS | 423          | 389             | 84,231.77     | 199.13           | 3.31              | 4.49             |
# | 2025-07-03 10:40–10:45   | NORTH  | FASHION     | 891          | 812             | 31,540.09     | 35.40            | 5.05              | 6.17             |
# | 2025-07-03 10:40–10:45   | SOUTH  | GROCERY     | 1204         | 1087            | 18,922.40     | 15.72            | 2.16              | 1.91             |
# +---------------------------+--------+-------------+--------------+-----------------+---------------+------------------+-------------------+------------------+
# ------------------------------------------------------------------
