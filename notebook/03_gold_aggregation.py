# =============================================================
# 03 — GOLD AGGREGATION
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Reads from Silver Delta table and computes real-time business
# KPIs using 5-minute tumbling windows with a 10-minute watermark
# for late-arriving events. Produces revenue metrics, order
# volumes, return rates and top categories by region — powering
# a live e-commerce operations dashboard.
#
# Layer    : Gold (business KPIs — dashboard ready)
# Trigger  : 60-second micro-batch
# Window   : 5-minute tumbling window on event_ts
# Watermark: 10 minutes (handles late-arriving events)
# Output   : shopstream_db.gold_order_kpis (Delta, update mode)
# Tech     : PySpark · Delta Lake · Windowed Aggregations
# Author   : Akhil Bakki
# GitHub   : github.com/akhilbakki/shopstream-realtime-pipeline
# =============================================================

from pyspark.sql.functions import (
    col, window, count, sum, avg,
    when, round, countDistinct
)

# ------------------------------------------------------------------
# 1. Read stream from Silver Delta table
# ------------------------------------------------------------------
silver_stream = (
    spark.readStream
    .format("delta")
    .table("shopstream_db.silver_order_events")
)

# ------------------------------------------------------------------
# 2. Compute real-time KPIs using 5-minute tumbling windows
#
#    withWatermark — waits 10 minutes for late data before
#    finalising each window. Prevents unbounded state growth
#    in long-running streaming jobs.
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
        countDistinct("customer_id").alias("unique_customers"),
        sum("order_value").alias("total_revenue"),
        avg("order_value").alias("avg_order_value"),
        sum(when(col("order_status") == "DELIVERED", 1).otherwise(0))
            .alias("delivered_count"),
        sum(when(col("order_status") == "CANCELLED", 1).otherwise(0))
            .alias("cancelled_count"),
        sum(when(col("is_returned") == True, 1).otherwise(0))
            .alias("returned_count"),
        sum("quantity").alias("total_units_sold")
    )
    .withColumn(
        "revenue_rounded",
        round(col("total_revenue"), 2)
    )
    .withColumn(
        "avg_order_value_rounded",
        round(col("avg_order_value"), 2)
    )
    .withColumn(
        "cancellation_rate_pct",
        round((col("cancelled_count") / col("total_orders")) * 100, 2)
    )
    .withColumn(
        "return_rate_pct",
        round((col("returned_count") / col("total_orders")) * 100, 2)
    )
    .withColumn(
        "delivery_rate_pct",
        round((col("delivered_count") / col("total_orders")) * 100, 2)
    )
)

# ------------------------------------------------------------------
# 3. Write Gold stream — update output mode
#    Only changed rows written per trigger — correct for aggregations
# ------------------------------------------------------------------
(
    gold_df.writeStream
    .format("delta")
    .outputMode("update")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", "/mnt/checkpoints/gold_orders")
    .toTable("shopstream_db.gold_order_kpis")
)

# ------------------------------------------------------------------
# Verify after ~3 minutes:
# SELECT * FROM shopstream_db.gold_order_kpis ORDER BY window DESC LIMIT 5;
#
# Sample Gold output:
# +---------------------------+--------+-------------+--------------+-----------------+-------------+----------------+-------------------+------------------+--------------------+
# | window                    | region | category    | total_orders | unique_customers | total_revenue| avg_order_value| cancellation_rate | return_rate_pct  | delivery_rate_pct  |
# +---------------------------+--------+-------------+--------------+-----------------+-------------+----------------+-------------------+------------------+--------------------+
# | 2025-07-03 10:40–10:45   | WEST   | ELECTRONICS | 423          | 389             | 84,231.77   | 199.13         | 3.31              | 4.49             | 78.25              |
# | 2025-07-03 10:40–10:45   | NORTH  | FASHION     | 891          | 812             | 31,540.09   | 35.40          | 5.05              | 6.17             | 72.50              |
# | 2025-07-03 10:40–10:45   | SOUTH  | GROCERY     | 1204         | 1087            | 18,922.40   | 15.72          | 2.16              | 1.91             | 89.12              |
# | 2025-07-03 10:40–10:45   | EAST   | ELECTRONICS | 318          | 298             | 63,182.82   | 198.69         | 4.40              | 5.03             | 75.79              |
# +---------------------------+--------+-------------+--------------+-----------------+-------------+----------------+-------------------+------------------+--------------------+
#
# KPIs powering the live dashboard:
#   - Real-time revenue by region and category
#   - Cancellation rate alerting (threshold: >10% triggers alert)
#   - Return rate trends per category
#   - Unique customer activity in 5-min windows
#   - End-to-end latency: <2 minutes event → Gold table
# ------------------------------------------------------------------
