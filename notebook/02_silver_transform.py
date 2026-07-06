# =============================================================
# 02 — SILVER TRANSFORMATION
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Reads from Bronze Delta table and applies data quality rules:
# null validation, type casting, order value range checks, and
# deduplication on order_id + event_ts. Invalid records are
# routed to a dead-letter table for investigation — never
# silently dropped.
#
# Layer    : Silver (cleaned, validated, deduplicated)
# Trigger  : 60-second micro-batch
# Output   : shopstream_db.silver_order_events (Delta)
#            shopstream_db.dead_letter_orders  (Delta)
# Tech     : PySpark · Delta Lake · Databricks Structured Streaming
# Author   : Akhil Bakki
# GitHub   : github.com/akhilbakki/shopstream-realtime-pipeline
# =============================================================

from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, upper, trim
)

# ------------------------------------------------------------------
# 1. Read stream from Bronze Delta table
# ------------------------------------------------------------------
bronze_stream = (
    spark.readStream
    .format("delta")
    .table("shopstream_db.bronze_order_events")
)

# ------------------------------------------------------------------
# 2. Define validation rules
#    Any record failing → dead-letter table
# ------------------------------------------------------------------
valid_condition = (
    col("order_id").isNotNull()        &
    col("customer_id").isNotNull()     &
    col("order_status").isNotNull()    &
    col("region").isNotNull()          &
    col("order_value").isNotNull()     &
    col("order_value") > 0             &
    col("quantity").isNotNull()        &
    col("quantity") > 0                &
    col("event_ts").isNotNull()
)

# ------------------------------------------------------------------
# 3. Silver — clean, cast, standardise
# ------------------------------------------------------------------
silver_df = (
    bronze_stream
    .filter(valid_condition)
    .withColumn("event_ts",       to_timestamp(col("event_ts")))
    .withColumn("processed_at",   current_timestamp())
    .withColumn("region",         upper(trim(col("region"))))        # standardise region casing
    .withColumn("category",       upper(trim(col("category"))))      # standardise category casing
    .withColumn("order_status",   upper(trim(col("order_status"))))  # standardise status casing
    .dropDuplicates(["order_id", "event_ts"])
)

# ------------------------------------------------------------------
# 4. Dead letter — invalid records for investigation
# ------------------------------------------------------------------
dead_letter_df = (
    bronze_stream
    .filter(~valid_condition)
    .withColumn("failed_at", current_timestamp())
)

# ------------------------------------------------------------------
# 5. Write Silver stream
# ------------------------------------------------------------------
(
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", "/mnt/checkpoints/silver_orders")
    .toTable("shopstream_db.silver_order_events")
)

# ------------------------------------------------------------------
# 6. Write Dead Letter stream
# ------------------------------------------------------------------
(
    dead_letter_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", "/mnt/checkpoints/dead_letter_orders")
    .toTable("shopstream_db.dead_letter_orders")
)

# ------------------------------------------------------------------
# Verify after ~2 minutes:
# SELECT count(*) FROM shopstream_db.silver_order_events;
# SELECT count(*) FROM shopstream_db.dead_letter_orders;
#
# Silver sample (clean, typed, standardised):
# +-----------+-------------+----------+-------------+--------------+-------------+----------+-------------------+---------------------------+
# | order_id  | customer_id | category | order_status| payment_method| order_value | region  | event_ts          | processed_at              |
# +-----------+-------------+----------+-------------+--------------+-------------+----------+-------------------+---------------------------+
# | ORD100234 | CUST_7821   | ELECTRONICS| PLACED    | CREDIT_CARD  | 299.99      | WEST     | 2025-07-03 10:45  | 2025-07-03 10:46:05       |
# | ORD100235 | CUST_3312   | FASHION  | SHIPPED     | UPI          | 49.99       | SOUTH    | 2025-07-03 10:45  | 2025-07-03 10:46:05       |
# +-----------+-------------+----------+-------------+-------------+-------------+----------+-------------------+---------------------------+
# ------------------------------------------------------------------
