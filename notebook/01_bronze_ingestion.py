# =============================================================
# 01 — BRONZE INGESTION
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Ingests live order and delivery events from Azure Event Hubs
# using Databricks Structured Streaming. Writes raw events to
# a Bronze Delta Lake table with exactly-once checkpoint
# guarantees. No transformations applied — raw data preserved
# for full replay capability.
#
# Layer    : Bronze (raw — never modified)
# Trigger  : 60-second micro-batch
# Output   : shopstream_db.bronze_order_events (Delta)
# Tech     : PySpark · Azure Event Hubs · Delta Lake · Databricks
# Author   : Akhil Bakki
# GitHub   : github.com/akhilbakki/shopstream-realtime-pipeline
# =============================================================

from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType
)

# ------------------------------------------------------------------
# 1. Schema — matches order event producer payload
# ------------------------------------------------------------------
ORDER_SCHEMA = StructType([
    StructField("order_id",        StringType(),   True),
    StructField("customer_id",     StringType(),   True),
    StructField("product_id",      StringType(),   True),
    StructField("category",        StringType(),   True),
    StructField("order_status",    StringType(),   True),
    StructField("payment_method",  StringType(),   True),
    StructField("region",          StringType(),   True),
    StructField("order_value",     DoubleType(),   True),
    StructField("quantity",        IntegerType(),  True),
    StructField("is_returned",     BooleanType(),  True),
    StructField("event_ts",        StringType(),   True),
])

# ------------------------------------------------------------------
# 2. Event Hubs configuration
#    Connection string stored in Databricks Secret Scope.
#    Never hardcode secrets — always use dbutils.secrets.get()
# ------------------------------------------------------------------
ehConf = {
    "eventhubs.connectionString": dbutils.secrets.get(
        scope="<your-scope-name>",
        key="<your-secret-key>"
    ),
    "eventhubs.consumerGroup":    "$Default",
    "eventhubs.startingPosition": (
        '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'
    )
}

# ------------------------------------------------------------------
# 3. Read stream from Azure Event Hubs
# ------------------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

# ------------------------------------------------------------------
# 4. Parse JSON payload + add ingestion metadata
# ------------------------------------------------------------------
bronze_df = (
    raw_stream
    .select(
        from_json(col("body").cast("string"), ORDER_SCHEMA).alias("data")
    )
    .select("data.*")
    .withColumn("ingested_at", current_timestamp())
)

# ------------------------------------------------------------------
# 5. Write to Bronze Delta table
#    Checkpoint enables exactly-once processing on job restart
# ------------------------------------------------------------------
(
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", "/mnt/checkpoints/bronze_orders")
    .toTable("shopstream_db.bronze_order_events")
)

# ------------------------------------------------------------------
# Verify after ~2 minutes:
# SELECT count(*) FROM shopstream_db.bronze_order_events;
#
# Sample rows:
# +-----------+-------------+------------+-------------+--------------+----------------+----------+-------------+----------+------------+---------------------------+---------------------------+
# | order_id  | customer_id | product_id | category    | order_status | payment_method | region   | order_value | quantity | is_returned| event_ts                  | ingested_at               |
# +-----------+-------------+------------+-------------+--------------+----------------+----------+-------------+----------+------------+---------------------------+---------------------------+
# | ORD100234 | CUST_7821   | PROD_4421  | Electronics | PLACED       | CREDIT_CARD    | WEST     | 299.99      | 1        | false      | 2025-07-03T10:45:23+00:00 | 2025-07-03 10:46:01       |
# | ORD100235 | CUST_3312   | PROD_2201  | Fashion     | SHIPPED      | UPI            | SOUTH    | 49.99       | 2        | false      | 2025-07-03T10:45:24+00:00 | 2025-07-03 10:46:01       |
# | ORD100236 | CUST_9901   | PROD_8801  | Grocery     | DELIVERED    | COD            | NORTH    | 18.50       | 5        | true       | 2025-07-03T10:45:25+00:00 | 2025-07-03 10:46:01       |
# +-----------+-------------+------------+-------------+--------------+----------------+----------+-------------+----------+------------+---------------------------+---------------------------+
# ------------------------------------------------------------------
