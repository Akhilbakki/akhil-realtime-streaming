# =============================================================
# 01 — BRONZE INGESTION
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Reads live order events from Azure Event Hubs using Databricks
# Structured Streaming. Writes raw events to Bronze Delta Lake
# table with exactly-once checkpoint guarantees.
#
# Layer    : Bronze (raw — never modified)
# Trigger  : 60-second micro-batch
# Output   : akhilstream_db.bronze_order_events (Delta)
# Tech     : PySpark · Azure Event Hubs · Delta Lake · Databricks
# Author   : Akhil Bakki
# =============================================================

from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType
)

# ------------------------------------------------------------------
# STEP 1 — Set up database
# ------------------------------------------------------------------
spark.sql("USE CATALOG hive_metastore")
spark.sql("CREATE DATABASE IF NOT EXISTS akhilstream_db")
spark.sql("USE DATABASE akhilstream_db")
print("✅ Database ready:", spark.catalog.currentDatabase())

# ------------------------------------------------------------------
# STEP 2 — Clear old checkpoint (only needed on first run or error)
# ------------------------------------------------------------------
checkpoint_path = "dbfs:/shopstream/checkpoints/bronze_orders"

try:
    dbutils.fs.rm(checkpoint_path, recurse=True)
    print("✅ Old checkpoint cleared")
except:
    print("ℹ️  No existing checkpoint found — fresh start")

# Create checkpoint directory
dbutils.fs.mkdirs(checkpoint_path)
print("✅ Checkpoint directory created:", checkpoint_path)

# ------------------------------------------------------------------
# STEP 3 — Verify Event Hubs connection string has EntityPath
# ------------------------------------------------------------------
conn_str = dbutils.secrets.get(scope="akhilstream-scope", key="eh-conn-str") + ';EntityPath=order-events' if 'EntityPath' not in dbutils.secrets.get(scope="akhilstream-scope", key="eh-conn-str") else dbutils.secrets.get(scope="akhilstream-scope", key="eh-conn-str")

if "EntityPath" not in conn_str:
    raise ValueError(
        "❌ EntityPath missing from connection string!\n"
        "Add ;EntityPath=order-events at the end of your secret.\n"
        "Example: Endpoint=sb://...;SharedAccessKey=...;EntityPath=order-events"
    )

print("✅ EntityPath found in connection string — good to go!")

# ------------------------------------------------------------------
# STEP 4 — Schema matching the order event producer payload
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

print("✅ Schema defined")

# ------------------------------------------------------------------
# STEP 5 — Event Hubs configuration
#    Connection string retrieved from Databricks Secret Scope
#    EntityPath=order-events must be included in the secret
# ------------------------------------------------------------------
ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_str),
    "eventhubs.consumerGroup":    "$Default",
    "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'
}

print("✅ Event Hubs config ready")

# ------------------------------------------------------------------
# STEP 6 — Read stream from Azure Event Hubs
# ------------------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

print("✅ Stream reader created")

# ------------------------------------------------------------------
# STEP 7 — Parse JSON payload + add ingestion timestamp
# ------------------------------------------------------------------
bronze_df = (
    raw_stream
    .select(
        from_json(col("body").cast("string"), ORDER_SCHEMA).alias("data")
    )
    .select("data.*")
    .withColumn("ingested_at", current_timestamp())
)

print("✅ Transformation defined")

# ------------------------------------------------------------------
# STEP 8 — Write to Bronze Delta table
#    append mode — raw data never modified
#    60s micro-batch trigger
#    checkpoint — exactly-once guarantee on restart
# ------------------------------------------------------------------
print("🚀 Starting Bronze streaming job...")
print("   Checkpoint : dbfs:/shopstream/checkpoints/bronze_orders")
print("   Table      : akhilstream_db.bronze_order_events")
print("   Trigger    : 60 seconds")
print("   Output mode: append")
print("")
print("⏳ Wait ~2 minutes then verify:")
print("   SELECT count(*) FROM akhilstream_db.bronze_order_events")

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", checkpoint_path)
    .toTable("hive_metastore.akhilstream_db.bronze_order_events")
)

# ------------------------------------------------------------------
# STEP 9 — Monitor stream status
# ------------------------------------------------------------------
import time
time.sleep(10)

status = query.status
print("\n📊 Stream status:")
print("   Message       :", status["message"])
print("   Is active     :", query.isActive)
print("   Recent progress:", query.lastProgress)

# ------------------------------------------------------------------
# Sample output after ~2 minutes:
# SELECT * FROM akhilstream_db.bronze_order_events LIMIT 5;
#
# +-----------+-------------+------------+-------------+--------------+----------------+--------+-------------+----------+------------+---------------------------+---------------------------+
# | order_id  | customer_id | product_id | category    | order_status | payment_method | region | order_value | quantity | is_returned| event_ts                  | ingested_at               |
# +-----------+-------------+------------+-------------+--------------+----------------+--------+-------------+----------+------------+---------------------------+---------------------------+
# | ORD234891 | CUST_7821   | PROD_4421  | Electronics | PLACED       | CREDIT_CARD    | WEST   | 299.99      | 1        | false      | 2025-07-03T10:45:23+00:00 | 2025-07-03 10:46:01       |
# | ORD234892 | CUST_3312   | PROD_2201  | Fashion     | SHIPPED      | UPI            | SOUTH  | 49.99       | 2        | false      | 2025-07-03T10:45:24+00:00 | 2025-07-03 10:46:01       |
# | ORD234893 | CUST_9901   | PROD_8801  | Grocery     | DELIVERED    | COD            | NORTH  | 18.50       | 5        | true       | 2025-07-03T10:45:25+00:00 | 2025-07-03 10:46:01       |
# +-----------+-------------+------------+-------------+--------------+----------------+--------+-------------+----------+------------+---------------------------+---------------------------+
# ------------------------------------------------------------------
