# =============================================================
# 02 — SILVER TRANSFORMATION
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Reads from Bronze Delta table and applies data quality rules:
# null validation, type casting, value range checks, and
# deduplication. Invalid records routed to dead-letter table.
#
# Layer    : Silver (cleaned, validated, deduplicated)
# Trigger  : 60-second micro-batch
# Output   : akhilstream_db.silver_order_events (Delta)
#            akhilstream_db.dead_letter_orders  (Delta)
# Tech     : PySpark · Delta Lake · Databricks Structured Streaming
# Author   : Akhil Bakki
# =============================================================

from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, upper, trim
)

# ------------------------------------------------------------------
# STEP 1 — Set up database
# ------------------------------------------------------------------
spark.sql("USE CATALOG hive_metastore")
spark.sql("CREATE DATABASE IF NOT EXISTS akhilstream_db")
spark.sql("USE DATABASE akhilstream_db")
print("✅ Database ready:", spark.catalog.currentDatabase())

# ------------------------------------------------------------------
# STEP 2 — Verify Bronze table has data before starting
# ------------------------------------------------------------------
bronze_count = spark.table("hive_metastore.akhilstream_db.bronze_order_events").count()
print(f"✅ Bronze table has {bronze_count:,} rows — ready to process")

if bronze_count == 0:
    raise ValueError(
        "❌ Bronze table is empty!\n"
        "Make sure notebook 01_bronze_ingestion is running first\n"
        "and wait at least 2 minutes before running this notebook."
    )

# ------------------------------------------------------------------
# STEP 3 — Clear old checkpoints (first run or after error)
# ------------------------------------------------------------------
silver_checkpoint     = "dbfs:/shopstream/checkpoints/silver_orders"
deadletter_checkpoint = "dbfs:/shopstream/checkpoints/dead_letter_orders"

for path in [silver_checkpoint, deadletter_checkpoint]:
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"✅ Cleared checkpoint: {path}")
    except:
        print(f"ℹ️  No existing checkpoint: {path}")

dbutils.fs.mkdirs(silver_checkpoint)
dbutils.fs.mkdirs(deadletter_checkpoint)
print("✅ Checkpoint directories created")

# ------------------------------------------------------------------
# STEP 4 — Read stream from Bronze Delta table
#    Delta readStream picks up new micro-batches as Bronze
#    ingestion writes them every 60 seconds
# ------------------------------------------------------------------
bronze_stream = (
    spark.readStream
    .format("delta")
    .table("hive_metastore.akhilstream_db.bronze_order_events")
)

print("✅ Bronze stream reader created")

# ------------------------------------------------------------------
# STEP 5 — Define validation rules
#    Records failing ANY condition → dead-letter table
#    Records passing ALL conditions → Silver table
# ------------------------------------------------------------------
valid_condition = (
    col("order_id").isNotNull()        &
    col("customer_id").isNotNull()     &
    col("order_status").isNotNull()    &
    col("region").isNotNull()          &
    col("order_value").isNotNull()     &
    (col("order_value") > 0)             &
    col("quantity").isNotNull()        &
    (col("quantity") > 0)                &
    col("event_ts").isNotNull()
)

print("✅ Validation rules defined")

# ------------------------------------------------------------------
# STEP 6 — Silver — clean, cast, standardise
# ------------------------------------------------------------------
silver_df = (
    bronze_stream
    .filter(valid_condition)
    .withColumn("event_ts",     to_timestamp(col("event_ts")))
    .withColumn("processed_at", current_timestamp())
    .withColumn("region",       upper(trim(col("region"))))
    .withColumn("category",     upper(trim(col("category"))))
    .withColumn("order_status", upper(trim(col("order_status"))))
    .dropDuplicates(["order_id", "event_ts"])
)

# ------------------------------------------------------------------
# STEP 7 — Dead letter — invalid records for investigation
# ------------------------------------------------------------------
dead_letter_df = (
    bronze_stream
    .filter(~valid_condition)
    .withColumn("failed_at", current_timestamp())
)

print("✅ Silver and Dead Letter transformations defined")

# ------------------------------------------------------------------
# STEP 8 — Write Silver stream
# ------------------------------------------------------------------
print("\n🚀 Starting Silver streaming job...")
print("   Checkpoint : dbfs:/shopstream/checkpoints/silver_orders")
print("   Table      : akhilstream_db.silver_order_events")
print("   Trigger    : 60 seconds")
print("   Output mode: append")

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", silver_checkpoint)
    .toTable("hive_metastore.akhilstream_db.silver_order_events")
)

# ------------------------------------------------------------------
# STEP 9 — Write Dead Letter stream
# ------------------------------------------------------------------
print("\n🚀 Starting Dead Letter streaming job...")
print("   Checkpoint : dbfs:/shopstream/checkpoints/dead_letter_orders")
print("   Table      : akhilstream_db.dead_letter_orders")

dead_letter_query = (
    dead_letter_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", deadletter_checkpoint)
    .toTable("hive_metastore.akhilstream_db.dead_letter_orders")
)

# ------------------------------------------------------------------
# STEP 10 — Monitor stream status
# ------------------------------------------------------------------
import time
time.sleep(10)

print("\n📊 Silver stream status:")
print("   Is active:", silver_query.isActive)
print("   Message  :", silver_query.status["message"])

print("\n📊 Dead Letter stream status:")
print("   Is active:", dead_letter_query.isActive)
print("   Message  :", dead_letter_query.status["message"])

print("\n⏳ Wait ~2 minutes then verify:")
print("   SELECT count(*) FROM akhilstream_db.silver_order_events")
print("   SELECT count(*) FROM akhilstream_db.dead_letter_orders")

# ------------------------------------------------------------------
# Sample output after ~2 minutes:
# SELECT * FROM akhilstream_db.silver_order_events LIMIT 5;
#
# +-----------+-------------+-------------+---------------+--------------+--------+-------------+---------------------------+---------------------------+
# | order_id  | customer_id | category    | order_status  | order_value  | region | quantity    | event_ts                  | processed_at              |
# +-----------+-------------+-------------+---------------+--------------+--------+-------------+---------------------------+---------------------------+
# | ORD234891 | CUST_7821   | ELECTRONICS | PLACED        | 299.99       | WEST   | 1           | 2025-07-03 10:45:23       | 2025-07-03 10:46:05       |
# | ORD234892 | CUST_3312   | FASHION     | SHIPPED       | 49.99        | SOUTH  | 2           | 2025-07-03 10:45:24       | 2025-07-03 10:46:05       |
# | ORD234893 | CUST_9901   | GROCERY     | DELIVERED     | 18.50        | NORTH  | 5           | 2025-07-03 10:45:25       | 2025-07-03 10:46:05       |
# +-----------+-------------+-------------+---------------+--------------+--------+-------------+---------------------------+---------------------------+
# ------------------------------------------------------------------
