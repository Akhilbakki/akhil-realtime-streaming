# =============================================================
# 02 — SILVER TRANSFORMATION
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Reads from Bronze Delta table, applies data quality rules:
# null validation, type casting, value range checks, and
# deduplication. Invalid records routed to dead-letter table.
#
# Layer    : Silver (cleaned, validated, deduplicated)
# Trigger  : 30-second micro-batch
# Output   : akhilstream_db.silver_order_events (Delta on ADLS)
#            akhilstream_db.dead_letter_orders  (Delta on ADLS)
# Tech     : PySpark · Delta Lake · Databricks Structured Streaming
# Author   : Akhil Bakki
# =============================================================

from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, upper, trim
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

BASE_PATH     = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_CP     = f"{BASE_PATH}/checkpoints/silver_orders"
DEADLETTER_CP = f"{BASE_PATH}/checkpoints/dead_letter_orders"

spark.sql("USE CATALOG hive_metastore")
spark.sql("CREATE DATABASE IF NOT EXISTS akhilstream_db")
spark.sql("USE DATABASE akhilstream_db")

print("✅ ADLS Gen2 configured :", BASE_PATH)
print("✅ Database ready       :", spark.catalog.currentDatabase())

# ------------------------------------------------------------------
# STEP 2 — Spark performance tuning
# ------------------------------------------------------------------
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print("✅ Spark performance tuning applied")

# ------------------------------------------------------------------
# STEP 3 — Verify Bronze table has data
# ------------------------------------------------------------------
bronze_count = spark.table("hive_metastore.akhilstream_db.bronze_order_events").count()
print(f"✅ Bronze table has {bronze_count:,} rows — ready to process")

if bronze_count == 0:
    raise ValueError(
        "❌ Bronze table is empty!\n"
        "Run notebook 01_bronze_ingestion first and wait 2 minutes."
    )

# ------------------------------------------------------------------
# STEP 4 — Clear old checkpoints
# ------------------------------------------------------------------
for path in [SILVER_CP, DEADLETTER_CP]:
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"✅ Cleared: {path}")
    except:
        print(f"ℹ️  No existing checkpoint: {path}")

dbutils.fs.mkdirs(SILVER_CP)
dbutils.fs.mkdirs(DEADLETTER_CP)
print("✅ Checkpoint directories created")

# ------------------------------------------------------------------
# STEP 5 — Read stream from Bronze Delta table
# ------------------------------------------------------------------
bronze_stream = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", "10")
    .table("hive_metastore.akhilstream_db.bronze_order_events")
)

print("✅ Bronze stream reader created")

# ------------------------------------------------------------------
# STEP 6 — Validation rules
# ------------------------------------------------------------------
valid_condition = (
    col("order_id").isNotNull()     &
    col("customer_id").isNotNull()  &
    col("order_status").isNotNull() &
    col("region").isNotNull()       &
    col("order_value").isNotNull()  &
    (col("order_value") > 0)        &
    col("quantity").isNotNull()     &
    (col("quantity") > 0)           &
    col("event_ts").isNotNull()
)

print("✅ Validation rules defined")

# ------------------------------------------------------------------
# STEP 7 — Silver — clean, cast, standardise
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
# STEP 8 — Dead letter — invalid records
# ------------------------------------------------------------------
dead_letter_df = (
    bronze_stream
    .filter(~valid_condition)
    .withColumn("failed_at", current_timestamp())
)

print("✅ Silver and Dead Letter transformations defined")

# ------------------------------------------------------------------
# STEP 9 — Write Silver stream
# ------------------------------------------------------------------
print("\n🚀 Starting Silver streaming job...")
SILVER_DELTA_PATH     = f"{BASE_PATH}/delta/silver"
DEADLETTER_DELTA_PATH = f"{BASE_PATH}/delta/dead_letter"

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", SILVER_CP)
    .option("mergeSchema", "true")
    .start(SILVER_DELTA_PATH)
)

dead_letter_query = (
    dead_letter_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", DEADLETTER_CP)
    .option("mergeSchema", "true")
    .start(DEADLETTER_DELTA_PATH)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS hive_metastore.akhilstream_db.silver_order_events
    USING DELTA LOCATION '{SILVER_DELTA_PATH}'
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS hive_metastore.akhilstream_db.dead_letter_orders
    USING DELTA LOCATION '{DEADLETTER_DELTA_PATH}'
""")

# ------------------------------------------------------------------
# STEP 11 — Monitor
# ------------------------------------------------------------------
import time
time.sleep(15)

print("\n📊 Silver stream  — Is active:", silver_query.isActive)
print("📊 Dead Letter    — Is active:", dead_letter_query.isActive)
print("\n⏳ Verify after ~2 minutes:")
print("   SELECT count(*) FROM akhilstream_db.silver_order_events")
print("   SELECT count(*) FROM akhilstream_db.dead_letter_orders")

# ------------------------------------------------------------------
# Sample output:
# silver_order_events — cleaned, typed, standardised
# dead_letter_orders  — null/invalid records for investigation
# ------------------------------------------------------------------
