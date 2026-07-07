# =============================================================
# 01 — BRONZE INGESTION
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Reads live order events from Azure Event Hubs using Databricks
# Structured Streaming. Writes raw events to Bronze Delta Lake
# table with exactly-once checkpoint guarantees via ADLS Gen2.
#
# Layer    : Bronze (raw — never modified)
# Trigger  : 30-second micro-batch (faster ingestion)
# Output   : akhilstream_db.bronze_order_events (Delta on ADLS)
# Tech     : PySpark · Azure Event Hubs · Delta Lake · ADLS Gen2
# Author   : Akhil Bakki
# =============================================================

from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType
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
BRONZE_CP     = f"{BASE_PATH}/checkpoints/bronze_orders"

spark.sql("USE CATALOG hive_metastore")
spark.sql("CREATE DATABASE IF NOT EXISTS akhilstream_db")
spark.sql("USE DATABASE akhilstream_db")

print("✅ ADLS Gen2 configured :", BASE_PATH)
print("✅ Database ready       :", spark.catalog.currentDatabase())

# ------------------------------------------------------------------
# STEP 2 — Spark performance tuning for faster ingestion
# ------------------------------------------------------------------
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

print("✅ Spark performance tuning applied")

# ------------------------------------------------------------------
# STEP 3 — Clear old checkpoint for fresh start
# ------------------------------------------------------------------
try:
    dbutils.fs.rm(BRONZE_CP, recurse=True)
    print("✅ Old checkpoint cleared")
except:
    print("ℹ️  No existing checkpoint — fresh start")

dbutils.fs.mkdirs(BRONZE_CP)
print("✅ Checkpoint directory created:", BRONZE_CP)

# ------------------------------------------------------------------
# STEP 4 — Schema matching order event producer payload
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
# STEP 5 — Verify Event Hubs connection string
# ------------------------------------------------------------------
conn_str = dbutils.secrets.get(scope="shopstream-scope", key="eh-conn-str")

if "EntityPath" not in conn_str:
    raise ValueError(
        "❌ EntityPath missing!\n"
        "Add ;EntityPath=order-events at the end of your secret."
    )
print("✅ EntityPath found in connection string")

# ------------------------------------------------------------------
# STEP 6 — Event Hubs configuration
# ------------------------------------------------------------------
ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_str),
    "eventhubs.consumerGroup":    "$Default",
    "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    "eventhubs.maxEventsPerTrigger": "50000",
    "eventhubs.prefetchCount":       "999",
    "eventhubs.receiverTimeout":     "PT1M",
    "eventhubs.operationTimeout":    "PT1M"
}

print("✅ Event Hubs config ready — maxEventsPerTrigger: 50000")

# ------------------------------------------------------------------
# STEP 7 — Read stream from Azure Event Hubs
# ------------------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

print("✅ Stream reader created")

# ------------------------------------------------------------------
# STEP 8 — Parse JSON payload + add ingestion metadata
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
# STEP 9 — Write to Bronze Delta table on ADLS Gen2
#    30s trigger for faster ingestion
#    maxFilesPerTrigger for better small file handling
# ------------------------------------------------------------------
print("\n🚀 Starting Bronze streaming job...")
print(f"   Checkpoint  : {BRONZE_CP}")
print(f"   Table       : akhilstream_db.bronze_order_events")
print(f"   Trigger     : 30 seconds")
print(f"   Output mode : append")
print(f"   Storage     : ADLS Gen2 → {STORAGE_ACCOUNT}")

BRONZE_DELTA_PATH = f"{BASE_PATH}/delta/bronze"

query = (
    bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", BRONZE_CP)
    .option("mergeSchema", "true")
    .start(BRONZE_DELTA_PATH)       # ← write directly to ADLS path
)

# Register as table so SQL queries still work
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS hive_metastore.akhilstream_db.bronze_order_events
    USING DELTA
    LOCATION '{BRONZE_DELTA_PATH}'
""")

# ------------------------------------------------------------------
# STEP 10 — Monitor stream status
# ------------------------------------------------------------------
import time
time.sleep(15)

print("\n📊 Stream status:")
print("   Is active:", query.isActive)
print("   Message  :", query.status["message"])

print("\n⏳ Verify after ~2 minutes:")
print("   SELECT count(*) FROM akhilstream_db.bronze_order_events")

# ------------------------------------------------------------------
# Sample output after ~2 minutes:
# +-----------+-------------+------------+-------------+--------------+
# | order_id  | customer_id | category   | order_status| order_value  |
# +-----------+-------------+------------+-------------+--------------+
# | ORD234891 | CUST_7821   | Electronics| PLACED      | 299.99       |
# | ORD234892 | CUST_3312   | Fashion    | SHIPPED     | 49.99        |
# | ORD234893 | CUST_9901   | Grocery    | DELIVERED   | 18.50        |
# +-----------+-------------+------------+-------------+--------------+
# ------------------------------------------------------------------
