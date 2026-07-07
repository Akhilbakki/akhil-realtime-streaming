# =============================================================
# 05 — DELTA LAKE MAINTENANCE
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Daily maintenance job — OPTIMIZE, ZORDER, VACUUM on all
# Delta tables. Prevents small file explosion from continuous
# streaming writes. Schedule daily at 2:00 AM.
#
# Operations : OPTIMIZE · ZORDER · VACUUM · auto-optimize
# Schedule   : Daily — Databricks Workflow (2:00 AM)
# Author     : Akhil Bakki
# =============================================================

from datetime import datetime

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

spark.sql("USE CATALOG hive_metastore")
spark.sql("USE DATABASE akhilstream_db")
print("✅ Database ready:", spark.catalog.currentDatabase())

print(f"\n{'='*65}")
print(f"  DELTA MAINTENANCE STARTED")
print(f"  Time   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Storage: {STORAGE_ACCOUNT}")
print(f"{'='*65}\n")

# ------------------------------------------------------------------
# STEP 2 — All tables to maintain
# ------------------------------------------------------------------
tables = {
    "bronze_order_events":      ["ingested_at"],
    "silver_order_events":      ["region", "category", "event_ts"],
    "gold_order_kpis":          ["region", "category"],
    "gold_revenue_by_payment":  ["payment_method", "region"],
    "gold_product_kpis":        ["product_id", "category"],
    "dead_letter_orders":       ["failed_at"],
}

# ------------------------------------------------------------------
# STEP 3 — Row counts before maintenance
# ------------------------------------------------------------------
print("📊 Row counts BEFORE maintenance:")
before_counts = {}
for table in tables:
    try:
        cnt = spark.table(f"hive_metastore.akhilstream_db.{table}").count()
        before_counts[table] = cnt
        print(f"   {table:<35} {cnt:>10,} rows")
    except:
        before_counts[table] = 0
        print(f"   {table:<35} {'NOT FOUND':>10}")

# ------------------------------------------------------------------
# STEP 4 — OPTIMIZE + ZORDER all tables
# ------------------------------------------------------------------
print("\n🔧 Running OPTIMIZE + ZORDER...")

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

for table, zorder_cols in tables.items():
    try:
        zorder_str = ", ".join(zorder_cols)
        spark.sql(f"""
            OPTIMIZE hive_metastore.akhilstream_db.{table}
            ZORDER BY ({zorder_str})
        """)
        print(f"  ✅ OPTIMIZE complete: {table}  ZORDER BY ({zorder_str})")
    except Exception as e:
        print(f"  ⚠️  OPTIMIZE skipped: {table} — {str(e)[:60]}")

# ------------------------------------------------------------------
# STEP 5 — VACUUM all tables (retain 7 days)
# ------------------------------------------------------------------
print("\n🗑️  Running VACUUM (retain 168 hours)...")

for table in tables:
    try:
        spark.sql(f"VACUUM hive_metastore.akhilstream_db.{table} RETAIN 168 HOURS")
        print(f"  ✅ VACUUM complete: {table}")
    except Exception as e:
        print(f"  ⚠️  VACUUM skipped: {table} — {str(e)[:60]}")

# ------------------------------------------------------------------
# STEP 6 — Enable auto-optimize on streaming tables
# ------------------------------------------------------------------
print("\n⚙️  Enabling auto-optimize on streaming tables...")

streaming_tables = ["bronze_order_events", "silver_order_events"]
for table in streaming_tables:
    try:
        spark.sql(f"""
            ALTER TABLE hive_metastore.akhilstream_db.{table}
            SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact'   = 'true'
            )
        """)
        print(f"  ✅ Auto-optimize enabled: {table}")
    except Exception as e:
        print(f"  ⚠️  Skipped: {table} — {str(e)[:60]}")

# ------------------------------------------------------------------
# STEP 7 — Row counts after maintenance
# ------------------------------------------------------------------
print("\n📊 Row counts AFTER maintenance:")
for table in tables:
    try:
        cnt = spark.table(f"hive_metastore.akhilstream_db.{table}").count()
        diff = cnt - before_counts.get(table, 0)
        diff_str = f"(+{diff:,})" if diff > 0 else ""
        print(f"   {table:<35} {cnt:>10,} rows  {diff_str}")
    except:
        print(f"   {table:<35} {'NOT FOUND':>10}")

# ------------------------------------------------------------------
# STEP 8 — Delta history for Silver table
# ------------------------------------------------------------------
print("\n📜 Delta history — silver_order_events (last 5):")
try:
    spark.sql("""
        DESCRIBE HISTORY hive_metastore.akhilstream_db.silver_order_events
        LIMIT 5
    """).select("version", "timestamp", "operation").show(truncate=False)
except Exception as e:
    print(f"  ⚠️  History unavailable: {str(e)[:60]}")

# ------------------------------------------------------------------
# STEP 9 — Test time travel
# ------------------------------------------------------------------
print("\n🕐 Testing Delta time travel on Silver table...")
try:
    result = spark.sql("""
        SELECT count(*) AS rows_at_version_0
        FROM hive_metastore.akhilstream_db.silver_order_events
        VERSION AS OF 0
    """)
    result.show()
    print("✅ Time travel working — Delta transaction log intact in ADLS Gen2")
except Exception as e:
    print(f"ℹ️  Time travel: {str(e)[:80]}")

# ------------------------------------------------------------------
# STEP 10 — ADLS Gen2 storage summary
# ------------------------------------------------------------------
print("\n📁 ADLS Gen2 storage summary:")
try:
    for folder in ["checkpoints", "delta"]:
        files = dbutils.fs.ls(f"{BASE_PATH}/{folder}/")
        print(f"  {folder}/  — {len(files)} items")
        for f in files:
            print(f"    📁 {f.name}")
except Exception as e:
    print(f"  ⚠️  {str(e)[:60]}")

print(f"\n{'='*65}")
print(f"  DELTA MAINTENANCE COMPLETE ✅")
print(f"  Time      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Tables    : {len(tables)} maintained")
print(f"  Operations: OPTIMIZE · ZORDER · VACUUM · auto-optimize")
print(f"  Storage   : {STORAGE_ACCOUNT} (ADLS Gen2)")
print(f"{'='*65}")
