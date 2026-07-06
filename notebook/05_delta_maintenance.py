# =============================================================
# 05 — DELTA LAKE MAINTENANCE
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Performs routine Delta Lake maintenance to prevent small file
# explosion from continuous streaming writes. Runs OPTIMIZE
# with ZORDER on frequently queried columns, VACUUM to remove
# stale versions, and enables auto-optimize on all tables.
# Schedule this notebook to run once daily (e.g. 2:00 AM).
#
# Operations : OPTIMIZE · ZORDER · VACUUM · auto-optimize
# Schedule   : Daily — Databricks Workflow (cron: 0 2 * * *)
# Tech       : Delta Lake · Databricks SQL · Spark SQL
# Author     : Akhil Bakki
# GitHub     : github.com/akhilbakki/shopstream-realtime-pipeline
# =============================================================

# ------------------------------------------------------------------
# 1. OPTIMIZE + ZORDER Silver table
#    ZORDER on region + event_ts speeds up Gold aggregation queries
#    that filter/group on these columns
# ------------------------------------------------------------------
print("Running OPTIMIZE on silver_order_events...")

spark.sql("""
    OPTIMIZE shopstream_db.silver_order_events
    ZORDER BY (region, category, event_ts)
""")

print("OPTIMIZE complete on silver_order_events")

# ------------------------------------------------------------------
# 2. OPTIMIZE Gold KPI table
# ------------------------------------------------------------------
print("Running OPTIMIZE on gold_order_kpis...")

spark.sql("""
    OPTIMIZE shopstream_db.gold_order_kpis
    ZORDER BY (region, category)
""")

print("OPTIMIZE complete on gold_order_kpis")

# ------------------------------------------------------------------
# 3. VACUUM — remove Delta versions older than 7 days
#    Reduces ADLS Gen2 storage costs significantly
#    Keep 168 hours (7 days) for time travel and debugging
# ------------------------------------------------------------------
print("Running VACUUM on all tables...")

spark.sql("VACUUM shopstream_db.bronze_order_events  RETAIN 168 HOURS")
spark.sql("VACUUM shopstream_db.silver_order_events  RETAIN 168 HOURS")
spark.sql("VACUUM shopstream_db.gold_order_kpis      RETAIN 168 HOURS")
spark.sql("VACUUM shopstream_db.dead_letter_orders   RETAIN 168 HOURS")

print("VACUUM complete on all tables")

# ------------------------------------------------------------------
# 4. Enable auto-optimize on Bronze + Silver tables
#    Prevents small file accumulation from high-frequency streaming writes
# ------------------------------------------------------------------
for table in ["bronze_order_events", "silver_order_events"]:
    spark.sql(f"""
        ALTER TABLE shopstream_db.{table}
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)
    print(f"Auto-optimize enabled on {table}")

# ------------------------------------------------------------------
# 5. Print table health summary
# ------------------------------------------------------------------
print("\n" + "="*60)
print("  DELTA MAINTENANCE COMPLETE")
print("="*60)

for table in ["bronze_order_events", "silver_order_events",
              "gold_order_kpis", "dead_letter_orders"]:
    row_count = spark.table(f"shopstream_db.{table}").count()
    print(f"  {table:<30} {row_count:>10,} rows")

print("="*60)

# ------------------------------------------------------------------
# 6. Test time travel — verify Delta history is intact
# ------------------------------------------------------------------
print("\nTesting Delta time travel on Silver table...")

spark.sql("""
    SELECT count(*) AS rows_1hr_ago
    FROM shopstream_db.silver_order_events
    TIMESTAMP AS OF dateadd(HOUR, -1, current_timestamp())
""").show()

print("Time travel verified — Delta transaction log intact")
# ------------------------------------------------------------------
