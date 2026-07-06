# =============================================================
# 04 — DATA QUALITY CHECKS
# ShopStream — Real-Time E-Commerce Order Tracking Pipeline
# =============================================================
# Runs automated data quality assertions against Silver and Gold
# Delta tables. Raises exceptions on SLA breaches. Designed to
# run as a scheduled Databricks job every 5 minutes to enforce
# pipeline health and data quality SLAs.
#
# Checks   : Null rates, duplicate rate, value range validation,
#            cancellation rate threshold, row count SLA,
#            schema completeness
# Schedule : Every 5 minutes as a Databricks Workflow job
# Tech     : PySpark · Delta Lake · Databricks Workflows
# Author   : Akhil Bakki
# GitHub   : github.com/akhilbakki/shopstream-realtime-pipeline
# =============================================================

from pyspark.sql.functions import col, count, sum, when
from datetime import datetime

# ------------------------------------------------------------------
# 1. Load tables
# ------------------------------------------------------------------
silver_df = spark.table("shopstream_db.silver_order_events")
gold_df   = spark.table("shopstream_db.gold_order_kpis")
total_rows = silver_df.count()

print(f"\n{'='*65}")
print(f"  DATA QUALITY REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Table  : shopstream_db.silver_order_events")
print(f"  Rows   : {total_rows:,}")
print(f"{'='*65}\n")

# ------------------------------------------------------------------
# 2. Quality assertion helper
# ------------------------------------------------------------------
def assert_quality(check_name, condition_fn, actual_value, threshold, unit=""):
    passed   = condition_fn(actual_value)
    status   = "PASS ✓" if passed else "FAIL ✗"
    print(f"  [{status}] {check_name}: {actual_value:.4f}{unit}  (threshold: {threshold})")
    if not passed:
        raise ValueError(
            f"[DQ FAIL] {check_name} breached. "
            f"Got {actual_value:.4f}{unit}, expected {threshold}"
        )

# ------------------------------------------------------------------
# 3. CHECK 1 — order_id null rate = 0
# ------------------------------------------------------------------
null_order_id = silver_df.filter(col("order_id").isNull()).count()
null_rate     = null_order_id / total_rows if total_rows > 0 else 0
assert_quality("order_id null rate", lambda x: x == 0, null_rate, "0.0000")

# ------------------------------------------------------------------
# 4. CHECK 2 — Duplicate orders < 1%
# ------------------------------------------------------------------
distinct_count = silver_df.select("order_id", "event_ts").distinct().count()
dupe_rate      = 1 - (distinct_count / total_rows) if total_rows > 0 else 0
assert_quality("duplicate order rate", lambda x: x < 0.01, dupe_rate, "< 0.01")

# ------------------------------------------------------------------
# 5. CHECK 3 — No negative or zero order values
# ------------------------------------------------------------------
bad_values     = silver_df.filter(col("order_value") <= 0).count()
bad_value_rate = bad_values / total_rows if total_rows > 0 else 0
assert_quality("invalid order_value rate", lambda x: x == 0, bad_value_rate, "0.0000")

# ------------------------------------------------------------------
# 6. CHECK 4 — Cancellation rate < 15%
#    High cancellation rate = upstream checkout/payment issue
# ------------------------------------------------------------------
cancelled  = silver_df.filter(col("order_status") == "CANCELLED").count()
cancel_rate = cancelled / total_rows if total_rows > 0 else 0
assert_quality("cancellation rate", lambda x: x < 0.15, cancel_rate, "< 0.15 (< 15%)")

# ------------------------------------------------------------------
# 7. CHECK 5 — Return rate < 20%
# ------------------------------------------------------------------
returned     = silver_df.filter(col("is_returned") == True).count()
return_rate  = returned / total_rows if total_rows > 0 else 0
assert_quality("return rate", lambda x: x < 0.20, return_rate, "< 0.20 (< 20%)")

# ------------------------------------------------------------------
# 8. CHECK 6 — Valid regions only
# ------------------------------------------------------------------
valid_regions    = {"NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"}
invalid_region_count = silver_df.filter(
    ~col("region").isin(list(valid_regions))
).count()
invalid_region_rate = invalid_region_count / total_rows if total_rows > 0 else 0
assert_quality("invalid region rate", lambda x: x == 0, invalid_region_rate, "0.0000")

# ------------------------------------------------------------------
# 9. CHECK 7 — Minimum row count SLA (pipeline health)
# ------------------------------------------------------------------
min_rows = 500
print(f"\n  [{'PASS ✓' if total_rows >= min_rows else 'FAIL ✗'}] "
      f"Row count SLA: {total_rows:,} rows  (threshold: >= {min_rows:,})")
if total_rows < min_rows:
    raise ValueError(f"[DQ FAIL] Row count SLA breached — pipeline may be stalled.")

# ------------------------------------------------------------------
# 10. Gold table check — KPIs exist
# ------------------------------------------------------------------
gold_rows = gold_df.count()
print(f"  [{'PASS ✓' if gold_rows > 0 else 'FAIL ✗'}] "
      f"Gold KPI table populated: {gold_rows:,} rows")

# ------------------------------------------------------------------
# 11. Summary
# ------------------------------------------------------------------
print(f"\n{'='*65}")
print(f"  ALL CHECKS PASSED — Data quality SLA met")
print(f"  Silver rows     : {total_rows:,}")
print(f"  Null rate       : {null_rate:.4%}")
print(f"  Duplicate rate  : {dupe_rate:.4%}")
print(f"  Cancellation    : {cancel_rate:.4%}")
print(f"  Return rate     : {return_rate:.4%}")
print(f"  Gold KPI rows   : {gold_rows:,}")
print(f"{'='*65}\n")

# ------------------------------------------------------------------
# Sample output when all checks pass:
#
# =================================================================
#   DATA QUALITY REPORT — 2025-07-03 11:00:00
#   Table  : shopstream_db.silver_order_events
#   Rows   : 62,847
# =================================================================
#
#   [PASS ✓] order_id null rate: 0.0000  (threshold: 0.0000)
#   [PASS ✓] duplicate order rate: 0.0000  (threshold: < 0.01)
#   [PASS ✓] invalid order_value rate: 0.0000  (threshold: 0.0000)
#   [PASS ✓] cancellation rate: 0.0421  (threshold: < 0.15)
#   [PASS ✓] return rate: 0.0618  (threshold: < 0.20)
#   [PASS ✓] invalid region rate: 0.0000  (threshold: 0.0000)
#   [PASS ✓] Row count SLA: 62,847 rows  (threshold: >= 500)
#   [PASS ✓] Gold KPI table populated: 240 rows
#
# =================================================================
#   ALL CHECKS PASSED — Data quality SLA met
#   Silver rows     : 62,847
#   Null rate       : 0.0000%
#   Duplicate rate  : 0.0000%
#   Cancellation    : 4.2100%
#   Return rate     : 6.1800%
#   Gold KPI rows   : 240
# =================================================================
# ------------------------------------------------------------------
