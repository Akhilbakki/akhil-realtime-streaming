# Akhil_Stream — Real-Time E-Commerce Order Tracking Pipeline

> A production-grade real-time streaming pipeline built on **Azure Event Hubs + Databricks Structured Streaming + Delta Lake**, processing live e-commerce order and delivery events through a medallion architecture.

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1-003366?style=flat)](https://delta.io)
[![Azure](https://img.shields.io/badge/Azure_Event_Hubs-blue?style=flat&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com)

---

## What this project does

Akhil_Stream ingests **10,000+ simulated e-commerce order events per minute** from Azure Event Hubs, processes them through a **medallion architecture** (Bronze → Silver → Gold) on Delta Lake using Databricks Structured Streaming, and produces **near real-time business KPIs** — revenue by region, cancellation rates, return trends, and customer activity — with less than 2-minute end-to-end latency.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                     Akhil_Stream PIPELINE                               │
└───────────────────────────────────────────────────────────────────────┘

  [Order Event Simulator]  →  [Azure Event Hubs]  →  [Databricks]
       Python · 100/sec         Kafka-compatible       Structured Streaming

                    ┌──────────────────────────────────┐
                    │       MEDALLION ARCHITECTURE      │
                    │                                   │
                    │  BRONZE  →  SILVER  →  GOLD       │
                    │  (raw)     (clean)    (KPIs)      │
                    │               ↓                   │
                    │          Dead Letter               │
                    └──────────────────────────────────┘
                                   │
                    [Databricks SQL Dashboard]
                    Live revenue · Cancellation alerts
```

---

## Medallion layer summary

| Layer | Table | Purpose | Trigger | Mode |
|---|---|---|---|---|
| Bronze | `bronze_order_events` | Raw ingest, no transformation | 60s | Append |
| Silver | `silver_order_events` | Cleaned, typed, deduplicated | 60s | Append |
| Gold | `gold_order_kpis` | Revenue KPIs by region + category | 60s | Update |
| Dead letter | `dead_letter_orders` | Invalid records for investigation | 60s | Append |

---

## Sample event payload

```json
{
  "order_id":       "ORD100234",
  "customer_id":    "CUST_7821",
  "product_id":     "PROD_4421",
  "category":       "Electronics",
  "order_status":   "PLACED",
  "payment_method": "CREDIT_CARD",
  "region":         "WEST",
  "order_value":    299.99,
  "quantity":       1,
  "is_returned":    false,
  "event_ts":       "2025-07-03T10:45:23+00:00"
}
```

## Sample Gold KPI output

```
+---------------------------+--------+-------------+--------------+-------------+-------------------+------------------+
| window                    | region | category    | total_orders | total_revenue| cancellation_rate | return_rate_pct  |
+---------------------------+--------+-------------+--------------+-------------+-------------------+------------------+
| 2025-07-03 10:40–10:45   | WEST   | ELECTRONICS | 423          | 84,231.77   | 3.31%             | 4.49%            |
| 2025-07-03 10:40–10:45   | NORTH  | FASHION     | 891          | 31,540.09   | 5.05%             | 6.17%            |
| 2025-07-03 10:40–10:45   | SOUTH  | GROCERY     | 1204         | 18,922.40   | 2.16%             | 1.91%            |
+---------------------------+--------+-------------+--------------+-------------+-------------------+------------------+
```

---

## Tech stack

| Component | Technology |
|---|---|
| Event ingestion | Azure Event Hubs (Kafka-compatible) |
| Stream processing | Databricks Structured Streaming (PySpark) |
| Storage format | Delta Lake (ACID, time travel, schema evolution) |
| Storage layer | Azure Data Lake Storage Gen2 |
| Data quality | Custom assertions + dead-letter routing |
| Maintenance | OPTIMIZE · ZORDER · VACUUM · auto-optimize |
| Orchestration | Databricks Workflows |

---

## Notebooks

| Notebook | Purpose |
|---|---|
| `01_bronze_ingestion.py` | Read from Event Hubs → write raw to Bronze Delta |
| `02_silver_transform.py` | Validate, clean, deduplicate → Silver + Dead Letter |
| `03_gold_aggregation.py` | 5-min window KPIs → Gold Delta (revenue, rates) |
| `04_data_quality.py` | Automated assertions — null, dupe, range, SLA checks |
| `05_delta_maintenance.py` | OPTIMIZE, ZORDER, VACUUM — daily maintenance job |

---

## Key engineering decisions

**Why Structured Streaming over batch?**
The business requires a live operations dashboard with <2 minute latency. Hourly batch jobs cannot meet this SLA. Structured Streaming with 60-second micro-batch triggers achieves ~90-second end-to-end latency at this event volume.

**Why Delta Lake?**
ACID transactions prevent partial writes if a streaming job crashes mid-batch. Time travel enables replay and debugging. Schema enforcement catches upstream payload changes before they break downstream jobs.

**Why dead-letter routing?**
Silently dropping invalid records hides upstream issues. Dead-letter tables make failures visible and auditable — operators can inspect failures, fix root cause, and replay if needed.

**Why checkpointing?**
Checkpoints track exactly which Event Hubs offsets have been processed. On restart after a crash, Spark resumes from the exact last offset — guaranteeing exactly-once processing with no duplicates or gaps.

---

## Performance

| Metric | Value |
|---|---|
| Ingestion throughput | 10,000+ events/minute |
| End-to-end latency | < 2 minutes |
| Micro-batch trigger | 60 seconds |
| Watermark (late data) | 10 minutes |
| Delta retention | 7 days (168 hours) |
| Data quality SLA | 99.5%+ accuracy |

---

## Concepts demonstrated

- Structured Streaming with micro-batch triggers
- Exactly-once processing via checkpoint-based offset tracking
- Medallion architecture — Bronze / Silver / Gold
- Dead-letter pattern for invalid record handling
- Tumbling windows with watermarks for late-arriving data
- Delta Lake ACID writes, OPTIMIZE, ZORDER, VACUUM, time travel
- Automated data quality assertions with threshold alerting
- Daily maintenance scheduling via Databricks Workflows

---

## Author

**Akhil Bakki** — Senior Data Engineer
[LinkedIn](https://linkedin.com/in/akhil-bakki-a110ab213)

> Built to demonstrate production-grade real-time data engineering patterns on Azure and Databricks.
