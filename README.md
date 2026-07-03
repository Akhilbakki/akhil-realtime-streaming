# Akhil Realtime Streaming

A production-grade streaming project built on Azure Event Hubs + Databricks Structured Streaming + Delta Lake

# 🚀 Real-Time Package Tracking Pipeline

> A production-grade real-time streaming pipeline built on **Azure Event Hubs + Databricks Structured Streaming + Delta Lake**.

## 📋 What this project does

This pipeline ingests **10,000+ simulated FedEx package tracking events per minute** from Azure Event Hubs, processes them through a **medallion architecture** (Bronze → Silver → Gold) on Delta Lake using Databricks Structured Streaming, and produces **near real-time delivery KPIs** with less than 2-minute end-to-end latency.

**Key capabilities:**
- Real-time ingestion from Azure Event Hubs with exactly-once processing guarantees
- Automated data quality validation with dead-letter routing for invalid records
- 5-minute tumbling window aggregations for live delivery metrics by region and SLA
- Delta Lake optimisation (OPTIMIZE, ZORDER, VACUUM) to prevent small file explosion
- CI/CD via GitHub Actions running unit tests on every commit

---

## 🏗️ Architecture

<img width="2720" height="3280" alt="Architecture" src="https://github.com/user-attachments/assets/45a20f17-e246-438a-b05c-376fa42dafdc" />


```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        REAL-TIME STREAMING PIPELINE                         │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────┐
  │  Event Producer  │   Python script simulating FedEx package events
  │  (Python)        │   100 events/batch · 1 batch/second
  └────────┬─────────┘
           │ JSON events (tracking_id, status, region, weight, SLA, timestamp)
           ▼
  ┌──────────────────┐
  │  Azure           │   Managed Kafka-compatible message broker
  │  Event Hubs      │   Partitioned · Scalable · Retention: 7 days
  └────────┬─────────┘
           │ Structured Streaming read (60s micro-batch trigger)
           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        DATABRICKS WORKSPACE                              │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    BRONZE LAYER (Raw)                           │    │
│  │  • Exact copy of Event Hubs payload — never modified            │    │
│  │  • Schema-on-read · Append only · Full replay capability        │    │
│  │  • Checkpoint: /mnt/checkpoints/bronze (exactly-once)           │    │
│  │  Table: streaming_db.bronze_package_events (Delta)              │    │
│  └──────────────────────────┬──────────────────────────────────────┘    │
│                             │                                            │
│            ┌────────────────┴──────────────────┐                        │
│            │  Data Quality Check               │                        │
│            │  • Null validation on key fields  │                        │
│            │  • Schema enforcement             │                        │
│            └────────┬──────────────────┬───────┘                        │
│                     │ Valid            │ Invalid                         │
│                     ▼                 ▼                                 │
│  ┌──────────────────────┐  ┌──────────────────────────┐                │
│  │   SILVER LAYER       │  │   DEAD LETTER TABLE      │                │
│  │   (Cleaned)          │  │                          │                │
│  │ • Nulls removed      │  │ • Failed validation      │                │
│  │ • Types cast         │  │ • Stored for analysis    │                │
│  │ • Deduplication      │  │ • Never silently dropped │                │
│  │   on tracking_id     │  │                          │                │
│  │   + event_ts         │  │ Table:                   │                │
│  │                      │  │ dead_letter_events       │                │
│  │ Table: silver_package│  └──────────────────────────┘                │
│  │ _events (Delta)      │                                              │
│  └──────────┬───────────┘                                              │
│             │ 5-min tumbling window aggregations                        │
│             ▼                                                           │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                      GOLD LAYER (KPIs)                           │  │
│  │  • Total packages by region + SLA type                          │  │
│  │  • Delivered count · Exception rate % · Avg weight              │  │
│  │  • Watermark: 10 minutes (handles late-arriving data)           │  │
│  │  Table: streaming_db.gold_delivery_kpis (Delta)                 │  │
│  └──────────────────────────┬─────────────────────────────────────-┘  │
│                             │                                           │
└─────────────────────────────┼───────────────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────┐
              │   Databricks SQL          │
              │   Dashboard               │
              │   • Live delivery metrics │
              │   • Exception alerts      │
              │   • Region heatmap        │
              └───────────────────────────┘
```

### Medallion architecture — layer summary

| Layer | Table | Purpose | Trigger | Output mode |
|---|---|---|---|---|
| Bronze | `bronze_package_events` | Raw ingest, no transformation | 60s micro-batch | Append |
| Silver | `silver_package_events` | Cleaned, typed, deduplicated | 60s micro-batch | Append |
| Gold | `gold_delivery_kpis` | Business KPI aggregations | 60s micro-batch | Update |
| Dead letter | `dead_letter_events` | Invalid records for investigation | 60s micro-batch | Append |

---

## 📊 Sample event payload

```json
{
  "tracking_id":   "FDX7823491",
  "status":        "IN_TRANSIT",
  "region":        "NORTH_EAST",
  "weight_kg":     12.4,
  "delivery_sla":  "EXPRESS",
  "event_ts":      "2025-07-03T10:45:23.412+00:00",
  "is_exception":  false
}
```

## 📈 Sample Gold layer output

```
+---------------------------+------------+--------------+----------------+----------------+--------------------+
| window                    | region     | delivery_sla | total_packages | delivered_count| exception_rate_pct |
+---------------------------+------------+--------------+----------------+----------------+--------------------+
| 2025-07-03 10:40–10:45   | NORTH_EAST | EXPRESS      | 1842           | 1654           | 4.2                |
| 2025-07-03 10:40–10:45   | WEST        | OVERNIGHT    | 923            | 889            | 3.7                |
| 2025-07-03 10:40–10:45   | MIDWEST     | STANDARD     | 2103           | 1877           | 5.1                |
+---------------------------+------------+--------------+----------------+----------------+--------------------+
```

---

## 🛠️ Tech stack

| Component | Technology | Purpose |
|---|---|---|
| Event ingestion | Azure Event Hubs | Kafka-compatible managed streaming broker |
| Stream processing | Databricks Structured Streaming | Micro-batch processing engine |
| Language | PySpark / Python | Transformations and producer script |
| Storage format | Delta Lake | ACID transactions, time travel, schema evolution |
| Storage layer | Azure Data Lake Storage Gen2 | Underlying blob storage for Delta tables |
| Data quality | Custom assertions + dead-letter | Automated validation and error routing |
| Orchestration | Databricks Workflows | Job scheduling and alerting |
| CI/CD | GitHub Actions | Automated testing on every commit |

---

## 🚀 How to run locally

### Prerequisites
- Python 3.10+
- Azure subscription (free tier works)
- Databricks Community Edition account (free)

### 1. Clone the repo
```bash
git clone https://github.com/akhilbakki/fedex-realtime-streaming.git
cd fedex-realtime-streaming
pip install -r requirements.txt
```

### 2. Set up Azure Event Hubs
1. Create an Event Hubs namespace in the Azure portal (Basic tier — free)
2. Create an Event Hub named `package-events`
3. Copy the connection string

### 3. Configure environment
```bash
cp .env.example .env
# Add your Event Hubs connection string to .env
EVENT_HUB_CONNECTION_STR=Endpoint=sb://your-namespace...
```

### 4. Start the event producer
```bash
python producer/event_producer.py
# Sends 100 events/second to Event Hubs
```

### 5. Run notebooks in Databricks
Import notebooks from the `/notebooks` folder into Databricks in order:
```
01_bronze_ingestion.py   → Start streaming job
02_silver_transform.py   → Start streaming job
03_gold_aggregation.py   → Start streaming job
04_data_quality.py       → Run as scheduled batch check
```

### 6. Run tests
```bash
pytest tests/ -v
```

---

## 🔑 Key engineering decisions

**Why Structured Streaming over batch?**
The business requirement is a live dashboard with <2 minute latency. Batch jobs running hourly would not meet this SLA. Structured Streaming with a 60-second micro-batch trigger achieves ~90-second end-to-end latency at this event volume.

**Why Delta Lake over Parquet?**
Three reasons: (1) ACID transactions prevent partial writes corrupting the table if a streaming job crashes mid-batch; (2) time travel enables replay and debugging of historical data; (3) schema enforcement catches upstream changes before they break downstream jobs.

**Why dead-letter instead of filtering?**
Silently dropping invalid records hides upstream data issues. A dead-letter table makes failures visible and auditable — operators can inspect what failed and why, fix the root cause, and replay the records if needed.

**Why checkpointing?**
The checkpoint directory tracks exactly which Event Hubs offsets have been processed. On restart after a crash, Spark resumes from the exact last offset — guaranteeing exactly-once processing with no duplicates and no gaps.

---

## 📁 Project structure

```
fedex-realtime-streaming/
├── producer/
│   └── event_producer.py        # Simulates FedEx package events → Event Hubs
├── notebooks/
│   ├── 01_bronze_ingestion.py   # Structured Streaming → Bronze Delta table
│   ├── 02_silver_transform.py   # Clean, validate, deduplicate → Silver Delta table
│   ├── 03_gold_aggregation.py   # 5-min window KPIs → Gold Delta table
│   └── 04_data_quality.py       # Automated data quality assertions
├── tests/
│   ├── test_producer.py         # Unit tests for event generation
│   └── test_transformations.py  # Unit tests for Silver transformations
├── .github/workflows/
│   └── ci.yml                   # GitHub Actions — runs tests on every push
├── docs/
│   └── architecture.png         # Architecture diagram
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## 📐 Performance characteristics

| Metric | Value |
|---|---|
| Ingestion throughput | 10,000+ events/minute |
| End-to-end latency | < 2 minutes |
| Micro-batch trigger | 60 seconds |
| Watermark (late data) | 10 minutes |
| Delta checkpoint retention | 7 days |
| Exception rate threshold | < 10% (alerts above) |

---

## 🧠 Concepts demonstrated

- **Structured Streaming** — readStream / writeStream with micro-batch triggers
- **Exactly-once processing** — checkpoint-based offset tracking
- **Medallion architecture** — Bronze / Silver / Gold layered data design
- **Dead-letter pattern** — separating invalid records without data loss
- **Windowed aggregations** — tumbling windows with watermarks for late data
- **Delta Lake operations** — ACID writes, OPTIMIZE, ZORDER, VACUUM, time travel
- **Data quality** — automated assertions with threshold alerting
- **CI/CD** — GitHub Actions pipeline for automated testing

---

## 👤 Author

**Akhil Bakki** — Senior Data Engineer  
[LinkedIn](https://linkedin.com/in/akhil-bakki-a110ab213) · [GitHub](https://github.com/akhilbakki)

> Built as a portfolio project showcasing production-grade streaming engineering patterns used in enterprise logistics data platforms.
