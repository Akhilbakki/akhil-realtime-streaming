# Akhil Realtime Streaming

A production-grade streaming project built on Azure Event Hubs + Databricks Structured Streaming + Delta Lake

## Project Structure

```
akhil-realtime-streaming/
├── producer/
│   └── event_producer.py        # Simulates FedEx package events
├── notebooks/
│   ├── 01_bronze_ingestion.py   # Structured Streaming → Bronze
│   ├── 02_silver_transform.py   # Clean, validate, deduplicate
│   ├── 03_gold_aggregation.py   # KPIs, metrics aggregation
│   └── 04_data_quality.py       # Great Expectations checks
├── tests/
│   ├── test_producer.py
│   └── test_transformations.py
├── .github/workflows/
│   └── ci.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## Getting Started

### Prerequisites
- Python 3.9+
- Databricks workspace
- Azure Event Hubs instance

### Installation

1. Clone the repository:
```bash
git clone https://github.com/Akhilbakki/akhil-realtime-streaming.git
cd akhil-realtime-streaming
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Running the Producer
```bash
python producer/event_producer.py
```

### Running the Notebooks
Execute notebooks in order:
1. `01_bronze_ingestion.py` - Ingest raw events
2. `02_silver_transform.py` - Transform and clean data
3. `03_gold_aggregation.py` - Create aggregations
4. `04_data_quality.py` - Validate data quality

### Running Tests
```bash
pytest tests/
```

## Contributing

Please create a pull request with your changes.

## License

MIT License
