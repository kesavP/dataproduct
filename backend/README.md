# Retail Data Product - Medallion Pipeline

A data product using hexagonal architecture (ports & adapters) to implement a medallion architecture (bronze → silver → gold layers) for retail order data on Databricks.

## Architecture

- **Domain**: Core business entities (Order, Campaign, Payment) and data quality rules
- **Application**: Use cases (IngestOrders, CleanOrders, EnrichCampaigns) 
- **Adapters**: Concrete implementations (CSV, Parquet, Delta, Spark, Pandas, AutoLoader, Kafka)
- **Ports**: Interfaces (DataSource, DataTarget, DataFrameEngine, DataQualityLogger)
- **Entrypoints**: Databricks jobs and local dev scripts

## Quick Start

### Install

```bash
poetry install
```

### Run Local Pipeline

```bash
poetry run python run_orders_ingestion.py
```

Processes sample CSV data through bronze → silver layers with data quality validation.

### Run Tests

```bash
# Unit tests (fast, in-memory)
poetry run pytest tests/unit/ -v

# Integration tests (uses Spark)
poetry run pytest tests/integration/ -v

# All tests with coverage
poetry run pytest tests/ --cov=retail_data_product --cov-report=html
```

### Deploy to Databricks

```bash
databricks bundle deploy -t dev
databricks bundle run orders_pipeline -t dev
```

## Project Structure

```
retail_data_product/
├── domain/              # Business logic
│   ├── data_governance/ # DQ rules, schemas, table definitions
│   └── retail/          # Orders, campaigns, payments
├── application/         # Orchestration
│   ├── use_cases/       # Bronze/Silver/Gold layer logic
│   └── ports/           # Interfaces
├── adapters/            # Implementations
│   ├── csv_data_source.py
│   ├── delta_data_source.py
│   ├── spark_dataframe_engine.py
│   └── ...
└── entrypoints/         # Entry points
    ├── orders_pipeline.py    # Databricks batch job
    ├── orders_streaming.py   # Databricks streaming job
    └── orders_api.py         # REST API wrapper
```

## Key Files

- `pyproject.toml` — Poetry dependencies and entry points
- `databricks.yml` — Databricks Asset Bundle configuration
- `run_orders_ingestion.py` — Local development script
- `tests/` — Unit, integration, and template tests

## Environment Variables

Local development uses defaults. For Databricks:

```bash
# Copy from api/.env.example
DATABRICKS_HOST=<workspace-url>
DATABRICKS_TOKEN=<personal-access-token>
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
DATABRICKS_CATALOG=dev
DATABRICKS_SCHEMA=retail
```

## Design Patterns

- **Hexagonal Architecture**: Core logic separated from framework code
- **Ports & Adapters**: Swap Spark/Pandas/etc without changing use cases
- **Data Quality Checks**: Validation rules on ingestion and transformation
- **Dead Letter Queue**: Failed records captured for debugging
- **Medallion Pattern**: Bronze (raw) → Silver (cleaned) → Gold (analytics)
