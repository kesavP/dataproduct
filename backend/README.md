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

mono repo is now clean

api						# FastAPI backend wrapper
backend					# Medallion pipeline (moved from root)
frontend				# React dashboard (new)
data					# Dev/test data
docker-compose.yml		# Local dev environment
package.json			# Root scripts
Readme.md				# Setup guide
.gitignore				# Updated for monorepo
.pre-commit-config.yaml	# Shared config


1. Configure api/.env with Databricks credentials
2. Run docker-compose up to start all services
3. Access the dashboard at http://localhost:3000

1. PySpark dominance — Used in all the main adapters and production pipelines since the target is Databricks
  - Data sources: delta_data_source.py, parquet_data_source.py, csv_data_source.py
  - Data targets: delta_data_target.py, parquet_data_target.py
  - Processing: spark_dataframe_engine.py
  - Streaming: autoloader_stream_source.py, orders_streaming.py
2. Pandas is minimal — Only used in:
  - pandas_dataframe_engine.py (optional local/test adapter)
  - csv_data_source.py (reads CSV before converting to Spark)
  - Tests
3. Hexagonal separation — The ~67% "Other" code is domain logic, use cases, and ports that are framework-agnostic. This is by design — the business logic doesn't know whether it's using PySpark, Pandas, or any other framework.


tests
------
cd backend : poetry run pytest tests/ -v --cov=retail_data_product --cov-report=html
Integration test :- cd backend
poetry run pytest tests/integration/ -v

┌─────────────┬────────────────────────────────┬──────────────────────────────────┐
│  Test Type  │            Command             │             Coverage             │
├─────────────┼────────────────────────────────┼──────────────────────────────────┤
│ Unit        │ pytest tests/unit/             │ Data sources, adapters, rules    │
├─────────────┼────────────────────────────────┼──────────────────────────────────┤
│ Integration │ pytest tests/integration/      │ Spark adapters, Delta operations │
├─────────────┼────────────────────────────────┼──────────────────────────────────┤
│ E2E (local) │ python run_orders_ingestion.py │ Full pipeline with real data


Key Design: Ports & Adapters

The hexagonal architecture shines here — tests use mock adapters that implement the same DataSource, DataTarget, DataFrameEngine interfaces as production adapters. So the same use case code runs against:
- In-memory mocks (unit tests, fast)
- Pandas engine (unit tests, realistic)
- PySpark engine (integration tests, production-like)

This means you can test the full medallion pipeline end-to-end without Databricks using run_orders_ingestion.py — it's a real execution with real Parquet files, just on local Spark instead of a cluster



cd backend
poetry build
databricks bundle destroy --force-lock
databricks bundle deploy -t dev 
databricks bundle run orders_pipeline -t dev /
	--landing-dir=/Volumes/main/retail/landing/clickstream/

The following resources will be deleted:
  delete resources.jobs.orders_pipeline
  delete resources.jobs.orders_streaming
  
  
--> update the environment valiables according to environment and run lamda function 
  aws lambda invoke --function-name ingest2databricks \
  --payload $(echo '{"batch_size": 10}' | base64) \
  response.json
  
--> verify select * from dev.retail.clickstream;


