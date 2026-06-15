# Retail Data Product - Monorepo

A full-stack data platform with a medallion architecture backend and React dashboard frontend.

## Structure

```
├── backend/                   # Python/Databricks medallion pipeline
│   ├── retail_data_product/   # Core domain and application logic
│   ├── tests/                 # Unit and integration tests
│   ├── pyproject.toml         # Poetry dependencies
│   ├── databricks.yml         # Databricks Asset Bundle config
│   └── run_*.py               # Local development scripts
│
├── api/                       # FastAPI wrapper for Databricks
│   ├── main.py                # FastAPI application
│   ├── databricks_client.py   # Databricks SQL client
│   ├── pyproject.toml         # API dependencies
│   ├── Dockerfile
│   └── .env.example
│
├── frontend/                  # React dashboard
│   ├── src/
│   │   ├── pages/             # Dashboard pages
│   │   ├── components/        # React components
│   │   ├── services/          # API client
│   │   └── main.jsx           # Entry point
│   ├── package.json
│   ├── vite.config.js
│   ├── Dockerfile
│   └── index.html
│
└── docker-compose.yml         # Local development environment
```

## Quick Start

### Prerequisites
- Docker & Docker Compose (for containerized setup)
- OR: Python 3.11+, Node.js 18+, Poetry (for local development)
- Databricks workspace with SQL Warehouse
- Databricks personal access token

### Environment Setup

1. **Copy environment template:**
```bash
cp api/.env.example api/.env
```

2. **Configure Databricks credentials:**
```bash
# Edit api/.env with your Databricks details:
DATABRICKS_HOST=your-workspace-url
DATABRICKS_TOKEN=your-token
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
```

### Option 1: Docker Compose (Recommended)

```bash
docker-compose up
```

- Frontend: http://localhost:3000
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs

### Option 2: Local Development

**Backend (medallion pipeline):**
```bash
cd backend
poetry install
poetry run python run_orders_ingestion.py
```

**API:**
```bash
cd api
poetry install
poetry run uvicorn main:app --reload
```

**Frontend:**
```bash
cd frontend
npm install
npm run dev
```

## Features

### Dashboard
- **Pipeline Metrics** - Monitor bronze/silver/DLQ record counts
- **Order Summary** - Total orders, revenue, averages
- **Orders Table** - Browse cleaned orders from silver layer
- **DLQ** - Investigate failed records

### API Endpoints
- `GET /health` - Health check
- `GET /api/orders/summary` - Order statistics
- `GET /api/orders` - Paginated cleaned orders
- `GET /api/pipeline/metrics` - Pipeline execution metrics
- `GET /api/pipeline/dlq` - Dead letter queue records

### Backend
- Hexagonal architecture (domain, adapters, ports)
- Databricks medallion pipeline (bronze → silver)
- Data quality validation & DLQ handling
- Streaming (Auto Loader) and batch ingestion support

## Development

### Adding a new dashboard page:
1. Create component in `frontend/src/pages/`
2. Add API client call in `frontend/src/services/api.js`
3. Register route in `frontend/src/App.jsx`

### Adding a new API endpoint:
1. Add Databricks client method in `api/databricks_client.py`
2. Create endpoint in `api/main.py`
3. Call from frontend via `frontend/src/services/api.js`

## Deployment

### Frontend
- Build: `npm run build`
- Deploy to: Vercel, Netlify, or S3 + CloudFront

### API
- Build Docker image: `docker build -t retail-api:latest api/`
- Deploy to: ECS, Cloud Run, Kubernetes

### Backend
- Deployed via Databricks Asset Bundle:
  ```bash
  cd backend
  databricks bundle deploy -t prod
  databricks bundle run orders_pipeline -t prod
  ```

## Configuration

**Catalog/Schema**: Currently using `dev.retail` for all tables (configured in `api/.env` and `backend/databricks.yml`)

## Next Steps

- [ ] Add authentication to API
- [ ] Add data quality dashboards
- [ ] Add real-time metrics updates (WebSocket)
- [ ] Add user authentication to frontend
- [ ] Add CI/CD pipeline (GitHub Actions)
- [ ] Add comprehensive test coverage
