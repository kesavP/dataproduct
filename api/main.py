"""FastAPI wrapper for Databricks medallion pipeline dashboard."""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Retail Data Product API",
    description="API for medallion pipeline dashboard",
    version="0.1.0"
)

# CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Update for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Models
class OrderSummary(BaseModel):
    total_orders: int
    total_amount: float
    avg_amount: float
    date_range: dict


class PipelineMetrics(BaseModel):
    bronze_records_ingested: int
    silver_records_cleaned: int
    dlq_records_failed: int
    last_run_timestamp: Optional[datetime]
    pipeline_status: str


class OrderRow(BaseModel):
    order_id: str
    order_date: str
    customer_name: str
    total_amount_eur: float
    campaign_id: str
    payment_status: str
    product_id: str


# Health check
@app.get("/health")
async def health_check():
    return {"status": "healthy"}


# Orders endpoints
@app.get("/api/orders/summary", response_model=OrderSummary)
async def get_orders_summary():
    """Get high-level order statistics from silver layer."""
    try:
        # TODO: Query Databricks SQL Warehouse
        # SELECT COUNT(*), SUM(total_amount_eur), AVG(total_amount_eur), 
        #        MIN(order_date), MAX(order_date)
        # FROM dev.retail.orders_cleaned
        return OrderSummary(
            total_orders=0,
            total_amount=0.0,
            avg_amount=0.0,
            date_range={"min": None, "max": None}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/orders", response_model=List[OrderRow])
async def get_orders(limit: int = 100, offset: int = 0):
    """Get cleaned orders from silver layer with pagination."""
    try:
        # TODO: Query Databricks SQL Warehouse
        # SELECT * FROM dev.retail.orders_cleaned LIMIT {limit} OFFSET {offset}
        return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Pipeline metrics endpoints
@app.get("/api/pipeline/metrics", response_model=PipelineMetrics)
async def get_pipeline_metrics():
    """Get pipeline execution metrics and data quality stats."""
    try:
        # TODO: Query bronze layer record count
        # TODO: Query silver layer record count
        # TODO: Query DLQ record count
        # TODO: Get last execution timestamp
        return PipelineMetrics(
            bronze_records_ingested=0,
            silver_records_cleaned=0,
            dlq_records_failed=0,
            last_run_timestamp=None,
            pipeline_status="pending"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/pipeline/dlq")
async def get_dlq_records(limit: int = 100):
    """Get records that failed transformation and landed in DLQ."""
    try:
        # TODO: Query Databricks SQL Warehouse
        # SELECT * FROM dev.retail.orders_transformation_dlq LIMIT {limit}
        return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
