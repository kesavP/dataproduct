"""FastAPI wrapper for Databricks medallion pipeline dashboard."""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
import os
import sys
import httpx
import asyncio
import logging
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Add the api directory to the path so we can import databricks_client
sys.path.insert(0, str(Path(__file__).parent))
from databricks_client import DatabricksSQLClient

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


class DatabricksStatementRequest(BaseModel):
    statement: str
    warehouse_id: Optional[str] = None
    byte_limit: Optional[int] = 16777216


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


# Bronze layer endpoints
@app.get("/api/bronze")
async def get_bronze_data(limit: int = 100, offset: int = 0):
    """Get raw data from bronze layer (clickstream table)."""
    try:
        client = DatabricksSQLClient()
        data = client.get_bronze_data(limit=limit, offset=offset)
        return {
            "data": data,
            "count": len(data),
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/bronze/count")
async def get_bronze_count():
    """Get total count of records in bronze layer."""
    try:
        client = DatabricksSQLClient()
        count = client.get_bronze_count()
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/databricks/query")
async def execute_databricks_query(request: DatabricksStatementRequest):
    """Execute a SQL query directly against Databricks SQL Warehouse."""
    try:
        logger.info("=== Databricks Query Request ===")
        logger.info(f"SQL Statement: {request.statement}")
        logger.info(f"Warehouse ID: {request.warehouse_id}")
        logger.info(f"Byte Limit: {request.byte_limit}")

        databricks_host = os.getenv("DATABRICKS_HOST")
        databricks_token = os.getenv("DATABRICKS_TOKEN")
        warehouse_id = request.warehouse_id or os.getenv("DATABRICKS_WAREHOUSE_ID", "880e5b3c9bc72fe6")

        logger.info(f"Raw DATABRICKS_HOST from env: {databricks_host}")
        logger.info(f"Raw DATABRICKS_TOKEN from env: {'[SET]' if databricks_token else '[NOT SET]'}")

        if not databricks_host or not databricks_token:
            logger.error("Missing Databricks credentials: DATABRICKS_HOST or DATABRICKS_TOKEN not set")
            raise HTTPException(
                status_code=500,
                detail="Databricks credentials not configured"
            )

        # Remove https:// prefix if it exists in the hostname
        if databricks_host.startswith("https://"):
            databricks_host = databricks_host.replace("https://", "")
        elif databricks_host.startswith("http://"):
            databricks_host = databricks_host.replace("http://", "")

        # Remove trailing slashes
        databricks_host = databricks_host.rstrip("/")

        url = f"https://{databricks_host}/api/2.0/sql/statements"
        logger.info(f"Cleaned Databricks Host: {databricks_host}")
        logger.info(f"Databricks API URL: {url}")

        headers = {
            "Authorization": f"Bearer {databricks_token}",
            "Content-Type": "application/json"
        }
        logger.debug(f"Request Headers: Authorization=Bearer [REDACTED], Content-Type=application/json")

        payload = {
            "warehouse_id": warehouse_id,
            "statement": request.statement,
            "byte_limit": request.byte_limit
        }
        logger.info(f"Request Payload: {payload}")

        logger.info(f"Sending POST request to Databricks API...")
        logger.info(f"Request Method: POST")
        logger.info(f"Request URL: {url}")
        logger.info(f"Request Headers: {headers}")
        logger.info(f"Request Body: {payload}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            logger.info(f"Response Status Code: {response.status_code}")
            logger.info(f"Response Headers: {dict(response.headers)}")
            logger.info(f"Response Text: {response.text}")

            if response.status_code != 200:
                logger.error(f"Databricks API returned non-200 status: {response.status_code}")
                logger.error(f"Response body: {response.text}")

            try:
                result = response.json()
                logger.info(f"Response JSON parsed successfully")
                logger.debug(f"Response Body: {result}")
                return result
            except Exception as json_error:
                logger.error(f"Failed to parse response as JSON: {json_error}")
                logger.error(f"Response text was: {response.text}")
                raise HTTPException(status_code=500, detail=f"Failed to parse Databricks response: {response.text}")

    except httpx.HTTPError as e:
        logger.error(f"Databricks API HTTP Error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Full error: {e}")
        raise HTTPException(status_code=500, detail=f"Databricks API error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in execute_databricks_query: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
