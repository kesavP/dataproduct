"""Databricks SQL Warehouse client."""

import os
from typing import List, Dict, Any
from databricks import sql


class DatabricksSQLClient:
    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.catalog = os.getenv("DATABRICKS_CATALOG", "dev")
        self.schema = os.getenv("DATABRICKS_SCHEMA", "retail")

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        try:
            with sql.connect(
                host=self.host,
                http_path=self.http_path,
                authentication=sql.auth.PATAuth(token=self.token),
            ) as connection:
                cursor = connection.cursor()
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            raise Exception(f"Databricks query failed: {str(e)}")

    def get_orders(self, limit: int = 100, offset: int = 0) -> List[Dict]:
        """Get cleaned orders from silver layer."""
        query = f"""
            SELECT * FROM {self.catalog}.{self.schema}.orders_cleaned
            LIMIT {limit} OFFSET {offset}
        """
        return self.execute_query(query)

    def get_orders_summary(self) -> Dict[str, Any]:
        """Get order summary statistics."""
        query = f"""
            SELECT 
                COUNT(*) as total_orders,
                SUM(total_amount_eur) as total_amount,
                AVG(total_amount_eur) as avg_amount,
                MIN(order_date) as min_date,
                MAX(order_date) as max_date
            FROM {self.catalog}.{self.schema}.orders_cleaned
        """
        results = self.execute_query(query)
        return results[0] if results else {}

    def get_dlq_records(self, limit: int = 100) -> List[Dict]:
        """Get records that failed transformation."""
        query = f"""
            SELECT * FROM {self.catalog}.{self.schema}.orders_transformation_dlq
            LIMIT {limit}
        """
        return self.execute_query(query)

    def get_pipeline_metrics(self) -> Dict[str, Any]:
        """Get pipeline metrics."""
        queries = {
            "bronze": f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.clickstream",
            "silver": f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.orders_cleaned",
            "dlq": f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.orders_transformation_dlq",
        }
        
        metrics = {}
        for layer, query in queries.items():
            try:
                result = self.execute_query(query)
                metrics[f"{layer}_count"] = result[0]["count"] if result else 0
            except Exception:
                metrics[f"{layer}_count"] = 0
        
        return metrics
