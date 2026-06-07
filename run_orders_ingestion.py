import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tests"))

from pyspark.sql import SparkSession

from retail_data_product.adapters import (
    CSVDataSource,
    ParquetDataSource,
    ParquetDataTarget,
    SparkDataFrameEngine,
)
from retail_data_product.application.use_cases.bronze.ingest_orders import IngestOrders
from retail_data_product.application.use_cases.silver.clean_orders import CleanOrders
from tests.adapters.in_memory_data_quality_logger import InMemoryDataQualityLogger

# Medallion architecture, processed end-to-end with Spark and stored as Parquet.
#   Bronze  data/orders.csv               -> lakehouse/bronze/orders_raw  (raw, as-is)
#   Silver  lakehouse/bronze/orders_raw   -> lakehouse/silver/orders_cleaned
#           (deduplicate orders keeping the latest order_date + enrich with
#            customer_name from data/customers.csv)
CSV_PATH = "data/orders.csv"
CUSTOMERS_CSV_PATH = "data/customers.csv"
BRONZE_ORDERS_PATH = "data/lakehouse/bronze/orders_raw"
SILVER_ORDERS_PATH = "data/lakehouse/silver/orders_cleaned"


def build_spark_session() -> SparkSession:
    """Create a local Spark session for the orders pipeline."""
    return (
        SparkSession.builder.master("local[1]")  # pyright: ignore[reportAttributeAccessIssue]
        .appName("orders_medallion_pipeline")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def print_dq(logger: InMemoryDataQualityLogger) -> None:
    """Pretty-print the data quality results captured by the logger."""
    for log in logger._dq_logs:
        status_symbol = "✓" if str(log["status"]).endswith("PASSED") else "✗"
        print(f"  {status_symbol} [{log['dqr_name']}] {log['status']} — {log['message']}")


spark = build_spark_session()

# -----------------------------------------------------------------------------
# Bronze: land the raw orders as-is.
# -----------------------------------------------------------------------------
print(f"=== Bronze: {CSV_PATH} -> {BRONZE_ORDERS_PATH} (Parquet) ===")

bronze_dq = InMemoryDataQualityLogger()
IngestOrders(
    orders_source=CSVDataSource(CSV_PATH),
    orders_target=ParquetDataTarget(BRONZE_ORDERS_PATH, spark_session=spark),
    dataframe_engine=SparkDataFrameEngine,
    data_quality_logger=bronze_dq,
).execute()
print_dq(bronze_dq)

# -----------------------------------------------------------------------------
# Silver: deduplicate (keep latest order_date) and enrich with customer name.
# -----------------------------------------------------------------------------
print(f"\n=== Silver: {BRONZE_ORDERS_PATH} -> {SILVER_ORDERS_PATH} (Parquet) ===")

silver_dq = InMemoryDataQualityLogger()
CleanOrders(
    orders_raw_source=ParquetDataSource(BRONZE_ORDERS_PATH, spark_session=spark),
    orders_cleaned_target=ParquetDataTarget(SILVER_ORDERS_PATH, spark_session=spark),
    dataframe_engine=SparkDataFrameEngine,
    data_quality_logger=silver_dq,
    customers_source=CSVDataSource(CUSTOMERS_CSV_PATH),
).execute()
print_dq(silver_dq)

# Read the persisted silver dataset back to confirm what was produced.
silver_orders = ParquetDataSource(SILVER_ORDERS_PATH, spark_session=spark)
rows = silver_orders.fetch(SparkDataFrameEngine).collect()

field_names = list(rows[0].keys()) if rows else []
print(f"\n--- Silver orders_cleaned ({len(rows)} rows, columns: {field_names}) ---")
for row in rows:
    print(f"  {row}")

spark.stop()
