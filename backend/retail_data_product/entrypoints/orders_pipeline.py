"""Databricks entry point for the orders medallion pipeline.

This is the composition root used when the pipeline runs on a Databricks
cluster (via a Databricks Job / Asset Bundle wheel task). Unlike the local
``run_orders_ingestion.py`` script, it does NOT create its own ``local[1]``
SparkSession or pip-configure Delta: on Databricks the ``spark`` session is
provided by the cluster and Delta is already enabled, so the existing Delta
adapters are wired to that injected session.

Run locations are passed as Job parameters so the same wheel deploys to dev,
staging and prod without code changes:

    --orders-path           Path to the raw orders dataset (UC Volume or cloud URI
                            such as abfss://...); CSV file or Parquet directory
    --orders-format         Format of the orders dataset: "csv" (default) or "parquet"
    --orders-cleaned-path   S3 path for the external orders_cleaned Delta table
    --customers-csv         Unity Catalog Volume path to the customers reference CSV
    --catalog               Unity Catalog catalog for the medallion tables
    --schema                Schema/database holding the orders tables

Bronze is created as an external table pointing to orders_path. Silver (orders_cleaned)
is written as a Delta table on S3 and registered as an external UC table pointing to
that location. The Delta adapters detect paths vs. table identifiers automatically.
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

from retail_data_product.adapters import (
    CSVDataSource,
    DeltaDataSource,
    DeltaDataTarget,
    ParquetDataSource,
    SparkDataFrameEngine,
)
from retail_data_product.application.ports import DataQualityLogger, DataSource
from retail_data_product.application.use_cases.bronze.ingest_orders import IngestOrders
from retail_data_product.application.use_cases.silver.clean_orders import CleanOrders
from retail_data_product.domain.data_governance import DataQualityReport


class StdoutDataQualityLogger(DataQualityLogger):
    """Minimal DQ logger that prints to the driver log (visible in Job output).

    Replace with an adapter that writes the report to a Delta audit table or
    MLflow for a real deployment; it implements the same ``log`` port.
    """

    def log(self, data_quality_report: DataQualityReport) -> None:
        print(f"[DQ] table={data_quality_report.table_name}")
        for result in data_quality_report.data_quality_results:
            print(f"  - {result}")


def build_orders_source(
    orders_format: str, path: str, spark: SparkSession
) -> DataSource:
    """Pick the source adapter for the orders dataset based on its format."""
    if orders_format == "parquet":
        return ParquetDataSource(path, spark_session=spark)
    if orders_format == "delta":
        return DeltaDataSource(path, spark_session=spark)
    return CSVDataSource(path)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Orders medallion pipeline")
    parser.add_argument("--orders-path", required=True)
    parser.add_argument(
        "--orders-format", choices=("csv", "parquet", "delta"), default="csv"
    )
    parser.add_argument("--orders-cleaned-path", required=True)
    parser.add_argument("--customers-csv", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    logger.info("=== Orders Pipeline Started ===")
    logger.info(f"Arguments:")
    logger.info(f"  orders_path: {args.orders_path}")
    logger.info(f"  orders_format: {args.orders_format}")
    logger.info(f"  orders_cleaned_path: {args.orders_cleaned_path}")
    logger.info(f"  customers_csv: {args.customers_csv}")
    logger.info(f"  catalog: {args.catalog}")
    logger.info(f"  schema: {args.schema}")

    # On Databricks this returns the pre-provisioned cluster session.
    spark = SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]

    # Ensure the target schema exists, then address tables by their UC name.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    bronze_table = f"{args.catalog}.{args.schema}.clickstream"
    silver_table = args.orders_cleaned_path
    silver_table_name = f"{args.catalog}.{args.schema}.orders_cleaned"

    # Validate critical paths are not null/empty
    if not silver_table or not str(silver_table).strip():
        logger.error(f"FATAL: silver_table (orders_cleaned_path) is null or empty!")
        raise ValueError(f"orders_cleaned_path cannot be null or empty. Received: {repr(silver_table)}")

    logger.info(f"Resolved table names:")
    logger.info(f"  bronze_table: {bronze_table}")
    logger.info(f"  silver_table (S3 path): {silver_table}")
    logger.info(f"  silver_table_name (UC ref): {silver_table_name}")
    logger.info(f"  silver_table type: {type(silver_table)}")
    logger.info(f"  silver_table length: {len(str(silver_table))}")

    # --- Bronze: Create external table pointing to orders_path ---
    logger.info(f"=== Bronze: Creating external table {bronze_table} from {args.orders_path} ===")
    format_spec = args.orders_format.upper()
    logger.debug(f"Format spec: {format_spec}")

    try:
        sql_stmt = f"""
            CREATE TABLE IF NOT EXISTS {bronze_table}
            USING {format_spec}
            LOCATION '{args.orders_path}'
        """
        logger.debug(f"Executing SQL: {sql_stmt}")
        spark.sql(sql_stmt)
        logger.info(f"Bronze table created/verified successfully")
    except Exception as e:
        logger.error(f"Error creating bronze table: {str(e)}", exc_info=True)
        if "LOCATION_OVERLAP" in str(e) or "already exists" in str(e):
            logger.info(f"Table or table with overlapping location already exists")
        else:
            raise
    logger.info(f"=== Bronze: Reading from external table {bronze_table} ===")

    # --- Silver: deduplicate (keep latest order_date) + enrich with name. ---
    logger.info(f"=== Silver: {bronze_table} -> {silver_table} (external Delta on S3) ===")
    try:
        logger.info(f"Creating DeltaDataTarget for path: {repr(silver_table)}")
        logger.info(f"  Type: {type(silver_table)}")
        logger.info(f"  Is None: {silver_table is None}")
        logger.info(f"  Is empty string: {silver_table == ''}")
        logger.info(f"  Length: {len(str(silver_table))}")

        orders_cleaned_target = DeltaDataTarget(silver_table, spark_session=spark)
        logger.info(f"DeltaDataTarget created successfully")
        logger.info(f"  is_table: {orders_cleaned_target.is_table}")
        logger.info(f"  path: {repr(orders_cleaned_target.path)}")
        logger.info(f"  path type: {type(orders_cleaned_target.path)}")
        logger.info(f"  path is None: {orders_cleaned_target.path is None}")

        logger.info(f"Starting CleanOrders execution")
        CleanOrders(
            orders_raw_source=DeltaDataSource(bronze_table, spark_session=spark),
            orders_cleaned_target=orders_cleaned_target,
            dataframe_engine=SparkDataFrameEngine,
            data_quality_logger=StdoutDataQualityLogger(),
            customers_source=CSVDataSource(args.customers_csv),
        ).execute()
        logger.info(f"CleanOrders execution completed successfully")
    except Exception as e:
        logger.error(f"Error during silver layer transformation: {str(e)}", exc_info=True)
        raise

    # Create external table reference pointing to the S3 location
    logger.info(f"=== Creating external table reference {silver_table_name} ===")
    logger.info(f"Creating external table pointing to location: {silver_table}")
    try:
        sql_stmt = f"""
            CREATE TABLE IF NOT EXISTS {silver_table_name}
            USING DELTA
            LOCATION '{silver_table}'
        """
        logger.debug(f"Executing SQL: {sql_stmt}")
        spark.sql(sql_stmt)
        logger.info(f"External table reference created/verified successfully")
    except Exception as e:
        logger.error(f"Error creating external table reference: {str(e)}", exc_info=True)
        if "LOCATION_OVERLAP" in str(e) or "already exists" in str(e):
            logger.info(f"Table or table with overlapping location already exists")
        else:
            raise

    logger.info("=== Pipeline finished ===")


if __name__ == "__main__":
    main()
