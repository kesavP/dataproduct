"""Databricks entry point for the orders medallion pipeline.

This is the composition root used when the pipeline runs on a Databricks
cluster (via a Databricks Job / Asset Bundle wheel task). Unlike the local
``run_orders_ingestion.py`` script, it does NOT create its own ``local[1]``
SparkSession or pip-configure Delta: on Databricks the ``spark`` session is
provided by the cluster and Delta is already enabled, so the existing Delta
adapters are wired to that injected session.

Run locations are passed as Job parameters so the same wheel deploys to dev,
staging and prod without code changes:

    --orders-csv      Unity Catalog Volume path to the raw orders CSV
    --customers-csv   Unity Catalog Volume path to the customers reference CSV
    --catalog         Unity Catalog catalog for the medallion tables
    --schema          Schema/database holding the orders tables

Bronze/silver are written as Unity Catalog managed Delta tables
(``<catalog>.<schema>.orders_raw`` and ``<catalog>.<schema>.orders_cleaned``);
the Delta adapters detect the table identifier and use ``saveAsTable`` /
``spark.read.table`` instead of a storage path.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from retail_data_product.adapters import (
    CSVDataSource,
    DeltaDataSource,
    DeltaDataTarget,
    SparkDataFrameEngine,
)
from retail_data_product.application.ports import DataQualityLogger
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Orders medallion pipeline")
    parser.add_argument("--orders-csv", required=True)
    parser.add_argument("--customers-csv", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # On Databricks this returns the pre-provisioned cluster session.
    spark = SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]

    # Ensure the target schema exists, then address tables by their UC name.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")
    bronze_table = f"{args.catalog}.{args.schema}.orders_raw"
    silver_table = f"{args.catalog}.{args.schema}.orders_cleaned"

    # --- Bronze: land raw orders as-is into a UC managed Delta table. ---
    print(f"=== Bronze: {args.orders_csv} -> {bronze_table} (UC managed) ===")
    IngestOrders(
        orders_source=CSVDataSource(args.orders_csv),
        orders_target=DeltaDataTarget(bronze_table, spark_session=spark),
        dataframe_engine=SparkDataFrameEngine,
        data_quality_logger=StdoutDataQualityLogger(),
    ).execute()

    # --- Silver: deduplicate (keep latest order_date) + enrich with name. ---
    print(f"=== Silver: {bronze_table} -> {silver_table} (UC managed) ===")
    CleanOrders(
        orders_raw_source=DeltaDataSource(bronze_table, spark_session=spark),
        orders_cleaned_target=DeltaDataTarget(silver_table, spark_session=spark),
        dataframe_engine=SparkDataFrameEngine,
        data_quality_logger=StdoutDataQualityLogger(),
        customers_source=CSVDataSource(args.customers_csv),
    ).execute()

    print("=== Pipeline finished ===")


if __name__ == "__main__":
    main()
