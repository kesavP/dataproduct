"""Databricks entry point for the streaming orders pipeline (Auto Loader).

Deployable, parameterised composition root for Option A. Auto Loader
incrementally ingests order files landing in a cloud directory and a
``foreachBatch`` callback runs the existing batch use cases (``IngestOrders``
then ``CleanOrders``) against each bounded micro-batch — reusing the DQ checks
and Delta MERGE verbatim.

This is the canonical streaming logic; ``run_orders_streaming.py`` at the repo
root is a thin local-demo wrapper that delegates to ``make_batch_processor``.

Run as a continuous Databricks Job (Python wheel task, entry point
``orders_streaming``). Auto Loader (``cloudFiles``) requires a Databricks
Runtime — it is not available in open-source Spark.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from retail_data_product.adapters import (
    AutoLoaderStreamSource,
    CSVDataSource,
    DeltaDataTarget,
    SparkDataFrameEngine,
    SparkDataFrameSource,
)
from retail_data_product.application.use_cases.bronze.ingest_orders import IngestOrders
from retail_data_product.application.use_cases.silver.clean_orders import CleanOrders
from retail_data_product.domain.retail.orders import ORDERS_RAW, ORDERS_TRANSFORMATION_DLQ
from retail_data_product.entrypoints.orders_pipeline import StdoutDataQualityLogger


def orders_spark_schema() -> StructType:
    """Spark schema for incoming order files, derived from the domain contract.

    Passed to Auto Loader as an explicit schema so the stream never has to infer
    from the data and starts cleanly even on an empty landing directory.
    """
    return SparkDataFrameEngine._schema_to_spark_schema(ORDERS_RAW.schema)


def make_batch_processor(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    customers_csv: str,
    dlq_table: str = None,
):
    """Build the foreachBatch callback that runs Bronze then Silver per batch.

    Args:
        spark: SparkSession
        bronze_table: Target for raw orders (bronze layer)
        silver_table: Target for cleaned orders (silver layer)
        customers_csv: Path to customers reference CSV
        dlq_table: Optional target for transformation failures in silver layer
    """

    def process_batch(micro_batch: SparkDataFrame, batch_id: int) -> None:
        # The same micro-batch feeds both layers; cache so it isn't recomputed.
        micro_batch.persist()
        try:
            print(f"=== Batch {batch_id}: {micro_batch.count()} rows ===")

            # Bronze: land the micro-batch as-is.
            IngestOrders(
                orders_source=SparkDataFrameSource(micro_batch),
                orders_target=DeltaDataTarget(bronze_table, spark_session=spark),
                dataframe_engine=SparkDataFrameEngine,
                data_quality_logger=StdoutDataQualityLogger(),
            ).execute()

            # Silver: dedup (keep latest order_date) + enrich, then MERGE upsert.
            dlq_target = None
            if dlq_table:
                dlq_target = DeltaDataTarget(dlq_table, spark_session=spark)

            CleanOrders(
                orders_raw_source=SparkDataFrameSource(micro_batch),
                orders_cleaned_target=DeltaDataTarget(silver_table, spark_session=spark),
                dataframe_engine=SparkDataFrameEngine,
                data_quality_logger=StdoutDataQualityLogger(),
                customers_source=CSVDataSource(customers_csv),
                dlq_target=dlq_target,
            ).execute()
        finally:
            micro_batch.unpersist()

    return process_batch


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Streaming orders pipeline")
    parser.add_argument("--landing-dir", required=True, help="Auto Loader source dir")
    parser.add_argument(
        "--orders-format", choices=("csv", "parquet"), default="csv",
        help="Format of the files Auto Loader ingests",
    )
    parser.add_argument("--customers-csv", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--schema-location", required=True, help="Auto Loader schema dir")
    parser.add_argument("--checkpoint-location", required=True)
    parser.add_argument(
        "--dlq-table",
        default=None,
        help="Optional Delta table for capturing transformation failures in silver layer",
    )
    parser.add_argument(
        "--processing-time",
        default="30 seconds",
        help="Micro-batch trigger interval (ignored when --available-now is set)",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Drain all pending files once and stop, instead of running continuously",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # On Databricks this returns the pre-provisioned cluster session.
    spark = SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

    bronze_table = f"{args.catalog}.{args.schema}.clickstream"
    silver_table = f"{args.catalog}.{args.schema}.orders_cleaned"
    dlq_table = args.dlq_table or f"{args.catalog}.{args.schema}.orders_transformation_dlq"

    # Read from clickstream Delta table at S3 location
    # Stream only new/changed records using Delta's built-in change tracking
    print(f"=== Starting continuous stream from {args.landing_dir} ===")

    streaming_df = spark.readStream \
        .format("delta") \
        .option("ignoreDeletes", "true") \
        .option("ignoreChanges", "false") \
        .load(args.landing_dir)

    trigger = (
        {"availableNow": True}
        if args.available_now
        else {"processingTime": args.processing_time}
    )

    query = streaming_df.writeStream \
        .foreachBatch(
            make_batch_processor(
                spark, bronze_table, silver_table, args.customers_csv, dlq_table
            )
        ) \
        .option("checkpointLocation", args.checkpoint_location) \
        .trigger(**trigger) \
        .queryName("orders_medallion_stream") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
