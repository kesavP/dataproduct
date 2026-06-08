"""Real-time orders pipeline — Structured Streaming + Auto Loader (Option A).

Streaming sibling of ``run_orders_ingestion.py``. Instead of a one-shot batch
run, Auto Loader incrementally ingests order files as they land in a cloud
directory, and a ``foreachBatch`` callback runs the *existing* batch use cases
(``IngestOrders`` then ``CleanOrders``) against each bounded micro-batch — so the
data-quality checks and Delta MERGE upserts are reused verbatim.

    file lands -> Auto Loader micro-batch -> foreachBatch:
        Bronze  micro-batch              -> <catalog>.<schema>.orders_raw
        Silver  micro-batch (dedup+enrich) -> <catalog>.<schema>.orders_cleaned

NOTE: Auto Loader (``cloudFiles``) is a Databricks Runtime feature — run this on
a Databricks cluster (e.g. a continuous Job task), not locally.
"""

from pyspark.sql import SparkSession

from retail_data_product.adapters import AutoLoaderStreamSource
from retail_data_product.entrypoints.orders_streaming import make_batch_processor

# -----------------------------------------------------------------------------
# Configuration — point these at your Unity Catalog volume / catalog / schema.
# -----------------------------------------------------------------------------
ORDERS_LANDING_DIR = "/Volumes/main/retail/landing/orders_stream"
CUSTOMERS_CSV_PATH = "/Volumes/main/retail/landing/customers.csv"
SCHEMA_LOCATION = "/Volumes/main/retail/landing/_schema/orders"
CHECKPOINT_LOCATION = "/Volumes/main/retail/landing/_checkpoints/orders"

CATALOG = "main"
SCHEMA = "retail"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.orders_raw"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.orders_cleaned"


def build_spark_session() -> SparkSession:
    """Return the active session (the cluster session on Databricks)."""
    return SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]


def main() -> None:
    spark = build_spark_session()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

    source = AutoLoaderStreamSource(
        path=ORDERS_LANDING_DIR,
        schema_location=SCHEMA_LOCATION,
        file_format="csv",
        spark_session=spark,
        reader_options={"header": "true", "cloudFiles.inferColumnTypes": "true"},
    )

    query = source.run(
        # Canonical foreachBatch logic lives in the packaged entry point.
        process_batch=make_batch_processor(
            spark, BRONZE_TABLE, SILVER_TABLE, CUSTOMERS_CSV_PATH
        ),
        checkpoint_location=CHECKPOINT_LOCATION,
        trigger={"processingTime": "30 seconds"},
        query_name="orders_medallion_stream",
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
