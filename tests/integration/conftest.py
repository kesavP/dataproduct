import pytest

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing with Delta Lake support."""
    builder = (
        SparkSession.builder.master("local[1]")  # pyright: ignore[reportAttributeAccessIssue]
        .appName("test_integration")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", 4)
        .config("spark.rdd.compress", "false")
        .config("spark.shuffle.compress", "false")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.executor.cores", 1)
        .config("spark.executor.instances", 1)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark

    spark.stop()
