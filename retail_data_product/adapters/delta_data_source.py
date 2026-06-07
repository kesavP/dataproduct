from typing import Type, Optional
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from .spark_dataframe_engine import SparkDataFrameEngine

from ..application.ports import DataSource, DataFrameEngine

from ..domain.data_governance import Dataframe

from .delta_data_target import _is_table_identifier


class DeltaDataSource(DataSource):
    """
    Delta Lake implementation of DataSource port.

    Uses Spark DataFrame engine internally for Delta Lake operations.
    """

    def __init__(self, path: str, spark_session: Optional[SparkSession] = None):
        """
        Initialize Delta Lake data source.

        Args:
            path: Either a storage path (local, ``dbfs:``, ``abfss://``, ``s3://``)
                or a Unity Catalog table identifier (``catalog.schema.table``).
                Identifiers without a path separator or URI scheme are read as
                UC managed tables via ``spark.read.table``.
            spark_session: Optional SparkSession. If None, creates a new one.
        """
        self.is_table = _is_table_identifier(path)
        self.path = path if self.is_table else Path(path).as_posix()
        self.spark_session = spark_session

        if spark_session is None:
            builder = (
                SparkSession.builder.master("local[1]")  # pyright: ignore[reportAttributeAccessIssue]
                .appName("delta_adapter")
                .config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
            )
            spark_session = configure_spark_with_delta_pip(builder).getOrCreate()

        self.spark = spark_session

    def fetch(self, dataframe_engine: Type[DataFrameEngine]) -> "Dataframe":
        """
        Read data from Delta table.

        Args:
            dataframe_engine: The engine type to use for the dataframe

        Returns:
            Dataframe domain object with the data
        """
        # Read from a UC managed table by name, or from a Delta path.
        if self.is_table:
            spark_df = self.spark.read.table(self.path)
        else:
            spark_df = self.spark.read.format("delta").load(self.path)
        spark_engine = SparkDataFrameEngine(spark=self.spark)
        spark_engine.sdf = spark_df

        # If user wants a different engine, convert
        if dataframe_engine == SparkDataFrameEngine:
            backend_df = spark_engine
        else:
            # Convert to list and create with target engine
            data = spark_engine.collect()
            backend_df = dataframe_engine.create_from_list_of_dict(data)

        return Dataframe(backend_dataframe=backend_df)
