from typing import Type, Optional
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from .spark_dataframe_engine import SparkDataFrameEngine

from ..application.ports import DataSource, DataFrameEngine

from ..domain.data_governance import Dataframe


class DeltaDataSource(DataSource):
    """
    Delta Lake implementation of DataSource port.

    Uses Spark DataFrame engine internally for Delta Lake operations.
    """

    def __init__(self, path: str, spark_session: Optional[SparkSession] = None):
        """
        Initialize Delta Lake data source/target.

        Args:
            path: Path to Delta Lake table (can be local or cloud storage)
            spark_session: Optional SparkSession. If None, creates a new one.
        """
        self.path = Path(path).as_posix()  # Ensure forward slashes for cross-platform
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
        # Read from Delta table
        spark_df = self.spark_session.read.format("delta").load(self.path)  # pyright: ignore[reportOptionalMemberAccess]
        spark_engine = SparkDataFrameEngine(spark=self.spark_session)
        spark_engine.sdf = spark_df

        # If user wants a different engine, convert
        if dataframe_engine == SparkDataFrameEngine:
            backend_df = spark_engine
        else:
            # Convert to list and create with target engine
            data = spark_engine.collect()
            backend_df = dataframe_engine.create_from_list_of_dict(data)

        return Dataframe(backend_dataframe=backend_df)
