from typing import Type, Optional
from pathlib import Path
from pyspark.sql import SparkSession

from .spark_dataframe_engine import SparkDataFrameEngine

from ..application.ports import DataSource, DataFrameEngine

from ..domain.data_governance import Dataframe


class ParquetDataSource(DataSource):
    """
    Parquet implementation of the DataSource port.

    Reads a Parquet dataset through Spark and returns it in the requested engine.
    """

    def __init__(self, path: str, spark_session: Optional[SparkSession] = None):
        """
        Args:
            path: Path to the Parquet dataset directory (local or cloud storage)
            spark_session: Optional SparkSession. If None, gets or creates one.
        """
        self.path = Path(path).as_posix()  # Ensure forward slashes for cross-platform
        self.spark_session = spark_session or SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]

    def fetch(self, dataframe_engine: Type[DataFrameEngine]) -> "Dataframe":
        """
        Read data from the Parquet dataset.

        Args:
            dataframe_engine: The engine type to use for the dataframe

        Returns:
            Dataframe domain object with the data
        """
        spark_df = self.spark_session.read.parquet(self.path)
        spark_engine = SparkDataFrameEngine(spark=self.spark_session)
        spark_engine.sdf = spark_df

        # If the caller wants a different engine, convert
        if dataframe_engine == SparkDataFrameEngine:
            backend_df = spark_engine
        else:
            data = spark_engine.collect()
            backend_df = dataframe_engine.create_from_list_of_dict(data)

        return Dataframe(backend_dataframe=backend_df)
