from typing import Type, Optional
from pathlib import Path
from pyspark.sql import SparkSession

from .spark_dataframe_engine import SparkDataFrameEngine

from ..application.ports import DataSource, DataFrameEngine

from ..domain.data_governance import Dataframe


def _normalize_location(path: str) -> str:
    """Normalise a local path without mangling cloud URIs.

    ``Path(...).as_posix()`` collapses the ``//`` in a URI (e.g.
    ``abfss://container@acct.dfs.core.windows.net/x`` -> ``abfss:/container...``),
    which Spark cannot read. URIs (anything with a ``scheme://``) are passed
    through untouched; only local filesystem paths are normalised to forward
    slashes for cross-platform use.
    """
    if "://" in path:
        return path
    return Path(path).as_posix()


class ParquetDataSource(DataSource):
    """
    Parquet implementation of the DataSource port.

    Reads a Parquet dataset through Spark and returns it in the requested engine.
    """

    def __init__(self, path: str, spark_session: Optional[SparkSession] = None):
        """
        Args:
            path: Path to the Parquet dataset directory. Local paths and cloud
                URIs (``abfss://``, ``wasbs://``, ``s3://`` ...) are both supported.
            spark_session: Optional SparkSession. If None, gets or creates one.
        """
        self.path = _normalize_location(path)
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
