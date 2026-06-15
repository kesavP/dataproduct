from __future__ import annotations

from typing import Type

from pyspark.sql import DataFrame as SparkDataFrame

from .spark_dataframe_engine import SparkDataFrameEngine

from ..application.ports import DataSource, DataFrameEngine

from ..domain.data_governance import Dataframe


class SparkDataFrameSource(DataSource):
    """
    Adapter: expose an already-materialised Spark DataFrame as a DataSource.

    This is the bridge that lets streaming reuse the batch use cases. Inside a
    Structured Streaming ``foreachBatch`` callback each micro-batch is a *bounded*
    Spark DataFrame; wrapping it here lets it be fed to ``IngestOrders`` /
    ``CleanOrders`` exactly like any other source, so the data-quality checks and
    Delta MERGE run unchanged.
    """

    def __init__(self, spark_df: SparkDataFrame):
        """
        Args:
            spark_df: A bounded Spark DataFrame (e.g. a streaming micro-batch).
        """
        self._sdf = spark_df

    def fetch(self, dataframe_engine: Type[DataFrameEngine]) -> Dataframe:
        spark_engine = SparkDataFrameEngine(spark=self._sdf.sparkSession)
        spark_engine.sdf = self._sdf

        if dataframe_engine == SparkDataFrameEngine:
            return Dataframe(backend_dataframe=spark_engine)

        # Different engine requested: materialise and rebuild with that engine.
        data = spark_engine.collect()
        return Dataframe(
            backend_dataframe=dataframe_engine.create_from_list_of_dict(data)
        )
