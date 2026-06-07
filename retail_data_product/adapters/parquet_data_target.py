import shutil
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .spark_dataframe_engine import SparkDataFrameEngine

from ..application.ports import DataTarget

from ..domain.data_governance import Dataframe


class ParquetDataTarget(DataTarget):
    """
    Parquet implementation of the DataTarget port.

    Persists data as plain Parquet files using Spark. Parquet has no native
    merge/upsert, so `upsert` is implemented as a read-modify-write:
      1. existing rows whose primary key also appears in the incoming batch are
         dropped (incoming wins),
      2. the remaining existing rows are unioned with the incoming batch,
      3. the dataset is rewritten via a staging directory that atomically
         replaces the target (you cannot overwrite a path Spark is still
         reading from in the same job).
    """

    def __init__(self, path: str, spark_session: Optional[SparkSession] = None):
        """
        Args:
            path: Path to the Parquet dataset directory (local or cloud storage)
            spark_session: Optional SparkSession. If None, gets or creates one.
        """
        self.path = Path(path).as_posix()  # Ensure forward slashes for cross-platform
        self.spark_session = spark_session or SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]

    def upsert(self, dataframe: "Dataframe", primary_key_column: str) -> None:
        """
        Upsert (merge) data into the Parquet dataset based on a primary key.

        Args:
            dataframe: The dataframe to upsert
            primary_key_column: The column used to match existing vs incoming rows
        """
        backend_engine = dataframe.backend_dataframe

        # Normalize the incoming data to a Spark DataFrame
        if isinstance(backend_engine, SparkDataFrameEngine):
            incoming = backend_engine.sdf
        else:
            data = backend_engine.collect()
            if not data:
                return
            spark_engine = SparkDataFrameEngine(self.spark_session)
            incoming = spark_engine.create_from_list_of_dict(data).sdf

        # Skip empty dataframes
        if len(incoming.columns) == 0 or incoming.count() == 0:
            return

        target_dir = Path(self.path)
        has_existing = target_dir.exists() and any(target_dir.glob("*.parquet"))

        if not has_existing:
            # First write — create the dataset
            incoming.write.format("parquet").mode("overwrite").save(self.path)
            return

        # Merge: keep existing rows not superseded by the incoming batch, then
        # append the incoming batch (incoming wins on primary key collisions).
        # The incoming batch defines the authoritative schema, so align the
        # existing rows to it — this drops columns the source no longer produces
        # (e.g. a column removed upstream) instead of resurrecting them as null.
        existing = self.spark_session.read.parquet(self.path)
        retained = existing.join(
            incoming.select(primary_key_column), on=primary_key_column, how="left_anti"
        )
        merged = retained.unionByName(incoming, allowMissingColumns=True).select(
            *incoming.columns
        )

        # Write to a staging dir first, then swap, so we never overwrite the
        # files `existing`/`merged` are still reading from.
        # add a coalesce to avoid too many small files, which would cause performance issues on read
        staging_path = f"{self.path}__staging"
        merged.coalesce(1).write.format("parquet").mode("overwrite").save(staging_path)
        shutil.rmtree(self.path, ignore_errors=True)
        shutil.move(staging_path, self.path)
