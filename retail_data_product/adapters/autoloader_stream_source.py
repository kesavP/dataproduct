from __future__ import annotations

from typing import Callable, Optional

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery


# A foreachBatch callback: receives the bounded micro-batch DataFrame and its id.
BatchProcessor = Callable[[SparkDataFrame, int], None]


class AutoLoaderStreamSource:
    """
    Adapter: incremental file ingestion via Databricks Auto Loader (cloudFiles).

    Auto Loader watches a cloud directory and incrementally picks up new files as
    they land, tracking which files were already processed. This adapter exposes
    that stream and wires it to a ``foreachBatch`` sink: each micro-batch arrives
    as a *bounded* Spark DataFrame, so the existing batch use cases (DQ checks +
    Delta MERGE upsert) can run against it without modification.

    NOTE: ``cloudFiles`` is a Databricks Runtime feature and is NOT available in
    open-source Spark. This adapter is meant to run on a Databricks cluster; for a
    purely local stream you would swap the ``format("cloudFiles")`` reader for a
    plain ``format("<file_format>")`` readStream over a directory.
    """

    def __init__(
        self,
        path: str,
        schema_location: str,
        file_format: str = "csv",
        spark_session: Optional[SparkSession] = None,
        reader_options: Optional[dict] = None,
    ):
        """
        Args:
            path: Source directory Auto Loader watches (e.g. a UC Volume path).
            schema_location: Location where Auto Loader persists the inferred
                schema and tracks schema evolution (a durable path/checkpoint).
            file_format: Underlying file format ("csv", "json", "parquet", ...).
            spark_session: SparkSession; on Databricks pass the cluster session.
                Falls back to ``getOrCreate`` (returns the cluster session there).
            reader_options: Extra reader options merged into the readStream, e.g.
                ``{"header": "true", "cloudFiles.inferColumnTypes": "true"}``.
        """
        self.path = path
        self.schema_location = schema_location
        self.file_format = file_format
        self.spark = spark_session or SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
        self.reader_options = reader_options or {}

    def read_stream(self) -> SparkDataFrame:
        """Build the Auto Loader streaming DataFrame (unbounded)."""
        reader = (
            self.spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", self.file_format)
            .option("cloudFiles.schemaLocation", self.schema_location)
        )
        for key, value in self.reader_options.items():
            reader = reader.option(key, value)
        return reader.load(self.path)

    def run(
        self,
        process_batch: BatchProcessor,
        checkpoint_location: str,
        trigger: Optional[dict] = None,
        query_name: Optional[str] = None,
    ) -> StreamingQuery:
        """
        Start the streaming query, routing each micro-batch to ``process_batch``.

        Args:
            process_batch: Callback ``(micro_batch_df, batch_id) -> None`` that
                applies the pipeline to one bounded micro-batch.
            checkpoint_location: Durable checkpoint for exactly-once progress.
            trigger: Kwargs for ``DataStreamWriter.trigger`` — e.g.
                ``{"processingTime": "30 seconds"}`` for continuous micro-batches
                or ``{"availableNow": True}`` to drain pending files and stop.
                Defaults to a 30-second processing-time trigger.
            query_name: Optional name for the streaming query.

        Returns:
            The started ``StreamingQuery``.
        """
        writer = (
            self.read_stream()
            .writeStream.option("checkpointLocation", checkpoint_location)
            .foreachBatch(process_batch)
        )

        writer = writer.trigger(**(trigger or {"processingTime": "30 seconds"}))

        if query_name is not None:
            writer = writer.queryName(query_name)

        return writer.start()
