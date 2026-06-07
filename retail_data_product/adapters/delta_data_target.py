from pathlib import Path
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

from .spark_dataframe_engine import SparkDataFrameEngine

from ..application.ports import DataTarget

from ..domain.data_governance import Dataframe


class DeltaDataTarget(DataTarget):
    """
    Delta Lake implementation of DataTarget port.

    Uses Spark DataFrame engine internally for Delta Lake operations.
    """

    def __init__(self, path: str, spark_session=None):
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

    def upsert(self, dataframe: "Dataframe", primary_key_column: str) -> None:
        """
        Upsert (merge) data into Delta table.

        Updates existing records and inserts new ones based on primary key.

        Args:
            dataframe: The dataframe to upsert
            primary_key_column: The column to use as primary key for merging
        """

        backend_engine = dataframe.backend_dataframe

        # Convert to Spark DataFrame if needed
        if not isinstance(backend_engine, SparkDataFrameEngine):
            # Not a Spark engine, need to convert
            data = backend_engine.collect()

            # Skip empty dataframes
            if not data:
                return

            spark_engine = SparkDataFrameEngine(self.spark_session)
            spark_df = spark_engine.create_from_list_of_dict(data).sdf
        else:
            # Already a Spark engine
            spark_df = backend_engine.sdf

            # Skip empty dataframes
            if len(spark_df.columns) == 0 or spark_df.count() == 0:
                return

        # Check if table exists
        delta_log = Path(self.path) / "_delta_log"

        if not delta_log.exists():
            # First write - create table
            spark_df.write.format("delta").mode("overwrite").save(self.path)
        else:
            # Table exists - perform merge (upsert)
            delta_table = DeltaTable.forPath(self.spark_session, self.path)  # pyright: ignore[reportArgumentType]

            # Create merge condition
            merge_condition = (
                f"target.{primary_key_column} = source.{primary_key_column}"
            )

            delta_table.alias("target").merge(
                spark_df.alias("source"), merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
