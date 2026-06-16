from pathlib import Path
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

from .spark_dataframe_engine import SparkDataFrameEngine

from ..application.ports import DataTarget

from ..domain.data_governance import Dataframe


def _is_table_identifier(location: str) -> bool:
    """Return True if ``location`` is a catalog table name rather than a path.

    Storage paths contain a path separator (``/`` or ``\\``) or a URI scheme
    (``dbfs:``, ``abfss://``, ``s3://`` ...). Anything else — e.g.
    ``catalog.schema.table`` — is treated as a Unity Catalog managed table.
    """
    return not any(sep in location for sep in ("/", "\\", ":"))


class DeltaDataTarget(DataTarget):
    """
    Delta Lake implementation of DataTarget port.

    Uses Spark DataFrame engine internally for Delta Lake operations.
    """

    def __init__(self, path: str, spark_session=None):
        """
        Initialize Delta Lake data target.

        Args:
            path: Either a storage path (local, ``dbfs:``, ``abfss://``, ``s3://``)
                or a Unity Catalog table identifier (``catalog.schema.table``).
                Identifiers without a path separator or URI scheme are treated as
                UC managed tables and written with ``saveAsTable``.
            spark_session: Optional SparkSession. If None, creates a new one.
        """
        if not path or not str(path).strip():
            raise ValueError(f"path cannot be null or empty. Received: {repr(path)}")

        self.is_table = _is_table_identifier(path)
        # Storage paths: use Path for local paths, keep URIs verbatim (they already use forward slashes).
        # Table identifiers are kept verbatim.
        if self.is_table:
            self.path = path
        elif "://" in path:
            # Cloud URI (s3://, abfss://, dbfs://, etc.) - keep as-is
            self.path = path
        else:
            # Local filesystem path - normalize to forward slashes
            self.path = Path(path).as_posix()
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

            spark_engine = SparkDataFrameEngine(self.spark)
            spark_df = spark_engine.create_from_list_of_dict(data).sdf
        else:
            # Already a Spark engine
            spark_df = backend_engine.sdf

            # Skip empty dataframes
            if len(spark_df.columns) == 0 or spark_df.count() == 0:
                return

        # Resolve whether the target already exists, then either create it on the
        # first write or merge (upsert) into the existing Delta table.
        if self.is_table:
            exists = self.spark.catalog.tableExists(self.path)
        else:
            exists = (Path(self.path) / "_delta_log").exists()

        if not exists:
            # First write - create the table.
            writer = spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true")
            if self.is_table:
                writer.saveAsTable(self.path)
            else:
                writer.save(self.path)
            return

        # Table exists - check for schema compatibility before merge.
        try:
            if self.is_table:
                delta_table = DeltaTable.forName(self.spark, self.path)
            else:
                delta_table = DeltaTable.forPath(self.spark, self.path)

            merge_condition = f"target.{primary_key_column} = source.{primary_key_column}"

            delta_table.alias("target").merge(
                spark_df.alias("source"), merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        except Exception as e:
            # If schema mismatch, drop and recreate the table
            if "DELTA_METADATA_MISMATCH" in str(e) or "schema mismatch" in str(e).lower():
                print(f"Schema mismatch detected. Dropping and recreating table: {self.path}")
                if self.is_table:
                    self.spark.sql(f"DROP TABLE IF EXISTS {self.path}")
                else:
                    import shutil
                    shutil.rmtree(self.path, ignore_errors=True)

                # Recreate the table with the new schema
                writer = spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true")
                if self.is_table:
                    writer.saveAsTable(self.path)
                else:
                    writer.save(self.path)
            else:
                raise
