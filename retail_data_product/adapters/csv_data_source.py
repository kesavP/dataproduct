from __future__ import annotations
from pathlib import Path
from typing import Type
import pandas as pd
from pyspark.sql import SparkSession

from .pandas_dataframe_engine import PandasDataFrameEngine
from .spark_dataframe_engine import SparkDataFrameEngine

from ..application.ports import DataSource, DataFrameEngine

from ..domain.data_governance import Dataframe


class CSVDataSource(DataSource):
    """
    Adapter: Read data from CSV files

    Supports:
    - Single CSV file
    - Directory of CSV files (reads all .csv files)
    - Custom delimiter, header options

    Example:
        source = CSVDataSource("data/orders.csv")
        df = source.fetch(PandasDataFrameEngine)
    """

    def __init__(
        self,
        path: str,
        delimiter: str = ",",
        header: bool = True,
        encoding: str = "utf-8",
    ):
        """
        Args:
            path: Path to CSV file or directory
            delimiter: Column delimiter (default: comma)
            header: Whether first row is header (default: True)
            encoding: File encoding (default: utf-8)
        """
        self.path = path
        self.delimiter = delimiter
        self.header = header
        self.encoding = encoding

    def fetch(self, dataframe_engine: Type[DataFrameEngine]) -> Dataframe:
        """
        Read CSV file(s) into dataframe

        Args:
            dataframe_engine: Engine to use (Pandas, Spark, etc.)

        Returns:
            Dataframe containing CSV data

        Raises:
            FileNotFoundError: If path doesn't exist
            ValueError: If no CSV files found in directory
        """
        path = Path(self.path)

        if not path.exists():
            raise FileNotFoundError(f"CSV path not found: {self.path}")

        # Determine if Spark or Pandas engine
        engine_name = dataframe_engine.__name__

        if "Spark" in engine_name:
            return self._read_with_spark()
        else:
            return self._read_with_pandas()

    def _read_with_pandas(self) -> Dataframe:
        """
        Read CSV with Pandas
        """

        path = Path(self.path)

        if path.is_file():
            # Single file
            df = pd.read_csv(
                path,
                sep=self.delimiter,
                header=0 if self.header else None,
                encoding=self.encoding,
            )
        else:
            # Directory - read all CSV files
            csv_files = list(path.glob("*.csv"))
            if not csv_files:
                raise ValueError(f"No CSV files found in directory: {self.path}")

            dfs = [
                pd.read_csv(
                    f,
                    sep=self.delimiter,
                    header=0 if self.header else None,
                    encoding=self.encoding,
                )
                for f in csv_files
            ]
            df = pd.concat(dfs, ignore_index=True)

        # Create engine with the pandas dataframe
        pandas_engine = PandasDataFrameEngine()
        pandas_engine.pdf = df

        return Dataframe(backend_dataframe=pandas_engine)

    def _read_with_spark(self) -> Dataframe:
        """
        Read CSV with Spark
        """

        # Get or create Spark session
        spark = SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]

        # Read CSV
        df = spark.read.csv(
            self.path,
            sep=self.delimiter,
            header=self.header,
            encoding=self.encoding,
            inferSchema=True,  # Infer data types
        )

        # Create engine with the spark dataframe
        spark_engine = SparkDataFrameEngine(spark=spark)
        spark_engine.sdf = df

        return Dataframe(backend_dataframe=spark_engine)
