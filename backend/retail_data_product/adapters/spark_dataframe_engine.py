from __future__ import annotations
from typing import Any, Literal, Optional
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField as SparkStructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
)

from ..application.ports import DataFrameEngine

from ..domain.data_governance import Schema, DataType


class SparkDataFrameEngine(DataFrameEngine):
    """
    Spark implementation of the DataFrameEngine port.

    This adapter uses PySpark to execute data operations in a distributed manner.
    """

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize the Spark engine.

        Args:
            spark: Optional SparkSession. If not provided, gets or creates one.
        """
        self.spark = spark or SparkSession.builder.getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
        self.sdf: SparkDataFrame = self.spark.createDataFrame([], StructType([]))
        self.schema = Schema(fields=())

    @staticmethod
    def create_from_list_of_dict(
        data: list[dict], schema: Optional[Schema] = None
    ) -> SparkDataFrameEngine:
        ret = SparkDataFrameEngine()

        if not data:
            # Empty dataframe
            if schema:
                spark_schema = SparkDataFrameEngine._schema_to_spark_schema(schema)
                ret.sdf = ret.spark.createDataFrame([], spark_schema)
            return ret

        # Create dataframe from data
        ret.sdf = ret.spark.createDataFrame(data)  # pyright: ignore[reportArgumentType, reportCallIssue]

        # If schema is provided, enforce it
        if schema:
            ret.enforce_schema(schema)

        return ret

    def collect(self) -> list[dict]:
        rows = self.sdf.collect()
        return [row.asDict() for row in rows]

    def colums_contains(self, column_name: str, value: Any) -> bool:
        if column_name not in self.sdf.columns:
            return False

        if value is None:
            count = self.sdf.filter(F.col(column_name).isNull()).count()
        else:
            count = self.sdf.filter(F.col(column_name) == value).count()

        return count > 0

    def get_column_names(self) -> list[str]:
        return self.sdf.columns

    def concat(self, other: DataFrameEngine):
        if not isinstance(other, SparkDataFrameEngine):
            other = SparkDataFrameEngine.create_from_list_of_dict(other.collect())
        self.sdf = self.sdf.union(other.sdf)

    def deduplicate_on_column(self, column: str):
        if column in self.sdf.columns:
            # Add row number to identify last occurrence
            from pyspark.sql.window import Window

            window = Window.partitionBy(column).orderBy(
                F.monotonically_increasing_id().desc()
            )
            self.sdf = self.sdf.withColumn("_row_num", F.row_number().over(window))
            self.sdf = self.sdf.filter(F.col("_row_num") == 1).drop("_row_num")

    def deduplicate_keep_latest(self, key_column: str, order_column: str) -> None:
        if key_column not in self.sdf.columns or order_column not in self.sdf.columns:
            return

        from pyspark.sql.window import Window

        # Rank rows within each key by the order column (latest first) and keep
        # only the top one, dropping earlier duplicates.
        window = Window.partitionBy(key_column).orderBy(F.col(order_column).desc())
        self.sdf = self.sdf.withColumn("_row_num", F.row_number().over(window))
        self.sdf = self.sdf.filter(F.col("_row_num") == 1).drop("_row_num")

    def clean_string_column(self, column: str) -> None:
        if column in self.sdf.columns:
            self.sdf = self.sdf.withColumn(column, F.trim(F.lower(F.col(column))))

    def filter_rows_by_value_in_set(self, column: str, valid_values: set):
        if column in self.sdf.columns:
            self.sdf = self.sdf.filter(F.col(column).isin(list(valid_values)))

    def join(
        self, other: DataFrameEngine, left_on: str, right_on: str, how: str = "left"
    ) -> None:
        if not isinstance(other, SparkDataFrameEngine):
            other = SparkDataFrameEngine.create_from_list_of_dict(other.collect())

        # If join columns have same name, avoid duplicate columns
        if left_on == right_on:
            self.sdf = self.sdf.join(other.sdf, on=left_on, how=how)
        else:
            self.sdf = self.sdf.join(
                other.sdf, F.col(left_on) == F.col(right_on), how=how
            )

    def add_column_from_expression(self, column_name: str, expression) -> None:
        # Expression should be a callable that takes the dataframe
        self.sdf = expression(self.sdf)

    def group_by_and_aggregate(self, group_columns: list[str], agg_dict: dict) -> None:
        # Convert pandas-style agg_dict to Spark aggregations
        spark_aggs = []
        for col, func in agg_dict.items():
            if func == "sum":
                spark_aggs.append(F.sum(col).alias(col))
            elif func == "mean":
                spark_aggs.append(F.mean(col).alias(col))
            elif func == "count":
                spark_aggs.append(F.count(col).alias(col))
            elif func == "min":
                spark_aggs.append(F.min(col).alias(col))
            elif func == "max":
                spark_aggs.append(F.max(col).alias(col))
            else:
                spark_aggs.append(F.first(col).alias(col))

        self.sdf = self.sdf.groupBy(*group_columns).agg(*spark_aggs)

    def filter_rows_by_condition(
        self,
        column: str,
        operator: Literal[">", "<", ">=", "<=", "==", "!="],
        value: Any,
    ) -> None:
        if column in self.sdf.columns:
            col_ref = F.col(column)
            if operator == ">":
                self.sdf = self.sdf.filter(col_ref > value)
            elif operator == "<":
                self.sdf = self.sdf.filter(col_ref < value)
            elif operator == ">=":
                self.sdf = self.sdf.filter(col_ref >= value)
            elif operator == "<=":
                self.sdf = self.sdf.filter(col_ref <= value)
            elif operator == "==":
                self.sdf = self.sdf.filter(col_ref == value)
            elif operator == "!=":
                self.sdf = self.sdf.filter(col_ref != value)

    def rename_column(self, old_name: str, new_name: str) -> None:
        if old_name in self.sdf.columns:
            self.sdf = self.sdf.withColumnRenamed(old_name, new_name)

    def drop_column(self, column: str) -> None:
        # Spark's drop is a no-op when the column is absent.
        self.sdf = self.sdf.drop(column)

    def fill_missing_dates(self, date_column: str, fill_value: dict) -> None:
        if date_column not in self.sdf.columns:
            return

        # Get min and max dates
        date_range = self.sdf.agg(
            F.min(date_column).alias("min_date"), F.max(date_column).alias("max_date")
        ).collect()[0]

        min_date = date_range["min_date"]
        max_date = date_range["max_date"]

        if min_date is None or max_date is None:
            return

        # Create full date range
        from datetime import datetime, timedelta

        if isinstance(min_date, str):
            min_date = datetime.strptime(min_date, "%Y-%m-%d")
            max_date = datetime.strptime(max_date, "%Y-%m-%d")

        date_list = []
        current_date = min_date
        while current_date <= max_date:
            date_list.append((current_date,))
            current_date += timedelta(days=1)

        # Create dataframe with full date range
        full_dates_df = self.spark.createDataFrame(date_list, [date_column])

        # Left join to get missing dates
        self.sdf = full_dates_df.join(self.sdf, on=date_column, how="left")

        # Fill null values
        for col, val in fill_value.items():
            if col in self.sdf.columns:
                self.sdf = self.sdf.fillna({col: val})

    def count_rows_where(
        self,
        column: str,
        operator: Literal["in", "not_in", ">", "<", ">=", "<=", "==", "!="],
        value: Any,
    ) -> int:
        if column not in self.sdf.columns:
            return 0

        col_ref = F.col(column)

        if operator == "in":
            filtered_df = self.sdf.filter(col_ref.isin(list(value)))
        elif operator == "not_in":
            filtered_df = self.sdf.filter(~col_ref.isin(list(value)))
        elif operator == ">":
            filtered_df = self.sdf.filter(col_ref > value)
        elif operator == "<":
            filtered_df = self.sdf.filter(col_ref < value)
        elif operator == ">=":
            filtered_df = self.sdf.filter(col_ref >= value)
        elif operator == "<=":
            filtered_df = self.sdf.filter(col_ref <= value)
        elif operator == "==":
            filtered_df = self.sdf.filter(col_ref == value)
        elif operator == "!=":
            filtered_df = self.sdf.filter(col_ref != value)
        else:
            raise ValueError(f"Unsupported operator: {operator}")

        return filtered_df.count()

    def count_rows(self) -> int:
        return self.sdf.count()

    def enforce_schema(self, target_schema: Schema) -> None:
        # Build list of columns with correct types
        select_exprs = []

        for field in target_schema.fields:
            if field.name in self.sdf.columns:
                # Column exists, cast to target type
                spark_type = SparkDataFrameEngine._datatype_to_spark_type(
                    field.data_type
                )
                select_exprs.append(
                    F.col(field.name).cast(spark_type).alias(field.name)
                )
            else:
                # Column missing, add as null
                spark_type = SparkDataFrameEngine._datatype_to_spark_type(
                    field.data_type
                )
                select_exprs.append(F.lit(None).cast(spark_type).alias(field.name))

        # Apply transformations
        self.sdf = self.sdf.select(*select_exprs)
        self.schema = target_schema

    @staticmethod
    def _datatype_to_spark_type(data_type: DataType):
        """
        Convert our DataType enum to Spark type.
        """
        type_mapping = {
            DataType.STRING: StringType(),
            DataType.INTEGER: IntegerType(),
            DataType.DOUBLE: DoubleType(),
            DataType.DATE: DateType(),
            DataType.DATETIME: TimestampType(),
        }
        return type_mapping.get(data_type, StringType())

    @staticmethod
    def _schema_to_spark_schema(schema: Schema) -> StructType:
        """
        Convert our Schema to Spark StructType.
        """
        spark_fields = []
        for field in schema.fields:
            spark_type = SparkDataFrameEngine._datatype_to_spark_type(field.data_type)
            spark_fields.append(SparkStructField(field.name, spark_type, nullable=True))
        return StructType(spark_fields)

    def get_schema(self) -> Schema:
        return self.schema
