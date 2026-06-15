from __future__ import annotations
import pandas as pd
from typing import Any, Literal, Optional

from ..application.ports import DataFrameEngine

from ..domain.data_governance import Schema, DataType


class PandasDataFrameEngine(DataFrameEngine):
    """
    Pandas implementation of the DataFrameEngine port.

    This adapter uses Pandas to execute data operations within memory.
    """

    def __init__(self):
        self.pdf = pd.DataFrame()
        self.schema = Schema(fields=tuple())

    @staticmethod
    def create_from_list_of_dict(
        data: list[dict], schema: Optional[Schema] = None
    ) -> PandasDataFrameEngine:
        ret = PandasDataFrameEngine()
        ret.pdf = pd.DataFrame(data)

        # If schema is provided, enforce it
        if schema:
            ret.enforce_schema(schema)

        return ret

    def collect(self) -> list[dict]:
        # Replace NaN with None for proper null handling
        return self.pdf.where(pd.notna(self.pdf), None).to_dict(orient="records")  # pyright: ignore[reportArgumentType]

    def colums_contains(self, column_name: str, value: Any) -> bool:
        if column_name in self.pdf.columns:
            if value is None:
                return bool(self.pdf[column_name].isnull().any())
            return bool(self.pdf[column_name].eq(value).any())
        return False

    def get_column_names(self) -> list[str]:
        return self.pdf.columns.tolist()

    def concat(self, other: DataFrameEngine):
        if not isinstance(other, PandasDataFrameEngine):
            other = PandasDataFrameEngine.create_from_list_of_dict(other.collect())
        self.pdf = pd.concat([self.pdf, other.pdf], ignore_index=True)

    def deduplicate_on_column(self, column: str):
        self.pdf = self.pdf.drop_duplicates(subset=column, keep="last").reset_index(
            drop=True
        )

    def deduplicate_keep_latest(self, key_column: str, order_column: str) -> None:
        if key_column in self.pdf.columns and order_column in self.pdf.columns:
            # Sort ascending by the order column so the latest lands last, then
            # keep the last row per key (drops earlier duplicates).
            self.pdf = (
                self.pdf.sort_values(order_column)
                .drop_duplicates(subset=key_column, keep="last")
                .reset_index(drop=True)
            )

    def clean_string_column(self, column: str) -> None:
        if column in self.pdf.columns:
            self.pdf[column] = self.pdf[column].astype(str).str.lower().str.strip()

    def filter_rows_by_value_in_set(self, column: str, valid_values: set) -> None:
        if column in self.pdf.columns:
            self.pdf = self.pdf[self.pdf[column].isin(valid_values)].reset_index(
                drop=True
            )

    def join(
        self, other: DataFrameEngine, left_on: str, right_on: str, how: str = "left"
    ) -> None:
        if not isinstance(other, PandasDataFrameEngine):
            other = PandasDataFrameEngine.create_from_list_of_dict(other.collect())
        self.pdf = self.pdf.merge(
            other.pdf,
            left_on=left_on,
            right_on=right_on,
            how=how,  # pyright: ignore[reportArgumentType]
        )

    def add_column_from_expression(self, column_name: str, expression) -> None:
        self.pdf[column_name] = expression(self.pdf)

    def group_by_and_aggregate(self, group_columns: list[str], agg_dict: dict) -> None:
        self.pdf = self.pdf.groupby(group_columns, as_index=False).agg(agg_dict)

    def filter_rows_by_condition(
        self,
        column: str,
        operator: Literal[">", "<", ">=", "<=", "==", "!="],
        value: Any,
    ) -> None:
        if column in self.pdf.columns:
            if operator == ">":
                self.pdf = self.pdf[self.pdf[column] > value].reset_index(drop=True)
            elif operator == "<":
                self.pdf = self.pdf[self.pdf[column] < value].reset_index(drop=True)
            elif operator == ">=":
                self.pdf = self.pdf[self.pdf[column] >= value].reset_index(drop=True)
            elif operator == "<=":
                self.pdf = self.pdf[self.pdf[column] <= value].reset_index(drop=True)
            elif operator == "==":
                self.pdf = self.pdf[self.pdf[column] == value].reset_index(drop=True)
            elif operator == "!=":
                self.pdf = self.pdf[self.pdf[column] != value].reset_index(drop=True)

    def rename_column(self, old_name: str, new_name: str) -> None:
        if old_name in self.pdf.columns:
            self.pdf = self.pdf.rename(columns={old_name: new_name})

    def drop_column(self, column: str) -> None:
        if column in self.pdf.columns:
            self.pdf = self.pdf.drop(columns=[column])

    def fill_missing_dates(self, date_column: str, fill_value: dict) -> None:
        if date_column in self.pdf.columns:
            self.pdf[date_column] = pd.to_datetime(self.pdf[date_column])
            date_range = pd.date_range(
                start=self.pdf[date_column].min(),
                end=self.pdf[date_column].max(),
                freq="D",
            )
            full_df = pd.DataFrame({date_column: date_range})
            self.pdf = full_df.merge(self.pdf, on=date_column, how="left")
            for col, val in fill_value.items():
                if col in self.pdf.columns:
                    self.pdf[col] = self.pdf[col].fillna(val)

    def count_rows_where(
        self,
        column: str,
        operator: Literal["in", "not_in", ">", "<", ">=", "<=", "==", "!="],
        value: Any,
    ) -> int:
        if column not in self.pdf.columns:
            return 0

        if operator == "in":
            return int(self.pdf[column].isin(value).sum())
        elif operator == "not_in":
            return int((~self.pdf[column].isin(value)).sum())
        elif operator == ">":
            return int((self.pdf[column] > value).sum())
        elif operator == "<":
            return int((self.pdf[column] < value).sum())
        elif operator == ">=":
            return int((self.pdf[column] >= value).sum())
        elif operator == "<=":
            return int((self.pdf[column] <= value).sum())
        elif operator == "==":
            return int((self.pdf[column] == value).sum())
        elif operator == "!=":
            return int((self.pdf[column] != value).sum())
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def count_rows(self) -> int:
        return len(self.pdf)

    def enforce_schema(self, target_schema: Schema) -> None:
        # Create a new dataframe with the target schema
        new_columns = {}

        for field in target_schema.fields:
            if field.name in self.pdf.columns:
                # Column exists, convert type
                col_data = self.pdf[field.name]

                try:
                    if field.data_type in (DataType.INTEGER, DataType.DOUBLE):
                        # Numeric conversion with coercion
                        col_data = pd.to_numeric(col_data, errors="coerce")
                        if field.data_type == DataType.INTEGER:
                            col_data = col_data.astype("Int64")
                    elif field.data_type in (DataType.DATETIME, DataType.DATE):
                        # DateTime conversion with coercion
                        col_data = pd.to_datetime(col_data, errors="coerce")
                    elif field.data_type == DataType.STRING:
                        # String conversion
                        col_data = col_data.astype("string")
                    new_columns[field.name] = col_data
                except Exception:
                    # On conversion error, create column with nulls
                    new_columns[field.name] = pd.Series(
                        [None] * len(self.pdf), dtype="object"
                    )
            else:
                new_columns[field.name] = pd.Series(
                    [None] * len(self.pdf), dtype="object"
                )

        # Create new dataframe with enforced schema
        # Preserve original length (don't drop rows on conversion errors)
        if len(self.pdf) > 0:
            self.pdf = pd.DataFrame(new_columns, index=self.pdf.index)
        else:
            # Empty dataframe
            self.pdf = pd.DataFrame(new_columns)
        self.schema = target_schema

    def get_schema(self) -> Schema:
        return self.schema
