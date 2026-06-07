from __future__ import annotations

from typing import Any, Literal, Optional, Protocol

from ...domain.data_governance import Schema


class DataFrameEngine(Protocol):
    """
    Interface for the DataFrame engine.

    This protocol defines the operations that any DataFrame engine implementation
    (e.g., Pandas, Spark) must provide to be compatible with the system.

    The main use case is to abstract data processing operations in a backend-agnostic
    manner, allowing the domain and application layers to remain decoupled from
    specific libraries. This allows for application or domain code to be tested in memory
    uniformally using the same methods.
    """

    @staticmethod
    def create_from_list_of_dict(
        data: list[dict],
        schema: Optional[Schema] = None,
    ) -> DataFrameEngine:
        """
        Creates a new DataFrameEngine instance from a list of dictionaries that holds the data.

        Args:
            data (list[dict]): The input data, where each dict represents a row.
            schema (Optional[Schema]): optional schema, if not provided, will be infered from the provided data

        Returns:
            DataFrameEngine: A new DataFrameEngine instance containing the data.
        """
        ...

    def collect(self) -> list[dict]:
        """
        Collects the data as a list of dictionaries for in memory operations. Be careful to not
        use it in a big data to avoid Out-Of-Memory errors.

        Returns:
            list[dict]: The current DataFrame contents as a list of records.
        """
        ...

    def colums_contains(self, column_name: str, value: Any) -> bool:
        """
        Checks if a column contains a specific value.

        Args:
            column_name (str): The column to search.
            value (Any): The value to check for.

        Returns:
            bool: True if the value is found in the column, False otherwise.
        """
        ...

    def get_column_names(self) -> list[str]:
        """
        Retrieves all column names in the DataFrame.

        Returns:
            list[str]: A list of column names.
        """
        ...

    def concat(self, other: DataFrameEngine) -> None:
        """
        Concatenates another DataFrameEngine to this one, in-place.

        Args:
            other (DataFrameEngine): The other DataFrameEngine to concatenate.

        Raises:
            TypeError: If the other engine is not of the same type.
        """
        ...

    def deduplicate_on_column(self, column: str) -> None:
        """
        Removes duplicate rows based on a column, keeping the last occurrence.

        Args:
            column (str): The column name to use for deduplication.
        """
        ...

    def deduplicate_keep_latest(self, key_column: str, order_column: str) -> None:
        """
        Removes duplicate rows on a key, keeping the row whose ``order_column``
        value is the most recent. Earlier duplicates are dropped.

        Args:
            key_column (str): Column identifying duplicates (e.g. "order_id").
            order_column (str): Column whose maximum value wins (e.g. "order_date").
        """
        ...

    def clean_string_column(self, column: str) -> None:
        """
        Cleans a string column by converting to lowercase and removing leading/trailing spaces.

        Args:
            column (str): The column name to clean.
        """
        ...

    def filter_rows_by_value_in_set(self, column: str, valid_values: set) -> None:
        """
        Filters rows to keep only those where the column value is in the valid_values set.

        Args:
            column (str): The column name to filter on.
            valid_values (set): Set of valid values to keep.
        """
        ...

    def join(
        self, other: DataFrameEngine, left_on: str, right_on: str, how: str = "left"
    ) -> None:
        """
        Joins another DataFrameEngine to this one, in-place.

        Args:
            other (DataFrameEngine): The other DataFrameEngine to join.
            left_on (str): The column name from the left DataFrame to join on.
            right_on (str): The column name from the right DataFrame to join on.
            how (str): Type of join ('left', 'right', 'inner', 'outer'). Default is 'left'.
        """
        ...

    def add_column_from_expression(self, column_name: str, expression) -> None:
        """
        Adds a new column based on an expression or function.

        Args:
            column_name (str): The name of the new column.
            expression: A function or expression to compute the column values.
        """
        ...

    def group_by_and_aggregate(self, group_columns: list[str], agg_dict: dict) -> None:
        """
        Groups by columns and applies aggregation functions, replacing the DataFrame in-place.

        Args:
            group_columns (list[str]): List of column names to group by.
            agg_dict (dict): Dictionary mapping column names to aggregation functions
                            (e.g., {'amount': 'sum', 'order_id': 'count'}).
        """
        ...

    def filter_rows_by_condition(
        self,
        column: str,
        operator: Literal[">", "<", ">=", "<=", "==", "!="],
        value: Any,
    ) -> None:
        """
        Filters rows based on a condition.

        Args:
            column (str): The column name to filter on.
            operator: The comparison operator ('>', '<', '>=', '<=', '==', '!=').
            value (Any): The value to compare against.
        """
        ...

    def rename_column(self, old_name: str, new_name: str) -> None:
        """
        Renames a column.

        Args:
            old_name (str): The current column name.
            new_name (str): The new column name.
        """
        ...

    def drop_column(self, column: str) -> None:
        """
        Drops a column from the DataFrame. No-op if the column is absent.

        Args:
            column (str): The column name to drop.
        """
        ...

    def fill_missing_dates(self, date_column: str, fill_value: dict) -> None:
        """
        Fills missing dates in a time series with specified values.

        Args:
            date_column (str): The date column name.
            fill_value (dict): Dictionary of column names to values for filling missing dates.
        """
        ...

    def count_rows_where(
        self,
        column: str,
        operator: Literal["in", "not_in", ">", "<", ">=", "<=", "==", "!="],
        value: Any,
    ) -> int:
        """
        Counts the number of rows where a condition is met.

        Args:
            column (str): The column name to check.
            operator: The comparison operator ('in', 'not_in', '>', '<', '>=', '<=', '==', '!=').
            value (Any): The value to compare against (can be a set for 'in'/'not_in' operators).

        Returns:
            int: The count of rows matching the condition.
        """
        ...

    def count_rows(self) -> int:
        """
        Counts the total number of rows in the DataFrame.

        Returns:
            int: The total number of rows.
        """
        ...

    def enforce_schema(self, target_schema: Schema) -> None:
        """
        Enforces the provided schema, parsing types (replacing with null values what failed to be parsed).
        We select only the columns in target_schema.

        Args:
            target_schema (Schema): Schema to enforce.
        """
        ...

    def get_schema(self) -> Schema:
        """
        Returns the dataframe schema

        Returns:
            Schema: dataframe schema
        """
        ...
