from dataclasses import dataclass
from typing import Protocol
from abc import abstractmethod

from ..entities.dataframe import Dataframe


@dataclass
class TransformationRule(Protocol):
    """
    Value Object protocol representing a transformation rule definition.

    Rules are immutable definitions of transformations. They define:
    - What transformation to apply (via apply method)
    - Configuration parameters set at creation

    Attributes:
        tfr_id (str): Unique identifier for this transformation rule
    """

    tfr_id: str

    @abstractmethod
    def apply(self, dataframe: Dataframe):
        """
        Apply the transformation rule to the dataframe (in-place modification).

        Args:
            dataframe (Dataframe): The dataframe to transform
        """


@dataclass
class DeduplicateKeepLatestRule:
    """
    Data-quality transformation that resolves duplicate records.

    For every group sharing the same ``key_column`` value, only the row with the
    most recent ``order_column`` value is kept; earlier duplicates are dropped.
    Typical use: collapse duplicate orders, ignoring the earlier ``order_date``
    and keeping the latest one.

    The actual deduplication is delegated to the backend engine, so the rule is
    engine-agnostic (works on Spark, Pandas, ...).

    Attributes:
        key_column (str): Column identifying duplicates (e.g. "order_id")
        order_column (str): Column whose latest value wins (e.g. "order_date")
        tfr_id (str): Unique identifier for this transformation rule
    """

    key_column: str
    order_column: str
    tfr_id: str = "deduplicate_keep_latest"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()
        df_backend.deduplicate_keep_latest(self.key_column, self.order_column)


@dataclass
class DropColumnsRule:
    """
    Drops the given columns from the dataframe (each ignored if absent).

    Typical use: discard a join key such as ``customer_id`` once the dataframe
    has been enriched with the human-readable value (``customer_name``).

    Attributes:
        columns (tuple[str, ...]): Column names to drop
        tfr_id (str): Unique identifier for this transformation rule
    """

    columns: tuple[str, ...]
    tfr_id: str = "drop_columns"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()
        for column in self.columns:
            df_backend.drop_column(column)
