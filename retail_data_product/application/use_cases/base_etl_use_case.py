from abc import ABC, abstractmethod
from typing import Type, Sequence

from ..ports import DataTarget, DataFrameEngine, DataQualityLogger

from ...domain.data_governance import (
    Dataframe,
    DataQualityRule,
    DataQualityChecker,
    TableDefinition,
)


class BaseETLUseCase(ABC):
    """
    Base class for ETL use cases following the Template Method pattern.

    This class provides the common orchestration for:
    1. Fetching data from source(s)
    2. Applying transformations
    3. Validating data quality
    4. Writing to target

    Concrete use cases implement the abstract methods to define their specific
    business logic while reusing the common workflow.
    """

    def __init__(
        self,
        data_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        data_quality_logger: DataQualityLogger,
    ):
        self._data_target = data_target
        self._dataframe_engine = dataframe_engine
        self.data_quality_logger = data_quality_logger

    @abstractmethod
    def fetch_data(self) -> Dataframe:
        """
        Fetch data from source(s).

        Override this method to define how to fetch data for this specific use case.
        Can involve fetching from single or multiple sources.

        Returns:
            Dataframe with the fetched data
        """

    @abstractmethod
    def get_target_table(self) -> TableDefinition:
        """
        Get the target table definition (domain value object).

        Returns:
            The TableDefinition that defines the target schema
        """

    @abstractmethod
    def get_quality_rules(self) -> Sequence[DataQualityRule]:
        """
        Get the data quality rules to apply.

        Returns:
            Sequence of DataQualityRule to evaluate
        """

    def get_transformation_rules(self) -> Sequence:
        """
        Get the transformation rules to apply.

        Override this method if transformations are needed.
        Default implementation returns empty sequence (no transformations).

        Returns:
            Sequence of transformation rules with an `apply(df)` method
        """
        return ()

    def execute(self):
        """
        Execute the ETL workflow.

        This is the Template Method that orchestrates the ETL process:
        1. Fetch data
        2. Apply transformations
        3. Validate data quality
        4. Write to target
        """
        # Step 1: Fetch data
        df = self.fetch_data()

        # Step 2: Apply transformations
        for rule in self.get_transformation_rules():
            rule.apply(df)

        # Step 3: Validate data quality
        target_table = self.get_target_table()
        quality_rules = self.get_quality_rules()

        dq_report = DataQualityChecker.check(
            df=df,
            rules=quality_rules,
            table_name=target_table.name,
        )

        self.data_quality_logger.log(dq_report)

        # For critical failures, abort the workflow
        # (non-critical failures like REMOVE_ROW, FLAG, LOG are already handled)
        dq_report.assert_no_critical_dq_are_failed()

        # Step 4: Write to target
        assert (
            target_table.primary_key is not None
        ), f"{target_table.name} domain definition does not contain a primary key which is required for upsert"

        self._data_target.upsert(df, target_table.primary_key[0])
