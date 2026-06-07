from typing import Type, Sequence

from ...ports import DataSource, DataTarget, DataFrameEngine, DataQualityLogger
from ..base_etl_use_case import BaseETLUseCase

from ....domain.data_governance import Dataframe, TableDefinition, DataQualityRule
from ....domain.retail.payments import (
    PAYMENTS_CLEANED,
    ValidPaymentStatusRule,
    CleanPaymentStatusRule,
)


class CleanPayments(BaseETLUseCase):
    """
    Silver Layer: Clean payment data by standardizing and removing invalid records.

    This use case transforms raw payment data into cleaned data by:
    - Cleaning payment status and channel fields
    - Validating payment statuses and channels
    - Removing invalid records (REMOVE_ROW action)
    """

    def __init__(
        self,
        payments_raw_source: DataSource,
        payments_cleaned_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        data_quality_logger: DataQualityLogger,
    ):
        """
        Args:
            payments_raw_source (DataSource): bronze.payments_raw table
            payments_cleaned_target (DataTarget): silver.payments_cleaned table
            dataframe_engine (Type[DataFrameEngine]): data processing engine
            data_quality_logger (DataQualityLogger): logger for data quality results
        """
        super().__init__(payments_cleaned_target, dataframe_engine, data_quality_logger)
        self._payments_raw_source = payments_raw_source

    def fetch_data(self) -> Dataframe:
        return self._payments_raw_source.fetch(self._dataframe_engine)

    def get_target_table(self) -> TableDefinition:
        return PAYMENTS_CLEANED

    def get_quality_rules(self) -> Sequence[DataQualityRule]:
        return (ValidPaymentStatusRule(),)

    def get_transformation_rules(self) -> Sequence:
        return (CleanPaymentStatusRule(),)
