from typing import Type, Sequence

from ...ports import DataSource, DataTarget, DataFrameEngine, DataQualityLogger
from ..base_etl_use_case import BaseETLUseCase

from ....domain.data_governance import Dataframe, TableDefinition, DataQualityRule
from ....domain.retail.payments import PAYMENTS_RAW, PaymentIdNotNull


class IngestPayments(BaseETLUseCase):
    """
    Bronze Layer: Ingest raw payment data from external source.

    This use case handles the ingestion of payment data into the bronze layer,
    performing critical data quality validations (payment_id not null).
    """

    def __init__(
        self,
        payments_source: DataSource,
        payments_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        data_quality_logger: DataQualityLogger,
    ):
        """
        Args:
            payments_source (DataSource): source of the payment raw data
            payments_target (DataTarget): target where to save the bronze payment raw data
            dataframe_engine (Type[DataFrameEngine]): data processing engine
            data_quality_logger (DataQualityLogger): logger for data quality results
        """
        super().__init__(payments_target, dataframe_engine, data_quality_logger)
        self._payments_source = payments_source

    def fetch_data(self) -> Dataframe:
        return self._payments_source.fetch(self._dataframe_engine)

    def get_target_table(self) -> TableDefinition:
        return PAYMENTS_RAW

    def get_quality_rules(self) -> Sequence[DataQualityRule]:
        return (PaymentIdNotNull(),)
