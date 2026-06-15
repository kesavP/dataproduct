from typing import Type, Sequence

from ...ports import DataSource, DataTarget, DataFrameEngine, DataQualityLogger
from ..base_etl_use_case import BaseETLUseCase

from ....domain.data_governance import Dataframe, TableDefinition, DataQualityRule
from ....domain.retail.orders import ORDERS_RAW, OrderIdNotNull


class IngestOrders(BaseETLUseCase):
    """
    Bronze Layer: Ingest raw order data from the E-commerce platform.

    This use case handles the ingestion of order data from into the bronze layer,
    performing critical data quality validations. Raw orders are landed as-is;
    cleaning and enrichment happen later in the silver layer.
    """

    def __init__(
        self,
        orders_source: DataSource,
        orders_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        data_quality_logger: DataQualityLogger,
    ):
        """
        Args:
            orders_source (DataSource): data source for the order raw data
            orders_target (DataTarget): data target where to save the raw orders table
            dataframe_engine (Type[DataFrameEngine]): data processing engine
            data_quality_logger (DataQualityLogger): logger for data quality results
        """
        super().__init__(orders_target, dataframe_engine, data_quality_logger)
        self._orders_source = orders_source

    def fetch_data(self) -> Dataframe:
        return self._orders_source.fetch(self._dataframe_engine)

    def get_target_table(self) -> TableDefinition:
        return ORDERS_RAW

    def get_quality_rules(self) -> Sequence[DataQualityRule]:
        return (OrderIdNotNull(),)
