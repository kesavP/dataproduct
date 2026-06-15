from typing import Type, Sequence

from ...ports import DataSource, DataTarget, DataFrameEngine, DataQualityLogger
from ..base_etl_use_case import BaseETLUseCase

from ....domain.data_governance import Dataframe, TableDefinition, DataQualityRule
from ....domain.retail.orders import (
    ORDERS_ENRICHED,
    OrderIdNotNull,
    PositiveTotalAmountRule,
    ConvertCurrencyRule,
    JoinPaymentsRule,
)


class EnrichOrders(BaseETLUseCase):
    """
    Silver Layer: Enrich order data by joining with payment data and converting currencies.

    This use case demonstrates a more complex ETL pattern where:
    - Multiple data sources are needed (orders + payments)
    - Additional parameters are required (exchange rates)
    - The transformation rules need dependencies injected
    """

    def __init__(
        self,
        orders_source: DataSource,
        payments_source: DataSource,
        orders_enriched_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        exchange_rates: dict[str, float],
        data_quality_logger: DataQualityLogger,
    ):
        """
        Args:
            orders_source (DataSource): bronze.clickstream table
            payments_source (DataSource): silver.payments_cleaned table
            orders_enriched_target (DataTarget): silver.orders_enriched table
            dataframe_engine (Type[DataFrameEngine]): data procesing engine
            exchange_rates (dict[str, float]): currency as key (eg EUR, USD) and exchanges
            rates to EUR
            data_quality_logger (DataQualityLogger): logger for data quality results
        """
        super().__init__(orders_enriched_target, dataframe_engine, data_quality_logger)
        self._orders_source = orders_source
        self._payments_source = payments_source
        self._exchange_rates = exchange_rates

    def fetch_data(self) -> Dataframe:
        return self._orders_source.fetch(self._dataframe_engine)

    def get_target_table(self) -> TableDefinition:
        return ORDERS_ENRICHED

    def get_quality_rules(self) -> Sequence[DataQualityRule]:
        return (OrderIdNotNull(), PositiveTotalAmountRule())

    def get_transformation_rules(self) -> Sequence:
        payments_df = self._payments_source.fetch(self._dataframe_engine)
        return (
            JoinPaymentsRule(payments_df=payments_df),
            ConvertCurrencyRule(exchange_rates=self._exchange_rates),
        )
