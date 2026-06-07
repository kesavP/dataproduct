from typing import Type, Sequence

from ...ports import DataSource, DataTarget, DataFrameEngine, DataQualityLogger
from ..base_etl_use_case import BaseETLUseCase

from ....domain.data_governance import Dataframe, TableDefinition, DataQualityRule
from ....domain.retail.analytics import (
    DAILY_GROSS_SALES,
    AvailableDataRule,
    AggregateDailySalesRule,
    FillMissingDatesRule,
)


class ComputeDailyGrossSales(BaseETLUseCase):
    """
    Gold Layer: Compute daily gross sales aggregates.

    This use case aggregates order data by day to produce daily sales metrics
    and validates that sufficient historical data is available.
    """

    def __init__(
        self,
        orders_enriched_source: DataSource,
        daily_gross_sales_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        data_quality_logger: DataQualityLogger,
    ):
        """
        Args:
            orders_enriched_source (DataSource): silver.orders_enriched table
            daily_gross_sales_target (DataTarget): gold.daily_gross_sales table
            dataframe_engine (Type[DataFrameEngine]): data processing engine
            data_quality_logger (DataQualityLogger): logger for data quality results
        """
        super().__init__(
            daily_gross_sales_target, dataframe_engine, data_quality_logger
        )
        self._orders_enriched_source = orders_enriched_source

    def fetch_data(self) -> Dataframe:
        return self._orders_enriched_source.fetch(self._dataframe_engine)

    def get_target_table(self) -> TableDefinition:
        return DAILY_GROSS_SALES

    def get_quality_rules(self) -> Sequence[DataQualityRule]:
        return (AvailableDataRule(),)

    def get_transformation_rules(self) -> Sequence:
        return (AggregateDailySalesRule(), FillMissingDatesRule())
