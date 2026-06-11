from typing import Type, Sequence, Optional

from ...ports import DataSource, DataTarget, DataFrameEngine, DataQualityLogger
from ..base_etl_use_case import BaseETLUseCase

from ....domain.data_governance import (
    Dataframe,
    TableDefinition,
    DataQualityRule,
    DropColumnsRule,
)
from ....domain.retail.orders import (
    ORDERS_CLEANED,
    OrderIdNotNull,
    DuplicateOrdersRule,
    FormatCustomerIdRule,
    JoinCustomersRule,
)


class CleanOrders(BaseETLUseCase):
    """
    Silver Layer: Clean and enrich raw orders.

    Reads the bronze ``clickstream`` dataset and produces ``orders_cleaned`` by:
    - collapsing duplicate orders, keeping the row with the latest ``order_date``
      (earlier duplicates are dropped),
    - enriching each order with the customer name from the customers reference
      data (when a customers source is provided).
    """

    def __init__(
        self,
        orders_raw_source: DataSource,
        orders_cleaned_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        data_quality_logger: DataQualityLogger,
        customers_source: Optional[DataSource] = None,
    ):
        """
        Args:
            orders_raw_source (DataSource): bronze.clickstream table
            orders_cleaned_target (DataTarget): silver.orders_cleaned table
            dataframe_engine (Type[DataFrameEngine]): data processing engine
            data_quality_logger (DataQualityLogger): logger for data quality results
            customers_source (Optional[DataSource]): reference source mapping
                customer_id -> customer_name. When provided, orders are enriched
                with the customer name.
        """
        super().__init__(orders_cleaned_target, dataframe_engine, data_quality_logger)
        self._orders_raw_source = orders_raw_source
        self._customers_source = customers_source

    def fetch_data(self) -> Dataframe:
        return self._orders_raw_source.fetch(self._dataframe_engine)

    def get_target_table(self) -> TableDefinition:
        return ORDERS_CLEANED

    def get_quality_rules(self) -> Sequence[DataQualityRule]:
        # DuplicateOrdersRule deduplicates (keep latest order_date) and audits the
        # number of earlier duplicates it removed in the data quality log.
        return (OrderIdNotNull(), DuplicateOrdersRule())

    def get_transformation_rules(self) -> Sequence:
        if self._customers_source is None:
            return ()
        customers_df = self._customers_source.fetch(self._dataframe_engine)
        # Format customer_id to match reference format, then join with customer names,
        # and drop the customer_id join key so the silver table exposes the name only.
        return (
            FormatCustomerIdRule(),
            JoinCustomersRule(customers_df=customers_df),
            DropColumnsRule(columns=("customer_id",)),
        )
