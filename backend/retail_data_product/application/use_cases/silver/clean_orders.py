from typing import Type, Sequence, Optional

from ...ports import DataSource, DataTarget, DataFrameEngine, DataQualityLogger
from ..base_etl_use_case import BaseETLUseCase
from ..transformation_handler import TransformationHandler

from ....domain.data_governance import (
    Dataframe,
    TableDefinition,
    DataQualityRule,
    DataQualityChecker,
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

    Transformation failures are optionally captured to a DLQ table instead of
    failing the entire batch. This allows partial success when records don't
    match the expected transformation schema.
    """

    def __init__(
        self,
        orders_raw_source: DataSource,
        orders_cleaned_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        data_quality_logger: DataQualityLogger,
        customers_source: Optional[DataSource] = None,
        dlq_target: Optional[DataTarget] = None,
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
            dlq_target (Optional[DataTarget]): target for transformation failures.
                When provided, records that fail transformation are written here
                instead of failing the entire batch.
        """
        super().__init__(orders_cleaned_target, dataframe_engine, data_quality_logger)
        self._orders_raw_source = orders_raw_source
        self._customers_source = customers_source
        self._dlq_target = dlq_target

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

    def execute(self):
        """
        Execute the ETL workflow with optional transformation failure capture.

        If a DLQ target is configured, transformation failures are captured
        there and don't fail the entire batch. Otherwise, failures propagate
        as exceptions (original behavior).
        """
        df = self.fetch_data()

        if self._dlq_target:
            handler = TransformationHandler(
                dataframe_engine=self._dataframe_engine,
                dlq_target=self._dlq_target,
                data_quality_logger=self.data_quality_logger,
            )
            handler.apply_transformations(df, self.get_transformation_rules())
        else:
            for rule in self.get_transformation_rules():
                rule.apply(df)

        target_table = self.get_target_table()
        quality_rules = self.get_quality_rules()

        dq_report = DataQualityChecker.check(
            df=df,
            rules=quality_rules,
            table_name=target_table.name,
        )

        self.data_quality_logger.log(dq_report)
        dq_report.assert_no_critical_dq_are_failed()

        assert (
            target_table.primary_key is not None
        ), f"{target_table.name} domain definition does not contain a primary key which is required for upsert"

        self._data_target.upsert(df, target_table.primary_key[0])
