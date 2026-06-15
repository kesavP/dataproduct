from ...data_governance import (
    FieldIsNotNullRule,
    DataQualityRule,
    DeduplicateKeepLatestRule,
    Dataframe,
    DataQualityResult,
    DataQualityStatus,
    DQCriticity,
    DQActionOnFail,
)


class OrderIdNotNull(FieldIsNotNullRule):
    """Check that order id is not null"""

    def __init__(self):
        super().__init__(
            dqr_id="order_id_not_null",
            criticity=DQCriticity.CRITICAL,
            action_on_fail=DQActionOnFail.ABORT_WORKFLOW,
            field_name="order_id",
        )


class PositiveTotalAmountRule(DataQualityRule):
    """Validates that order amounts are positive values."""

    def __init__(self):
        super().__init__(
            dqr_id="positive_total_amount",
            criticity=DQCriticity.WARNING,
            action_on_fail=DQActionOnFail.REMOVE_ROW,
        )

    def evaluate_dataframe(self, dataframe: Dataframe) -> DataQualityResult:
        if "total_amount_eur" not in dataframe.get_column_names():
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message="total_amount_eur column not found",
            )

        invalid_count = dataframe.count_rows_where("total_amount_eur", "<=", 0)

        if invalid_count == 0:
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.PASSED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message="All amounts are positive",
            )
        else:
            if self.action_on_fail == DQActionOnFail.REMOVE_ROW:
                dataframe.filter_rows_by_condition("total_amount_eur", ">", 0)

            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message=f"Found {invalid_count} non-positive amounts - rows removed",
            )


class DuplicateOrdersRule(DataQualityRule):
    """
    Resolves duplicate orders, keeping the row with the latest order_date.

    Earlier duplicates are dropped and the number of removed rows is reported so
    the deduplication is auditable in the data quality log. The actual removal is
    delegated to the reusable DeduplicateKeepLatestRule transformation.
    """

    def __init__(self):
        super().__init__(
            dqr_id="duplicate_orders",
            criticity=DQCriticity.WARNING,
            action_on_fail=DQActionOnFail.REMOVE_ROW,
        )

    def evaluate_dataframe(self, dataframe: Dataframe) -> DataQualityResult:
        for required in ("order_id", "order_date"):
            if required not in dataframe.get_column_names():
                return DataQualityResult(
                    dqr_name=self.dqr_id,
                    status=DataQualityStatus.FAILED,
                    action_fail=self.action_on_fail,
                    dq_criticiy=self.criticity,
                    message=f"{required} column not found",
                )

        rows_before = dataframe.count_rows()
        DeduplicateKeepLatestRule(
            key_column="order_id", order_column="order_date"
        ).apply(dataframe)
        removed = rows_before - dataframe.count_rows()

        if removed == 0:
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.PASSED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message="No duplicate orders found",
            )

        return DataQualityResult(
            dqr_name=self.dqr_id,
            status=DataQualityStatus.FAILED,
            action_fail=self.action_on_fail,
            dq_criticiy=self.criticity,
            message=f"Removed {removed} earlier duplicate order(s), kept latest order_date",
        )
