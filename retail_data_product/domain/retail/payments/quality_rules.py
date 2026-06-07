from ...data_governance import (
    FieldIsNotNullRule,
    DataQualityRule,
    Dataframe,
    DataQualityResult,
    DataQualityStatus,
    DQCriticity,
    DQActionOnFail,
)


class PaymentIdNotNull(FieldIsNotNullRule):
    """Check that payment id is not null"""

    def __init__(self):
        super().__init__(
            dqr_id="payment_id_not_null",
            criticity=DQCriticity.CRITICAL,
            action_on_fail=DQActionOnFail.ABORT_WORKFLOW,
            field_name="payment_id",
        )


class ValidPaymentStatusRule(DataQualityRule):
    """Validates that payment status belongs to the allowed set of values."""

    VALID_STATUSES = frozenset({"success", "failed", "refunded"})

    def __init__(self):
        super().__init__(
            dqr_id="valid_payment_status",
            criticity=DQCriticity.WARNING,
            action_on_fail=DQActionOnFail.REMOVE_ROW,
        )

    def evaluate_dataframe(self, dataframe: Dataframe) -> DataQualityResult:
        if "payment_status" not in dataframe.get_column_names():
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message="payment_status column not found",
            )

        invalid_count = dataframe.count_rows_where(
            "payment_status", "not_in", self.VALID_STATUSES
        )

        if invalid_count == 0:
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.PASSED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message="All payment statuses are valid",
            )
        else:
            if self.action_on_fail == DQActionOnFail.REMOVE_ROW:
                dataframe.filter_rows_by_value_in_set(
                    "payment_status", self.VALID_STATUSES
                )

            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message=f"Found {invalid_count} invalid payment statuses - rows removed",
            )
