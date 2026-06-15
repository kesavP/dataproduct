from ...data_governance import (
    FieldIsNotNullRule,
    DataQualityRule,
    Dataframe,
    DataQualityResult,
    DataQualityStatus,
    DQCriticity,
    DQActionOnFail,
)


class CampaignIdNotNull(FieldIsNotNullRule):
    """Check that campaign id is not null"""

    def __init__(self):
        super().__init__(
            dqr_id="campaign_id_not_null",
            criticity=DQCriticity.CRITICAL,
            action_on_fail=DQActionOnFail.ABORT_WORKFLOW,
            field_name="campaign_id",
        )


class ValidChannelRule(DataQualityRule):
    """Validates that marketing channel belongs to the allowed set of values."""

    VALID_CHANNELS = frozenset({"email", "sms"})

    def __init__(self):
        super().__init__(
            dqr_id="valid_channel",
            criticity=DQCriticity.WARNING,
            action_on_fail=DQActionOnFail.REMOVE_ROW,
        )

    def evaluate_dataframe(self, dataframe: Dataframe) -> DataQualityResult:
        if "channel" not in dataframe.get_column_names():
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message="channel column not found",
            )

        invalid_count = dataframe.count_rows_where(
            "channel", "not_in", self.VALID_CHANNELS
        )

        if invalid_count == 0:
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.PASSED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message="All channels are valid",
            )
        else:
            if self.action_on_fail == DQActionOnFail.REMOVE_ROW:
                dataframe.filter_rows_by_value_in_set("channel", self.VALID_CHANNELS)

            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message=f"Found {invalid_count} invalid channels - rows removed",
            )
