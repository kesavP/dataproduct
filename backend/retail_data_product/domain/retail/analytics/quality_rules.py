from ...data_governance import (
    DataQualityRule,
    Dataframe,
    DataQualityResult,
    DataQualityStatus,
    DQCriticity,
    DQActionOnFail,
)


class RoiValueBoundsRule(DataQualityRule):
    """Validates that ROI values are within reasonable bounds."""

    def __init__(
        self,
        min_roi: float = -100.0,
        max_roi: float = 10000.0,
    ):
        super().__init__(
            dqr_id="roi_value_bounds",
            criticity=DQCriticity.WARNING,
            action_on_fail=DQActionOnFail.LOG,
        )
        self.min_roi = min_roi  # Cannot lose more than 100%
        self.max_roi = max_roi  # Sanity check upper bound

    def evaluate_dataframe(self, dataframe: Dataframe) -> DataQualityResult:
        if "roi" not in dataframe.get_column_names():
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message="roi column not found",
            )

        below_min = dataframe.count_rows_where("roi", "<", self.min_roi)
        above_max = dataframe.count_rows_where("roi", ">", self.max_roi)
        out_of_bounds = below_min + above_max

        if out_of_bounds == 0:
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.PASSED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message=f"All ROI values within bounds [{self.min_roi}, {self.max_roi}]",
            )
        else:
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message=f"Found {out_of_bounds} ROI values out of bounds [{self.min_roi}, {self.max_roi}]",
            )


class AvailableDataRule(DataQualityRule):
    """Validates that the dataframe contains data (not empty)."""

    def __init__(self):
        super().__init__(
            dqr_id="available_data",
            criticity=DQCriticity.CRITICAL,
            action_on_fail=DQActionOnFail.ABORT_WORKFLOW,
        )

    def evaluate_dataframe(self, dataframe: Dataframe) -> DataQualityResult:
        row_count = dataframe.count_rows()

        if row_count > 0:
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.PASSED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message=f"Data available: {row_count} rows",
            )
        else:
            return DataQualityResult(
                dqr_name=self.dqr_id,
                status=DataQualityStatus.FAILED,
                action_fail=self.action_on_fail,
                dq_criticiy=self.criticity,
                message="No data available in dataframe",
            )
