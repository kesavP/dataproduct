from dataclasses import dataclass, field

from ..value_objects import DataQualityResult, DataQualityStatus, DQActionOnFail


@dataclass
class DataQualityReport:
    """
    Aggregate root representing a collection of data quality results for a table.

    This aggregate ensures consistency across all DQ evaluations for a single table
    and provides operations to assert on the overall quality.

    Attributes:
        table_name (str): name of the targeted table
        data_quality_results (list[DataQualityResult]): list of data quality results
    """

    table_name: str
    data_quality_results: list[DataQualityResult] = field(default_factory=list)

    def add_a_data_quality_result(self, dq_result: DataQualityResult):
        """
        Add a data quality result to the report.

        This is the only way to add results, maintaining the aggregate boundary.

        Args:
            dq_result (DataQualityResult): The data quality result to add
        """
        self.data_quality_results.append(dq_result)

    def assert_no_critical_dq_are_failed(self):
        """
        Assert that no critical data quality rules have failed.

        This enforces the business invariant: critical failures must abort workflow.

        Raises:
            AssertionError: If any critical DQ rule has failed
        """
        for dq_result in self.data_quality_results:
            assert (
                dq_result.status == DataQualityStatus.PASSED
                or dq_result.action_fail != DQActionOnFail.ABORT_WORKFLOW
            ), (
                f"Data quality rule {dq_result.dqr_name} evaluated for table "
                f"{self.table_name} failed with status {dq_result.status} and "
                f"action is {dq_result.action_fail}. Aborting workflow."
            )
