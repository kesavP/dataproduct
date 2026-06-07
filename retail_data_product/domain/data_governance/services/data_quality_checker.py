from typing import Sequence

from ..entities.dataframe import Dataframe
from ..value_objects.data_quality import DataQualityRule
from ..entities.data_quality_report import DataQualityReport


class DataQualityChecker:
    """
    Domain Service that checks data quality against rules.

    Stateless service that takes a Dataframe and rules,
    returns a quality report.
    """

    @staticmethod
    def check(
        df: Dataframe,
        rules: Sequence[DataQualityRule],
        table_name: str = "",
    ) -> DataQualityReport:
        """
        Check a dataframe against quality rules.

        Args:
            df: The dataframe to check
            rules: Sequence of rules to evaluate
            table_name: Optional table name for the report

        Returns:
            DataQualityReport with all evaluation results
        """
        report = DataQualityReport(table_name=table_name)

        for rule in rules:
            result = rule.evaluate_dataframe(df)
            report.add_a_data_quality_result(result)

        return report
