from typing import Protocol

from ...domain.data_governance import DataQualityReport


class DataQualityLogger(Protocol):
    """
    Data Quality Logger interface. Its role is to define how to save a Data quality report object into
    the environment (most likely a table).
    """

    def log(self, data_quality_report: DataQualityReport) -> None:
        """
        Log the data quality report.

        Args:
            data_quality_report (DataQualityReport): data quality report to log
        """
        ...
