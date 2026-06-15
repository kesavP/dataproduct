from retail_data_product.application.ports import DataQualityLogger

from retail_data_product.domain.data_governance import DataQualityReport


class InMemoryDataQualityLogger(DataQualityLogger):
    def __init__(self):
        """
        In-memory DataQualityLogger implementation. Principaly used to do unit testing.
        """
        self._dq_logs = []

    def log(self, data_quality_report: DataQualityReport):
        table_name = data_quality_report.table_name
        for dq in data_quality_report.data_quality_results:
            self._dq_logs.append(
                {
                    "table_name": table_name,
                    "dqr_name": dq.dqr_name,
                    "status": dq.status,
                    "action_fail": dq.action_fail,
                    "dq_criticiy": dq.dq_criticiy,
                    "message": dq.message,
                    "evaluation_datetime": dq.evaluation_datetime,
                }
            )
