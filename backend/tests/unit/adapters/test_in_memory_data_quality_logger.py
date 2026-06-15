import datetime

from tests.adapters import InMemoryDataQualityLogger
from retail_data_product.domain.data_governance import (
    DataQualityReport,
    DataQualityResult,
    DataQualityStatus,
    DQActionOnFail,
    DQCriticity,
)


class TestInMemoryDataQualityLogger:
    """Unit tests for InMemoryDataQualityLogger adapter."""

    def test_log_stores_data_quality_results(self):
        """Test that log() correctly stores DQ results in memory."""
        # Arrange
        logger = InMemoryDataQualityLogger()
        evaluation_time = datetime.datetime(2025, 12, 14, 10, 30, 0)

        report = DataQualityReport(table_name="orders_raw")
        report.add_a_data_quality_result(
            DataQualityResult(
                dqr_name="order_id_not_null",
                status=DataQualityStatus.PASSED,
                action_fail=DQActionOnFail.ABORT_WORKFLOW,
                dq_criticiy=DQCriticity.CRITICAL,
                message="All order IDs are valid",
                evaluation_datetime=evaluation_time,
            )
        )

        # Act
        logger.log(report)

        # Assert
        assert len(logger._dq_logs) == 1
        logged_entry = logger._dq_logs[0]
        assert logged_entry["table_name"] == "orders_raw"
        assert logged_entry["dqr_name"] == "order_id_not_null"
        assert logged_entry["status"] == DataQualityStatus.PASSED
        assert logged_entry["action_fail"] == DQActionOnFail.ABORT_WORKFLOW
        assert logged_entry["dq_criticiy"] == DQCriticity.CRITICAL
        assert logged_entry["message"] == "All order IDs are valid"
        assert logged_entry["evaluation_datetime"] == evaluation_time

    def test_log_stores_multiple_results_from_single_report(self):
        """Test that log() stores all results from a report with multiple DQ rules."""
        # Arrange
        logger = InMemoryDataQualityLogger()

        report = DataQualityReport(table_name="payments_cleaned")
        report.add_a_data_quality_result(
            DataQualityResult(
                dqr_name="payment_id_not_null",
                status=DataQualityStatus.PASSED,
                action_fail=DQActionOnFail.ABORT_WORKFLOW,
                dq_criticiy=DQCriticity.CRITICAL,
                message="All payment IDs are valid",
            )
        )
        report.add_a_data_quality_result(
            DataQualityResult(
                dqr_name="valid_payment_status",
                status=DataQualityStatus.FAILED,
                action_fail=DQActionOnFail.REMOVE_ROW,
                dq_criticiy=DQCriticity.WARNING,
                message="Found 3 invalid payment statuses",
            )
        )

        # Act
        logger.log(report)

        # Assert
        assert len(logger._dq_logs) == 2
        assert logger._dq_logs[0]["dqr_name"] == "payment_id_not_null"
        assert logger._dq_logs[1]["dqr_name"] == "valid_payment_status"
        assert logger._dq_logs[1]["status"] == DataQualityStatus.FAILED

    def test_log_accumulates_results_from_multiple_reports(self):
        """Test that multiple log() calls accumulate results."""
        # Arrange
        logger = InMemoryDataQualityLogger()

        report1 = DataQualityReport(table_name="orders_raw")
        report1.add_a_data_quality_result(
            DataQualityResult(
                dqr_name="order_id_not_null",
                status=DataQualityStatus.PASSED,
                action_fail=DQActionOnFail.ABORT_WORKFLOW,
                dq_criticiy=DQCriticity.CRITICAL,
                message="OK",
            )
        )

        report2 = DataQualityReport(table_name="campaigns_raw")
        report2.add_a_data_quality_result(
            DataQualityResult(
                dqr_name="campaign_id_not_null",
                status=DataQualityStatus.PASSED,
                action_fail=DQActionOnFail.ABORT_WORKFLOW,
                dq_criticiy=DQCriticity.CRITICAL,
                message="OK",
            )
        )

        # Act
        logger.log(report1)
        logger.log(report2)

        # Assert
        assert len(logger._dq_logs) == 2
        assert logger._dq_logs[0]["table_name"] == "orders_raw"
        assert logger._dq_logs[1]["table_name"] == "campaigns_raw"

    def test_log_empty_report(self):
        """Test that logging an empty report adds no entries."""
        # Arrange
        logger = InMemoryDataQualityLogger()
        report = DataQualityReport(table_name="empty_table")

        # Act
        logger.log(report)

        # Assert
        assert len(logger._dq_logs) == 0
