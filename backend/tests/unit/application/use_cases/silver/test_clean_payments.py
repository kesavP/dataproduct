import pytest

from tests.adapters import (
    InMemoryDataSource,
    InMemoryDataTarget,
    InMemoryDataQualityLogger,
)
from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.use_cases.silver.clean_payments import (
    CleanPayments,
)


class TestCleanPayments:
    """Test suite for CleanPayments use case."""

    @pytest.fixture
    def sample_payments_raw_data(self):
        """Sample raw payment data for testing."""
        return [
            {
                "payment_id": "PAY001",
                "order_id": "ORD001",
                "payment_date": "2024-01-15",
                "amount": 150.75,
                "status": "completed",
                "channel": "credit_card",
            },
            {
                "payment_id": "PAY002",
                "order_id": "ORD002",
                "payment_date": "2024-01-16",
                "amount": 89.99,
                "status": "pending",
                "channel": "paypal",
            },
        ]

    def test_execute_cleans_payments_to_target(self, sample_payments_raw_data):
        # Given: A use case configured with in-memory adapters
        source = InMemoryDataSource(data=sample_payments_raw_data)
        target = InMemoryDataTarget(data=[])
        dq_logger = InMemoryDataQualityLogger()
        use_case = CleanPayments(
            payments_raw_source=source,
            payments_cleaned_target=target,
            dataframe_engine=PandasDataFrameEngine,
            data_quality_logger=dq_logger,
        )

        # When: Executing the use case
        use_case.execute()

        # Then: Data should be written to target
        result_data = target._data_as_dataframe.collect()

        assert len(result_data) == 2
        assert result_data[0]["payment_id"] == "PAY001"
        assert result_data[1]["payment_id"] == "PAY002"
