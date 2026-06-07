import pytest

from tests.adapters import (
    InMemoryDataSource,
    InMemoryDataTarget,
    InMemoryDataQualityLogger,
)
from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.use_cases.bronze.ingest_payments import (
    IngestPayments,
)


class TestIngestPayments:
    """Test suite for IngestPayments use case."""

    @pytest.fixture
    def sample_payments_data(self):
        """Sample payment data for testing."""
        return [
            {
                "payment_id": "PAY001",
                "order_id": "ORD001",
                "payment_date": "2024-01-15",
                "amount": 150.75,
                "status": "completed",
            },
            {
                "payment_id": "PAY002",
                "order_id": "ORD002",
                "payment_date": "2024-01-16",
                "amount": 89.99,
                "status": "completed",
            },
        ]

    def test_execute_ingests_payments_to_target(self, sample_payments_data):
        # Given: A use case configured with in-memory adapters
        source = InMemoryDataSource(data=sample_payments_data)
        target = InMemoryDataTarget(data=[])
        dq_logger = InMemoryDataQualityLogger()
        use_case = IngestPayments(
            payments_source=source,
            payments_target=target,
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
