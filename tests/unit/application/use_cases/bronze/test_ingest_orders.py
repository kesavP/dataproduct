import pytest

from tests.adapters import (
    InMemoryDataSource,
    InMemoryDataTarget,
    InMemoryDataQualityLogger,
)
from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.use_cases.bronze.ingest_orders import (
    IngestOrders,
)


class TestIngestOrders:
    """Test suite for IngestOrders use case."""

    @pytest.fixture
    def sample_orders_data(self):
        """Sample order data for testing."""
        return [
            {
                "order_id": "ORD001",
                "customer_id": "CUST001",
                "order_date": "2024-01-15",
                "amount": 150.75,
            },
            {
                "order_id": "ORD002",
                "customer_id": "CUST002",
                "order_date": "2024-01-16",
                "amount": 89.99,
            },
        ]

    def test_execute_ingests_orders_to_target(self, sample_orders_data):
        # Given: A use case configured with in-memory adapters
        source = InMemoryDataSource(data=sample_orders_data)
        target = InMemoryDataTarget(data=[])
        dq_logger = InMemoryDataQualityLogger()
        use_case = IngestOrders(
            orders_source=source,
            orders_target=target,
            dataframe_engine=PandasDataFrameEngine,
            data_quality_logger=dq_logger,
        )

        # When: Executing the use case
        use_case.execute()

        # Then: Data should be written to target
        result_data = target._data_as_dataframe.collect()

        assert len(result_data) == 2
        assert result_data[0]["order_id"] == "ORD001"
        assert result_data[1]["order_id"] == "ORD002"
