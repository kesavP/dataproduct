import pytest

from tests.adapters import (
    InMemoryDataSource,
    InMemoryDataTarget,
    InMemoryDataQualityLogger,
)
from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.use_cases.silver.enrich_orders import (
    EnrichOrders,
)


class TestEnrichOrders:
    """Test suite for EnrichOrders use case."""

    @pytest.fixture
    def sample_orders_data(self):
        """Sample order data for testing."""
        return [
            {
                "order_id": "ORD001",
                "customer_id": "CUST001",
                "order_date": "2024-01-15",
                "total_amount": 150.75,
                "currency": "USD",
            },
            {
                "order_id": "ORD002",
                "customer_id": "CUST002",
                "order_date": "2024-01-16",
                "total_amount": 89.99,
                "currency": "EUR",
            },
        ]

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

    @pytest.fixture
    def exchange_rates(self):
        """Sample exchange rates."""
        return {"USD": 0.92, "EUR": 1.0, "GBP": 1.17}

    def test_execute_enriches_orders_to_target(
        self, sample_orders_data, sample_payments_data, exchange_rates
    ):
        # Given: A use case configured with in-memory adapters
        orders_source = InMemoryDataSource(data=sample_orders_data)
        payments_source = InMemoryDataSource(data=sample_payments_data)
        target = InMemoryDataTarget(data=[])
        dq_logger = InMemoryDataQualityLogger()
        use_case = EnrichOrders(
            orders_source=orders_source,
            payments_source=payments_source,
            orders_enriched_target=target,
            dataframe_engine=PandasDataFrameEngine,
            exchange_rates=exchange_rates,
            data_quality_logger=dq_logger,
        )

        # When: Executing the use case
        use_case.execute()

        # Then: Data should be written to target
        result_data = target._data_as_dataframe.collect()

        assert len(result_data) == 2
        assert result_data[0]["order_id"] == "ORD001"
        assert result_data[1]["order_id"] == "ORD002"
