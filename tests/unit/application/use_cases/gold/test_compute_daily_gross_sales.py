import pytest

from tests.adapters import (
    InMemoryDataSource,
    InMemoryDataTarget,
    InMemoryDataQualityLogger,
)
from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.use_cases.gold.compute_daily_gross_sales import (
    ComputeDailyGrossSales,
)


class TestComputeDailyGrossSales:
    """Test suite for ComputeDailyGrossSales use case."""

    @pytest.fixture
    def sample_orders_enriched_data(self):
        """Sample enriched order data for testing."""
        return [
            {
                "order_id": "ORD001",
                "order_date": "2024-01-15",
                "total_amount_eur": 150.75,
            },
            {
                "order_id": "ORD002",
                "order_date": "2024-01-15",
                "total_amount_eur": 89.99,
            },
            {
                "order_id": "ORD003",
                "order_date": "2024-01-16",
                "total_amount_eur": 200.50,
            },
        ]

    def test_execute_computes_daily_sales_to_target(self, sample_orders_enriched_data):
        # Given: A use case configured with in-memory adapters
        source = InMemoryDataSource(data=sample_orders_enriched_data)
        target = InMemoryDataTarget(data=[])
        dq_logger = InMemoryDataQualityLogger()
        use_case = ComputeDailyGrossSales(
            orders_enriched_source=source,
            daily_gross_sales_target=target,
            dataframe_engine=PandasDataFrameEngine,
            data_quality_logger=dq_logger,
        )

        # When: Executing the use case
        use_case.execute()

        # Then: Data should be written to target
        result_data = target._data_as_dataframe.collect()

        assert len(result_data) == 2
        assert str(result_data[0]["order_date"]).startswith("2024-01-15")
        assert str(result_data[1]["order_date"]).startswith("2024-01-16")
