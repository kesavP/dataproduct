import pytest

from tests.adapters import (
    InMemoryDataSource,
    InMemoryDataTarget,
    InMemoryDataQualityLogger,
)
from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.use_cases.silver.enrich_campaigns import (
    EnrichCampaigns,
)


class TestEnrichCampaigns:
    """Test suite for EnrichCampaigns use case."""

    @pytest.fixture
    def sample_campaigns_data(self):
        """Sample campaign data for testing."""
        return [
            {
                "campaign_id": "CAMP001",
                "channel": "email",
                "cost": 1500.50,
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
            {
                "campaign_id": "CAMP002",
                "channel": "sms",
                "cost": 800.00,
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        ]

    @pytest.fixture
    def sample_orders_data(self):
        """Sample order data for testing."""
        return [
            {
                "order_id": "ORD001",
                "campaign_id": "CAMP001",
                "total_amount_eur": 250.00,
            },
            {
                "order_id": "ORD002",
                "campaign_id": "CAMP001",
                "total_amount_eur": 180.00,
            },
            {
                "order_id": "ORD003",
                "campaign_id": "CAMP002",
                "total_amount_eur": 95.00,
            },
        ]

    def test_execute_enriches_campaigns_to_target(
        self, sample_campaigns_data, sample_orders_data
    ):
        # Given: A use case configured with in-memory adapters
        campaigns_source = InMemoryDataSource(data=sample_campaigns_data)
        orders_source = InMemoryDataSource(data=sample_orders_data)
        target = InMemoryDataTarget(data=[])
        dq_logger = InMemoryDataQualityLogger()
        use_case = EnrichCampaigns(
            campaigns_source=campaigns_source,
            orders_source=orders_source,
            campaigns_enriched_target=target,
            dataframe_engine=PandasDataFrameEngine,
            data_quality_logger=dq_logger,
        )

        # When: Executing the use case
        use_case.execute()

        # Then: Data should be written to target
        result_data = target._data_as_dataframe.collect()

        assert len(result_data) == 2
        assert result_data[0]["campaign_id"] == "CAMP001"
        assert result_data[1]["campaign_id"] == "CAMP002"
