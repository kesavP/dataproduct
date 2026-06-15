import pytest

from tests.adapters import (
    InMemoryDataSource,
    InMemoryDataTarget,
    InMemoryDataQualityLogger,
)
from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.use_cases.bronze.ingest_campaigns import (
    IngestCampaigns,
)


class TestIngestCampaigns:
    """Test suite for IngestCampaigns use case."""

    @pytest.fixture
    def sample_campaigns_data(self):
        """Sample campaign data for testing."""
        return [
            {
                "campaign_id": "CAMP001",
                "channel": "email",
                "cost": 1500.50,
            },
            {
                "campaign_id": "CAMP002",
                "channel": "sms",
                "cost": 800.00,
            },
        ]

    def test_execute_ingests_campaigns_to_target(self, sample_campaigns_data):
        # Given: A use case configured with in-memory adapters
        source = InMemoryDataSource(data=sample_campaigns_data)
        target = InMemoryDataTarget(data=[])
        dq_logger = InMemoryDataQualityLogger()
        use_case = IngestCampaigns(
            campaigns_source=source,
            campaigns_target=target,
            dataframe_engine=PandasDataFrameEngine,
            data_quality_logger=dq_logger,
        )

        # When: Executing the use case
        use_case.execute()

        # Then: Data should be written to target
        result_data = target._data_as_dataframe.collect()

        assert len(result_data) == 2
        assert result_data[0]["campaign_id"] == "CAMP001"
        assert result_data[0]["channel"] == "email"
        assert result_data[0]["cost"] == 1500.50
        assert result_data[1]["campaign_id"] == "CAMP002"
