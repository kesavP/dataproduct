from typing import Type, Sequence

from ...ports import DataSource, DataTarget, DataFrameEngine, DataQualityLogger
from ..base_etl_use_case import BaseETLUseCase

from ....domain.data_governance import Dataframe, TableDefinition, DataQualityRule
from ....domain.retail.campaigns import CAMPAIGNS_RAW, CampaignIdNotNull


class IngestCampaigns(BaseETLUseCase):
    """
    Bronze Layer: Ingest raw campaign data from external source.

    This use case handles the ingestion of marketing campaign data into
    the bronze layer, performing critical data quality validations.
    """

    def __init__(
        self,
        campaigns_source: DataSource,
        campaigns_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        data_quality_logger: DataQualityLogger,
    ):
        """
        Args:
            campaigns_source (DataSource): data source for the campaign raw data
            campaigns_target (DataTarget): data target where to save the raw campaigns table
            dataframe_engine (Type[DataFrameEngine]): data processing engine
            data_quality_logger (DataQualityLogger): logger for data quality results
        """
        super().__init__(campaigns_target, dataframe_engine, data_quality_logger)
        self._campaigns_source = campaigns_source

    def fetch_data(self) -> Dataframe:
        return self._campaigns_source.fetch(self._dataframe_engine)

    def get_target_table(self) -> TableDefinition:
        return CAMPAIGNS_RAW

    def get_quality_rules(self) -> Sequence[DataQualityRule]:
        return (CampaignIdNotNull(),)
