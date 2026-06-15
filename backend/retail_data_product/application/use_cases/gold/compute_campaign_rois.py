from typing import Type, Sequence

from ...ports import DataSource, DataTarget, DataFrameEngine, DataQualityLogger
from ..base_etl_use_case import BaseETLUseCase

from ....domain.data_governance import Dataframe, TableDefinition, DataQualityRule
from ....domain.retail.analytics import (
    CAMPAIGN_ROIS,
    RoiValueBoundsRule,
    AvailableDataRule,
    CalculateRoiRule,
)


class ComputeCampaignROIs(BaseETLUseCase):
    """
    Gold Layer: Calculate Return on Investment (ROI) for marketing campaigns.

    This use case computes campaign ROI metrics and validates that values
    are within reasonable bounds (flagging outliers for review).
    """

    def __init__(
        self,
        campaigns_enriched_source: DataSource,
        campaign_rois_target: DataTarget,
        dataframe_engine: Type[DataFrameEngine],
        data_quality_logger: DataQualityLogger,
    ):
        """
        Args:
            campaigns_enriched_source (DataSource): silver.campaign_enriched table
            campaign_rois_target (DataTarget): gold.campaign_rois table
            dataframe_engine (Type[DataFrameEngine]): data processing engine
            data_quality_logger (DataQualityLogger): logger for data quality results
        """
        super().__init__(campaign_rois_target, dataframe_engine, data_quality_logger)
        self._campaigns_enriched_source = campaigns_enriched_source

    def fetch_data(self) -> Dataframe:
        return self._campaigns_enriched_source.fetch(self._dataframe_engine)

    def get_target_table(self) -> TableDefinition:
        return CAMPAIGN_ROIS

    def get_quality_rules(self) -> Sequence[DataQualityRule]:
        return (AvailableDataRule(), RoiValueBoundsRule())

    def get_transformation_rules(self) -> Sequence:
        return (CalculateRoiRule(),)
