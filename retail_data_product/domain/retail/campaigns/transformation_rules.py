from dataclasses import dataclass
from ...data_governance import Dataframe


@dataclass(frozen=True)
class CleanChannelRule:
    """Cleans marketing channel names by converting to lowercase and trimming spaces."""

    tfr_id: str = "clean_channel"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()
        df_backend.clean_string_column("channel")


@dataclass
class CalculateCampaignRevenueRule:
    """Calculates total revenue per campaign by summing order amounts."""

    orders_df: Dataframe
    tfr_id: str = "calculate_campaign_revenue"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()
        orders_backend = self.orders_df.get_backend_dataframe()

        # Join campaigns with orders
        df_backend.join(orders_backend, "campaign_id", "campaign_id", "left")

        # Aggregate revenue by campaign
        df_backend.group_by_and_aggregate(
            ["campaign_id", "channel", "cost", "start_date", "end_date"],
            {"total_amount_eur": "sum"},
        )
        df_backend.rename_column("total_amount_eur", "campaign_revenue")
