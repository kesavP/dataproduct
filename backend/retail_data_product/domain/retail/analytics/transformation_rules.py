from dataclasses import dataclass
from ...data_governance import Dataframe


@dataclass
class AggregateDailySalesRule:
    """Aggregates orders into daily gross sales."""

    date_column: str = "order_date"
    amount_column: str = "total_amount_eur"
    tfr_id: str = "aggregate_daily_sales"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()

        # Add a count column before grouping
        df_backend.add_column_from_expression("order_count", lambda df: 1)

        # Group by date and aggregate
        df_backend.group_by_and_aggregate(
            group_columns=[self.date_column],
            agg_dict={
                self.amount_column: "sum",
                "order_count": "sum",
            },
        )
        df_backend.rename_column(self.amount_column, "gross_sales_eur")


@dataclass
class FillMissingDatesRule:
    """Fills missing dates in a time series with default values."""

    date_column: str = "order_date"
    fill_value: float = 0.0
    tfr_id: str = "fill_missing_dates"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()

        # Generate date range and fill missing
        df_backend.fill_missing_dates(
            date_column=self.date_column,
            fill_value={"gross_sales_eur": self.fill_value, "order_count": 0},
        )


@dataclass
class CalculateRoiRule:
    """Calculates ROI from cost and revenue columns."""

    cost_column: str = "cost"
    revenue_column: str = "campaign_revenue"
    tfr_id: str = "calculate_roi"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()

        # ROI = (revenue - cost) / cost * 100
        df_backend.add_column_from_expression(
            "roi",
            lambda df: (df[self.revenue_column] - df[self.cost_column])
            / df[self.cost_column]
            * 100,
        )
