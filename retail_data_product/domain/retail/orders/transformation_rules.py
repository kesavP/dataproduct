from dataclasses import dataclass
from ...data_governance import Dataframe


@dataclass
class ConvertCurrencyRule:
    """Converts order amounts to EUR based on provided exchange rates."""

    exchange_rates: dict[str, float]
    tfr_id: str = "convert_currency"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()

        def convert_to_eur(df):
            return df.apply(
                lambda row: row["total_amount"]
                * self.exchange_rates.get(row["currency"], 1.0),
                axis=1,
            )

        df_backend.add_column_from_expression("total_amount_eur", convert_to_eur)


@dataclass
class JoinPaymentsRule:
    """Joins order data with payment information."""

    payments_df: Dataframe
    tfr_id: str = "join_payments"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()
        payments_backend = self.payments_df.get_backend_dataframe()
        df_backend.join(payments_backend, "order_id", "order_id", "left")


@dataclass
class FormatCustomerIdRule:
    """Formats customer_id to match customers reference format (CUST-{id}).

    Only prepends CUST- if not already present.
    """

    tfr_id: str = "format_customer_id"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()

        def format_id(df):
            # For Spark DataFrames
            if hasattr(df, 'withColumn'):
                import pyspark.sql.functions as F
                return df.withColumn(
                    "customer_id",
                    F.when(
                        F.col("customer_id").cast("string").startswith("CUST-"),
                        F.col("customer_id").cast("string")
                    ).otherwise(
                        F.concat(F.lit("CUST-"), F.col("customer_id").cast("string"))
                    )
                )
            # For Pandas DataFrames
            else:
                df["customer_id"] = df["customer_id"].astype(str).apply(
                    lambda x: x if x.startswith("CUST-") else f"CUST-{x}"
                )
                return df

        df_backend.sdf = format_id(df_backend.sdf)


@dataclass
class JoinCustomersRule:
    """
    Enriches orders with the customer name from the customers reference data.

    Left-joins on ``customer_id`` so every order keeps its row even when the
    customer is unknown. The customers reference may carry more than one record
    per ``customer_id``, so it is deduplicated first to avoid fanning out orders.
    """

    customers_df: Dataframe
    tfr_id: str = "join_customers"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()
        customers_backend = self.customers_df.get_backend_dataframe()
        customers_backend.deduplicate_on_column("customer_id")
        df_backend.join(customers_backend, "customer_id", "customer_id", "left")
