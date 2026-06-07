from dataclasses import dataclass
from ...data_governance import Dataframe


@dataclass(frozen=True)
class CleanPaymentStatusRule:
    """Cleans payment_status by converting to lowercase and trimming spaces."""

    tfr_id: str = "clean_payment_status"

    def apply(self, dataframe: Dataframe) -> None:
        df_backend = dataframe.get_backend_dataframe()
        df_backend.clean_string_column("payment_status")
