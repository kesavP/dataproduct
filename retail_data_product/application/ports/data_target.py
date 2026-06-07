from typing import Protocol

from ...domain.data_governance import Dataframe


class DataTarget(Protocol):
    """
    Data Target interface. Its role is to define how to save a Dataframe object into
    the environment (most likely a table).
    """

    def upsert(self, dataframe: Dataframe, primary_key_column: str) -> None:
        """
        Upsert the data to the target using the primary key identifier.

        Args:
            dataframe (Dataframe): Dataframe to upsert
            primary_key_column (str): column that will be used as primary key
        """
        ...
