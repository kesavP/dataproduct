from typing import Protocol, Type

from ...domain.data_governance import Dataframe
from .dataframe_engine import DataFrameEngine


class DataSource(Protocol):
    """
    Data source interface. It is responsible to generate Dataframe objects from
    the environment (file in a landing, external tables, APIs, ...).
    """

    def fetch(
        self,
        dataframe_engine: Type["DataFrameEngine"],
    ) -> Dataframe:
        """
        Fetch a Dataframe from the source.

        Args:
            dataframe_engine (Type[&quot;DataFrameEngine&quot;]): Engine to be used to
            get the Dataframe

        Returns:
            Dataframe: Dataframe object
        """
        ...
