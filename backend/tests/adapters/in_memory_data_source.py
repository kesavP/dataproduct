from typing import Type

from retail_data_product.adapters.pandas_dataframe_engine import PandasDataFrameEngine

from retail_data_product.application.ports import DataSource, DataFrameEngine

from retail_data_product.domain.data_governance import Dataframe


class InMemoryDataSource(DataSource):
    def __init__(self, data: list[dict]):
        """
        In-memory DataSource implementation. Principaly used to do unit testing.

        Args:
            data (list[dict]): data in the source
        """
        self._data_as_dataframe = PandasDataFrameEngine.create_from_list_of_dict(data)

    def fetch(
        self,
        dataframe_engine: Type["DataFrameEngine"],
    ) -> Dataframe:
        if dataframe_engine == PandasDataFrameEngine:
            backend_df = self._data_as_dataframe
        else:
            backend_df = dataframe_engine.create_from_list_of_dict(
                self._data_as_dataframe.collect(), self._data_as_dataframe.get_schema()
            )
        return Dataframe(backend_dataframe=backend_df)
