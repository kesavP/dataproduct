from retail_data_product.adapters.pandas_dataframe_engine import PandasDataFrameEngine

from retail_data_product.application.ports import DataTarget

from retail_data_product.domain.data_governance import Dataframe


class InMemoryDataTarget(DataTarget):
    def __init__(self, data: list[dict]):
        """
        In-memory DataTarget implementation. Principaly used to do unit testing.

        Args:
            data (list[dict]): Initial data in the data target
        """
        self._data_as_dataframe = PandasDataFrameEngine.create_from_list_of_dict(data)

    def upsert(self, dataframe: Dataframe, primary_key_column: str):
        self._data_as_dataframe.concat(dataframe.get_backend_dataframe())
        self._data_as_dataframe.deduplicate_on_column(primary_key_column)
