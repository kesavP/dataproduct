from typing import Optional


from tests.templates.ports.test_dataframes import DataFrameEngineTestTemplate

from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.ports import DataFrameEngine
from retail_data_product.domain.data_governance import Schema


class TestPandasDataFrameEngine(DataFrameEngineTestTemplate):
    def create_dataframe_from_list_of_dict(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataFrameEngine:
        return PandasDataFrameEngine.create_from_list_of_dict(data, schema)
