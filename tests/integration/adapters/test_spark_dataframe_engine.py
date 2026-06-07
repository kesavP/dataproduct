from typing import Optional
import pytest

from retail_data_product.adapters import SparkDataFrameEngine
from retail_data_product.application.ports import DataFrameEngine
from retail_data_product.domain.data_governance import Schema
from tests.templates.ports.test_dataframes import DataFrameEngineTestTemplate


class TestSparkDataFrameEngine(DataFrameEngineTestTemplate):
    @pytest.fixture(autouse=True)
    def setup(self, spark):
        """Automatically inject spark session as instance attribute."""
        self.spark = spark

    def create_dataframe_from_list_of_dict(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataFrameEngine:
        return SparkDataFrameEngine.create_from_list_of_dict(data, schema)
