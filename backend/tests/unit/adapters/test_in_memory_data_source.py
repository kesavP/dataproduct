from typing import Optional, Type

from tests.adapters import InMemoryDataSource
from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.ports import DataFrameEngine, DataSource
from retail_data_product.domain.data_governance import Schema
from tests.templates.ports.test_data_sources import DataSourceTestTemplate


class TestInMemoryDataSource(DataSourceTestTemplate):
    """Unit tests for InMemoryDataSource using PandasDataFrameEngine"""

    def create_data_source_with_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataSource:
        """Create an in-memory data source with test data"""
        return InMemoryDataSource(data)

    def get_dataframe_engine(self) -> Type[DataFrameEngine]:
        """Get the dataframe engine to use for testing"""
        return PandasDataFrameEngine
