from abc import abstractmethod
from typing import Optional, Type

from retail_data_product.application.ports import DataFrameEngine, DataSource
from retail_data_product.domain.data_governance import Schema


class DataSourceTestTemplate:
    @abstractmethod
    def create_data_source_with_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataSource:
        """Create a data source with test data"""

    @abstractmethod
    def get_dataframe_engine(self) -> Type[DataFrameEngine]:
        """Get the dataframe engine to use for testing"""

    def test_fetch(self):
        # Given
        data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        source = self.create_data_source_with_data(data)
        engine = self.get_dataframe_engine()

        # When
        result = source.fetch(engine)

        # Then
        collected = result.collect()
        assert len(collected) == 3
        assert all(key in collected[0] for key in ["id", "name", "age"])

    def test_fetch_empty_data(self):
        # Given
        data = []
        source = self.create_data_source_with_data(data)
        engine = self.get_dataframe_engine()

        # When
        result = source.fetch(engine)

        # Then
        collected = result.collect()
        assert len(collected) == 0
