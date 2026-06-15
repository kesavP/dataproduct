from typing import Optional
from tests.adapters import InMemoryDataTarget
from retail_data_product.adapters import PandasDataFrameEngine
from retail_data_product.application.ports import DataTarget
from retail_data_product.domain.data_governance import Dataframe, Schema
from tests.templates.ports.test_data_targets import DataTargetTestTemplate


class TestInMemoryDataTarget(DataTargetTestTemplate):
    """Unit tests for InMemoryDataTarget using PandasDataFrameEngine"""

    def create_data_target_with_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataTarget:
        """Create an in-memory data target with initial test data"""
        return InMemoryDataTarget(data)

    def create_dataframe_from_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> Dataframe:
        """Create a dataframe from data for testing"""
        engine = PandasDataFrameEngine.create_from_list_of_dict(data)
        return Dataframe(backend_dataframe=engine)

    def get_target_data(self, target: DataTarget) -> list[dict]:
        """Get the current data from the target"""
        return target._data_as_dataframe.collect()  # pyright: ignore[reportAttributeAccessIssue]
