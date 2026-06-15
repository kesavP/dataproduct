from abc import abstractmethod
from typing import Optional

from retail_data_product.application.ports import DataTarget
from retail_data_product.domain.data_governance import Dataframe, Schema


class DataTargetTestTemplate:
    @abstractmethod
    def create_data_target_with_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataTarget:
        """Create a data target with initial test data"""

    @abstractmethod
    def create_dataframe_from_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> Dataframe:
        """Create a dataframe from data for testing"""

    @abstractmethod
    def get_target_data(self, target: DataTarget) -> list[dict]:
        """Get the current data from the target"""

    def test_upsert_inserts_new_records(self):
        """Test that upsert inserts new records"""
        # Given
        initial_data = [{"id": 1, "name": "Alice", "value": 100}]
        target = self.create_data_target_with_data(initial_data)
        new_data = [{"id": 2, "name": "Bob", "value": 200}]
        df = self.create_dataframe_from_data(new_data)

        # When
        target.upsert(df, primary_key_column="id")

        # Then
        result = self.get_target_data(target)
        assert len(result) == 2
        ids = {r["id"] for r in result}
        assert ids == {1, 2}

    def test_upsert_updates_existing_records(self):
        """Test that upsert updates existing records"""
        # Given
        initial_data = [{"id": 1, "name": "Alice", "value": 100}]
        target = self.create_data_target_with_data(initial_data)
        updated_data = [{"id": 1, "name": "Alice", "value": 150}]
        df = self.create_dataframe_from_data(updated_data)

        # When
        target.upsert(df, primary_key_column="id")

        # Then
        result = self.get_target_data(target)
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["value"] == 150
