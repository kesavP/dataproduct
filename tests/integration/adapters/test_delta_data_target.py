import tempfile
import shutil
from typing import Optional
import pytest

from retail_data_product.adapters import (
    DeltaDataTarget,
    DeltaDataSource,
    SparkDataFrameEngine,
)
from retail_data_product.application.ports import DataTarget
from retail_data_product.domain.data_governance import Dataframe, Schema
from tests.templates.ports.test_data_targets import DataTargetTestTemplate


class TestDeltaDataTarget(DataTargetTestTemplate):
    """Integration tests for DeltaDataTarget using SparkDataFrameEngine"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, spark):
        """Setup and teardown for each test"""
        self.spark = spark
        self.temp_dir = tempfile.mkdtemp(prefix="delta_test_target_")
        yield
        # Cleanup
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_data_target_with_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataTarget:
        """Create a Delta data target with initial test data"""
        target = DeltaDataTarget(self.temp_dir, spark_session=self.spark)

        # Write initial data if provided
        if data:
            df = self.create_dataframe_from_data(data, schema)
            target.upsert(df, primary_key_column="id")

        return target

    def create_dataframe_from_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> Dataframe:
        """Create a dataframe from data for testing"""
        engine = SparkDataFrameEngine.create_from_list_of_dict(data, schema)
        return Dataframe(backend_dataframe=engine)

    def get_target_data(self, target: DataTarget) -> list[dict]:
        """Get the current data from the target"""
        # Read back from Delta table
        source = DeltaDataSource(target.path, spark_session=self.spark)  # pyright: ignore[reportAttributeAccessIssue]
        df = source.fetch(SparkDataFrameEngine)
        return df.collect()
