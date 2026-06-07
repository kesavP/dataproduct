import tempfile
import shutil
from typing import Optional, Type
import pytest

from retail_data_product.adapters import DeltaDataSource, SparkDataFrameEngine
from retail_data_product.application.ports import DataFrameEngine, DataSource
from retail_data_product.domain.data_governance import Schema
from tests.templates.ports.test_data_sources import DataSourceTestTemplate


class TestDeltaDataSource(DataSourceTestTemplate):
    """Integration tests for DeltaDataSource using SparkDataFrameEngine"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self, spark):
        """Setup and teardown for each test"""
        self.spark = spark
        self.temp_dir = tempfile.mkdtemp(prefix="delta_test_source_")
        yield
        # Cleanup
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_data_source_with_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataSource:
        """Create a Delta data source with test data"""
        # First write data to Delta table
        if data:  # Only write if there's data
            engine = SparkDataFrameEngine.create_from_list_of_dict(data, schema)
            engine.sdf.write.format("delta").mode("overwrite").save(self.temp_dir)
        else:
            # Create empty Delta table with schema
            from pyspark.sql.types import (
                StructType,
                StructField,
                IntegerType,
                StringType,
            )

            empty_schema = StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),
                ]
            )
            empty_df = self.spark.createDataFrame([], empty_schema)
            empty_df.write.format("delta").mode("overwrite").save(self.temp_dir)

        return DeltaDataSource(self.temp_dir, spark_session=self.spark)

    def get_dataframe_engine(self) -> Type[DataFrameEngine]:
        """Get the dataframe engine to use for testing"""
        return SparkDataFrameEngine

    def test_fetch_nonexistent_table(self):
        """Test fetching from non-existent Delta table"""
        # Given
        nonexistent_path = tempfile.mkdtemp(prefix="delta_nonexistent_")
        source = DeltaDataSource(nonexistent_path, spark_session=self.spark)

        # When/Then
        try:
            with pytest.raises(Exception):  # Delta will raise an error
                source.fetch(SparkDataFrameEngine)
        finally:
            shutil.rmtree(nonexistent_path, ignore_errors=True)
