import tempfile
from pathlib import Path
from typing import Optional, Type
import pytest

from retail_data_product.adapters import CSVDataSource, PandasDataFrameEngine
from retail_data_product.application.ports import DataFrameEngine, DataSource
from retail_data_product.domain.data_governance import Schema
from tests.templates.ports.test_data_sources import DataSourceTestTemplate


class TestCSVDataSource(DataSourceTestTemplate):
    """Unit tests for CSVDataSource using PandasDataFrameEngine"""

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test"""
        self.temp_dir = tempfile.mkdtemp()
        yield
        # Cleanup
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_data_source_with_data(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataSource:
        """Create a CSV data source with test data"""
        import pandas as pd

        # Write data to CSV
        csv_path = Path(self.temp_dir) / "test_data.csv"

        if not data:
            # Create empty CSV with headers for empty data case
            with open(csv_path, "w") as f:
                if not schema:
                    f.write("id,name,age\n")
                else:
                    f.write(",".join([f.name for f in schema.fields]) + "\n")
        else:
            df = pd.DataFrame(data)
            df.to_csv(csv_path, index=False)

        return CSVDataSource(str(csv_path))

    def get_dataframe_engine(self) -> Type[DataFrameEngine]:
        """Get the dataframe engine to use for testing"""
        return PandasDataFrameEngine

    def test_csv_file_not_found(self):
        """Test handling of non-existent CSV file"""
        # Given
        source = CSVDataSource("/nonexistent/path/file.csv")

        # When/Then
        with pytest.raises(FileNotFoundError):
            source.fetch(PandasDataFrameEngine)
