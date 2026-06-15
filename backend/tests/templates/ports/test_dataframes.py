from abc import abstractmethod
from typing import Optional

from retail_data_product.application.ports import DataFrameEngine
from retail_data_product.domain.data_governance import Schema, DataType, SchemaField


class _DataFrameEngineBase:
    @abstractmethod
    def create_dataframe_from_list_of_dict(
        self, data: list[dict], schema: Optional[Schema] = None
    ) -> DataFrameEngine:
        pass


class FilterRowByCondition(_DataFrameEngineBase):
    """Test of the method create_from_list_of_dict of DataFrameEngine"""

    def test_filter_rows_by_condition_greater_than(self):
        """Test filtering rows with > operator"""
        # Given
        data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}, {"id": 3, "value": 30}]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        engine.filter_rows_by_condition("value", ">", 15)

        # Then
        result = engine.collect()
        assert len(result) == 2
        assert all(row["value"] > 15 for row in result)

    def test_filter_rows_by_condition_equals(self):
        """Test filtering rows with == operator"""
        # Given
        data = [
            {"id": 1, "status": "active"},
            {"id": 2, "status": "inactive"},
            {"id": 3, "status": "active"},
        ]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        engine.filter_rows_by_condition("status", "==", "active")

        # Then
        result = engine.collect()
        assert len(result) == 2
        assert all(row["status"] == "active" for row in result)

    def test_filter_rows_by_value_in_set(self):
        """Test filtering rows by values in a set"""
        # Given
        data = [
            {"id": 1, "category": "A"},
            {"id": 2, "category": "B"},
            {"id": 3, "category": "C"},
            {"id": 4, "category": "A"},
        ]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        engine.filter_rows_by_value_in_set("category", {"A", "C"})

        # Then
        result = engine.collect()
        assert len(result) == 3
        assert all(row["category"] in ["A", "C"] for row in result)


class CountRowsWhere(_DataFrameEngineBase):
    """Test of the method create_from_list_of_dict of DataFrameEngine"""

    def test_count_rows_where_equals(self):
        """Test counting rows with condition"""
        # Given
        data = [
            {"id": 1, "status": "active"},
            {"id": 2, "status": "inactive"},
            {"id": 3, "status": "active"},
        ]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        count = engine.count_rows_where("status", "==", "active")

        # Then
        assert count == 2

    def test_count_rows_where_in(self):
        """Test counting rows with 'in' operator"""
        # Given
        data = [
            {"id": 1, "category": "A"},
            {"id": 2, "category": "B"},
            {"id": 3, "category": "C"},
        ]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        count = engine.count_rows_where("category", "in", {"A", "C"})

        # Then
        assert count == 2


class SchemaOperation(_DataFrameEngineBase):
    def test_create_with_schema(self):
        """Test creating dataframe with schema enforcement"""
        # Given
        data = [
            {"id": "1", "name": "Alice", "extra": "ignored"},
            {"id": "2", "name": "Bob"},
        ]
        schema = Schema(
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
                SchemaField(name="age", data_type=DataType.INTEGER),
            )
        )

        # When
        engine = self.create_dataframe_from_list_of_dict(data, schema=schema)

        # Then
        result = engine.collect()
        # Should have 3 columns as defined in schema
        assert set(result[0].keys()) == {"id", "name", "age"}
        # ID should be converted to integer
        assert isinstance(result[0]["id"], int)
        # Age should be null (missing in data)
        assert result[0]["age"] is None

    def test_enforce_schema_type_conversion(self):
        """Test schema enforcement with type conversion"""
        # Given
        data = [{"id": "1", "value": "100.5"}, {"id": "2", "value": "200.3"}]
        engine = self.create_dataframe_from_list_of_dict(data)
        schema = Schema(
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="value", data_type=DataType.DOUBLE),
            )
        )

        # When
        engine.enforce_schema(schema)

        # Then
        result = engine.collect()
        assert isinstance(result[0]["id"], int)
        assert isinstance(result[0]["value"], float)
        assert result[0]["id"] == 1
        assert abs(result[0]["value"] - 100.5) < 0.01

    def test_enforce_schema_adds_missing_columns(self):
        """Test that schema enforcement adds missing columns"""
        # Given
        data = [{"id": 1}]
        schema = Schema(
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
                SchemaField(name="age", data_type=DataType.INTEGER),
            )
        )
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        engine.enforce_schema(schema)

        # Then
        result = engine.collect()
        assert "id" in result[0]
        assert "name" in result[0]
        assert "age" in result[0]
        assert result[0]["name"] is None
        assert result[0]["age"] is None

    def test_enforce_schema_removes_extra_columns(self):
        """Test that schema enforcement removes extra columns"""
        # Given
        data = [{"id": 1, "name": "Alice", "extra1": "val", "extra2": "val"}]
        schema = Schema(
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
            )
        )
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        engine.enforce_schema(schema)

        # Then
        result = engine.collect()
        assert set(result[0].keys()) == {"id", "name"}


class DataFrameEngineTestTemplate(
    FilterRowByCondition, CountRowsWhere, SchemaOperation
):
    def test_get_column_names(self):
        # Given
        data = [{"id": 1, "name": "Alice", "age": 30}]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When/Then
        assert set(engine.get_column_names()) == {"id", "name", "age"}
        assert len(engine.get_column_names()) == 3

    def test_clean_string_column(self):
        """Test cleaning string column (lowercase and trim)"""
        # Given
        data = [{"id": 1, "name": "  ALICE  "}, {"id": 2, "name": "Bob   "}]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        engine.clean_string_column("name")

        # Then
        result = engine.collect()
        assert result[0]["name"] == "alice"
        assert result[1]["name"] == "bob"

    def test_colums_contains_value(self):
        """Test checking if column contains a value"""
        # Given
        data = [{"id": 1, "status": "active"}, {"id": 2, "status": "inactive"}]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When/Then
        assert engine.colums_contains("status", "active")
        assert not engine.colums_contains("status", "pending")

    def test_colums_contains_null(self):
        """Test checking if column contains null values"""
        # Given
        data = [{"id": 1, "value": 10}, {"id": 2, "value": None}]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When/Then
        assert engine.colums_contains("value", None)

    def test_rename_column(self):
        """Test renaming a column"""
        # Given
        data = [{"old_name": 1, "value": 10}]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        engine.rename_column("old_name", "new_name")

        # Then
        result = engine.collect()
        assert "new_name" in result[0]
        assert "old_name" not in result[0]

    def test_deduplicate_on_column(self):
        """Test deduplication keeping last occurrence"""
        # Given
        data = [
            {"id": 1, "value": "first"},
            {"id": 2, "value": "unique"},
            {"id": 1, "value": "last"},
        ]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        engine.deduplicate_on_column("id")

        # Then
        result = engine.collect()
        assert len(result) == 2
        # Find the row with id=1
        id1_rows = [r for r in result if r["id"] == 1]
        assert len(id1_rows) == 1
        assert id1_rows[0]["value"] == "last"

    def test_group_by_and_aggregate(self):
        """Test group by with aggregations"""
        # Given
        data = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        engine.group_by_and_aggregate(
            group_columns=["category"], agg_dict={"value": "sum"}
        )

        # Then
        result = engine.collect()
        assert len(result) == 2
        result_dict = {r["category"]: r["value"] for r in result}
        assert result_dict["A"] == 30
        assert result_dict["B"] == 30

    def test_count_rows(self):
        """Test counting total rows"""
        # Given
        data = [{"id": 1}, {"id": 2}, {"id": 3}]
        engine = self.create_dataframe_from_list_of_dict(data)

        # When
        count = engine.count_rows()

        # Then
        assert count == 3

    def test_join_left(self):
        """Test left join"""
        # Given
        data1 = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        data2 = [{"id": 1, "city": "NYC"}, {"id": 3, "city": "LA"}]
        engine1 = self.create_dataframe_from_list_of_dict(data1)
        engine2 = self.create_dataframe_from_list_of_dict(data2)

        # When
        engine1.join(engine2, "id", "id", how="left")

        # Then
        result = engine1.collect()
        assert len(result) == 2
        assert result[0]["city"] == "NYC"
        assert result[1]["city"] is None

    def test_concat(self):
        """Test concatenating dataframes"""
        # Given
        data1 = [{"id": 1, "value": 10}]
        data2 = [{"id": 2, "value": 20}]
        engine1 = self.create_dataframe_from_list_of_dict(data1)
        engine2 = self.create_dataframe_from_list_of_dict(data2)

        # When
        engine1.concat(engine2)

        # Then
        result = engine1.collect()
        assert len(result) == 2
        assert any(r["id"] == 1 for r in result)
        assert any(r["id"] == 2 for r in result)
