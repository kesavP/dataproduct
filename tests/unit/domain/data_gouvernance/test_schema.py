import pytest

from retail_data_product.domain.data_governance import Schema, SchemaField, DataType


class TestSchemaField:
    """Tests for SchemaField value object"""

    def test_schema_field_creation(self):
        """Test creating a schema field"""
        field = SchemaField(
            name="user_id",
            data_type=DataType.INTEGER,
            description="Unique user identifier",
        )

        assert field.name == "user_id"
        assert field.data_type == DataType.INTEGER
        assert field.description == "Unique user identifier"


class TestSchema:
    """Tests for Schema value object"""

    def test_schema_creation(self):
        """Test creating a schema"""
        schema = Schema(
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )
        assert len(schema.fields) == 2

    def test_schema_duplicate_field_names_raises_error(self):
        """Test that duplicate field names raise error"""
        with pytest.raises(ValueError):
            Schema(
                fields=(
                    SchemaField(name="id", data_type=DataType.INTEGER),
                    SchemaField(name="id", data_type=DataType.STRING),
                )
            )
