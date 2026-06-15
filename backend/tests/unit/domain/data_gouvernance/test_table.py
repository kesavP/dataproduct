import pytest

from retail_data_product.domain.data_governance import (
    TableDefinition,
    Schema,
    SchemaField,
    DataType,
)


class TestTableDefinition:
    """Tests for TableDefinition value object"""

    def test_schema_invalid_primary_key_raises_error(self):
        """Test that invalid primary key raises error"""
        with pytest.raises(
            ValueError, match="Primary key field 'not_in_table' not found"
        ):
            TableDefinition(
                name="test_table",
                schema=Schema(
                    fields=(
                        SchemaField(name="id", data_type=DataType.INTEGER),
                        SchemaField(name="name", data_type=DataType.STRING),
                    ),
                ),
                primary_key=("not_in_table",),
            )
