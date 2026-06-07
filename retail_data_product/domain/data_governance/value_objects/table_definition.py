from dataclasses import dataclass
from enum import Enum
from typing import Optional


# =============================================================================
# Data Types
# =============================================================================


class DataType(Enum):
    """
    Enumeration of supported data types for schema fields.

    Inherits from str to maintain string compatibility for serialization.
    """

    STRING = "string"
    INTEGER = "integer"
    DOUBLE = "double"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"

    def __str__(self) -> str:
        """Returns the string value of the enum."""
        return self.value


# =============================================================================
# Schema
# =============================================================================


@dataclass(frozen=True)
class SchemaField:
    """
    Value object representing a single column/field definition in a schema.

    Immutable specification of a column's structure and constraints.

    Attributes:
        name (str): Name of the column
        data_type (DataType): Expected data type for the column
        description (str): Optional description of the column
    """

    name: str
    data_type: DataType
    description: Optional[str] = None
    nullable: bool = True


@dataclass(frozen=True)
class Schema:
    """
    Value object representing the complete structure of a table or dataframe.

    Immutable specification of all fields/columns and their constraints.
    Two schemas with the same fields are considered equal.

    Attributes:
        fields (tuple[SchemaField, ...]): Fields that compose the schema

    Raises:
        ValueError: If a fields have duplicate name
    """

    fields: tuple[SchemaField, ...]

    def __post_init__(self):
        """Validate schema definition."""
        field_names = [f.name for f in self.fields]
        if len(field_names) != len(set(field_names)):
            duplicates = [name for name in field_names if field_names.count(name) > 1]
            raise ValueError(f"Duplicate field names in schema: {duplicates}")

    def get_field(self, field_name: str) -> Optional[SchemaField]:
        """
        Get a field by name.

        Args:
            field_name (str): Name of the field to retrieve

        Returns:
            SchemaField or None: The field if found, None otherwise
        """
        for field in self.fields:
            if field.name == field_name:
                return field
        return None

    def __str__(self) -> str:
        """Returns a string representation of the schema."""
        fields_str = ", ".join(f"{f.name}:{f.data_type}" for f in self.fields)
        return f"Schema({fields_str})"

    def __repr__(self) -> str:
        """Detailed representation of schema."""
        return self.__str__()


# =============================================================================
# Table Definition
# =============================================================================


@dataclass(frozen=True)
class TableDefinition:
    """
    Describes the expected structure and quality rules for a dataset.

    This is a Value Object, not an Entity or Aggregate.
    It defines WHAT a table should look like, not the table itself.

    Attributes:
        name: Unique table name identifier
        schema: Expected schema for the table
        primary_key: Tuple of column names forming the primary key
        description: Human-readable description of the table
    """

    name: str
    schema: Schema
    primary_key: tuple[str, ...] = ()
    description: str = ""

    def __post_init__(self):
        """Validate table definition."""
        if self.primary_key:
            field_names = {f.name for f in self.schema.fields}
            for pk_field in self.primary_key:
                if pk_field not in field_names:
                    raise ValueError(
                        f"Primary key field '{pk_field}' not found in schema"
                    )
