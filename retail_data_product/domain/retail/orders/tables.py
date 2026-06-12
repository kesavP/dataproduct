from ...data_governance import (
    TableDefinition,
    Schema,
    SchemaField,
    DataType,
)


ORDERS_RAW = TableDefinition(
    name="clickstream",
    description="Raw clickstream orders from E-commerce platform.",
    schema=Schema(
        fields=(
            SchemaField(
                name="order_id",
                data_type=DataType.STRING,
                description="Unique identifier for the order",
            ),
            SchemaField(
                name="order_date",
                data_type=DataType.DATE,
                description="Date when the order was placed",
            ),
            SchemaField(
                name="customer_id",
                data_type=DataType.STRING,
                description="Customer who placed the order",
            ),
            SchemaField(
                name="total_amount",
                data_type=DataType.DOUBLE,
                description="Total order amount in original currency",
            ),
            SchemaField(
                name="currency",
                data_type=DataType.STRING,
                description="Currency code (USD, EUR, etc.)",
            ),
            SchemaField(
                name="campaign_id",
                data_type=DataType.STRING,
                description="Marketing campaign that led to this order",
            ),
        ),
    ),
    primary_key=("order_id",),
)


ORDERS_ENRICHED = TableDefinition(
    name="orders_enriched",
    description="""
    Enriched orders with:
    - Currency conversion to EUR
    - Joined payment status
    - Positive amount validation
    """,
    schema=Schema(
        fields=(
            SchemaField(
                name="order_id",
                data_type=DataType.STRING,
                description="Unique identifier for the order",
            ),
            SchemaField(
                name="order_date",
                data_type=DataType.DATE,
                description="Date when the order was placed",
            ),
            SchemaField(
                name="customer_id",
                data_type=DataType.STRING,
                description="Customer who placed the order",
            ),
            SchemaField(
                name="total_amount_eur",
                data_type=DataType.DOUBLE,
                description="Total order amount converted to EUR",
            ),
            SchemaField(
                name="campaign_id",
                data_type=DataType.STRING,
                description="Marketing campaign that led to this order",
            ),
            SchemaField(
                name="payment_status",
                data_type=DataType.STRING,
                description="Payment status from joined payments table",
            ),
        ),
    ),
    primary_key=("order_id",),
)


ORDERS_CLEANED = TableDefinition(
    name="orders_cleaned",
    description="""
    Cleaned orders for the silver layer:
    - Duplicate orders collapsed, keeping the latest order_date
    - Enriched with customer name and payment status from clickstream data
    """,
    schema=Schema(
        fields=(
            SchemaField(
                name="order_id",
                data_type=DataType.STRING,
                description="Unique identifier for the order",
            ),
            SchemaField(
                name="order_date",
                data_type=DataType.DATE,
                description="Date when the order was placed",
            ),
            SchemaField(
                name="customer_name",
                data_type=DataType.STRING,
                description="Customer name from clickstream data",
            ),
            SchemaField(
                name="total_amount_eur",
                data_type=DataType.DOUBLE,
                description="Total order amount converted to EUR",
            ),
            SchemaField(
                name="campaign_id",
                data_type=DataType.STRING,
                description="Marketing campaign that led to this order",
            ),
            SchemaField(
                name="payment_status",
                data_type=DataType.STRING,
                description="Payment status from clickstream data",
            ),
            SchemaField(
                name="product_id",
                data_type=DataType.STRING,
                description="Product identifier from clickstream data",
            ),
        ),
    ),
    primary_key=("order_id",),
)


ORDERS_TRANSFORMATION_DLQ = TableDefinition(
    name="orders_transformation_dlq",
    description="""
    Dead Letter Queue for orders that fail transformation in the silver layer.
    Records stored here failed during enrichment/deduplication and could not be
    written to orders_cleaned. Includes the original record and error details.
    """,
    schema=Schema(
        fields=(
            SchemaField(
                name="order_id",
                data_type=DataType.STRING,
                description="Unique identifier for the order (if extractable from record)",
            ),
            SchemaField(
                name="transformation_rule_id",
                data_type=DataType.STRING,
                description="Identifier of the transformation rule that failed",
            ),
            SchemaField(
                name="error_message",
                data_type=DataType.STRING,
                description="Error message from the transformation failure",
            ),
            SchemaField(
                name="error_type",
                data_type=DataType.STRING,
                description="Type of error (e.g., KeyError, AttributeError, ValueError)",
            ),
            SchemaField(
                name="original_record",
                data_type=DataType.STRING,
                description="JSON-serialized original record that failed",
            ),
            SchemaField(
                name="captured_at",
                data_type=DataType.STRING,
                description="ISO timestamp when the record was captured to DLQ",
            ),
        ),
    ),
    primary_key=None,
)
