from ...data_governance import (
    TableDefinition,
    Schema,
    SchemaField,
    DataType,
)


PAYMENTS_RAW = TableDefinition(
    name="payments_raw",
    description="Raw payments from Payment Gateway.",
    schema=Schema(
        fields=(
            SchemaField(
                name="payment_id",
                data_type=DataType.STRING,
                description="Unique payment transaction identifier",
            ),
            SchemaField(
                name="order_id",
                data_type=DataType.STRING,
                description="Order associated with this payment",
            ),
            SchemaField(
                name="payment_status",
                data_type=DataType.STRING,
                description="Payment status (success, failed, pending, refunded)",
            ),
            SchemaField(
                name="payment_date",
                data_type=DataType.DATETIME,
                description="When the payment was processed",
            ),
        ),
    ),
    primary_key=("payment_id",),
)


PAYMENTS_CLEANED = TableDefinition(
    name="payments_cleaned",
    description="""
    Cleaned payment data with standardized status values.
    - Status values normalized to lowercase
    - Whitespace removed
    - Valid statuses: success, failed, refunded
    """,
    schema=Schema(
        fields=(
            SchemaField(
                name="payment_id",
                data_type=DataType.STRING,
                description="Unique payment transaction identifier",
            ),
            SchemaField(
                name="order_id",
                data_type=DataType.STRING,
                description="Order associated with this payment",
            ),
            SchemaField(
                name="payment_status",
                data_type=DataType.STRING,
                description="Cleaned payment status: success, failed, or refunded",
            ),
            SchemaField(
                name="payment_date",
                data_type=DataType.DATETIME,
                description="When the payment was processed",
            ),
        ),
    ),
    primary_key=("payment_id",),
)
