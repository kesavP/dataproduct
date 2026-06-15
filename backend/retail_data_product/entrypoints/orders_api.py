"""REST API for querying orders_cleaned table."""

from flask import Flask, jsonify, request
from pyspark.sql import SparkSession

app = Flask(__name__)
spark = SparkSession.builder.appName("orders_api").getOrCreate()


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"})


@app.route('/api/orders', methods=['GET'])
def get_orders():
    """Get cleaned orders with optional filters.

    Query params:
    - limit: number of rows (default 100)
    - offset: starting row (default 0)
    """
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)

    df = spark.sql(
        f"SELECT * FROM main.retail.orders_cleaned LIMIT {limit} OFFSET {offset}"
    )

    rows = df.collect()
    return jsonify({
        "count": len(rows),
        "orders": [row.asDict() for row in rows]
    })


@app.route('/api/orders/<order_id>', methods=['GET'])
def get_order(order_id):
    """Get specific order by ID."""
    df = spark.sql(
        f"SELECT * FROM main.retail.orders_cleaned WHERE order_id = '{order_id}'"
    )

    rows = df.collect()
    if not rows:
        return jsonify({"error": "Order not found"}), 404

    return jsonify(rows[0].asDict())


@app.route('/api/orders/customer/<customer_name>', methods=['GET'])
def get_orders_by_customer(customer_name):
    """Get all orders for a customer."""
    df = spark.sql(
        f"SELECT * FROM main.retail.orders_cleaned WHERE customer_name = '{customer_name}'"
    )

    rows = df.collect()
    return jsonify({
        "customer": customer_name,
        "count": len(rows),
        "orders": [row.asDict() for row in rows]
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
