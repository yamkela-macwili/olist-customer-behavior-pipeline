"""
Transform layer: build fact_orders and dim_customers from raw tables.

Reads via SQL (joins/aggregation done in the database, not in pandas, so the
logic matches what would run against the real Postgres raw tables), writes
the result back with pandas to_sql. This keeps the transform testable against
SQLite fixtures while staying close to how it will actually run in production
against Postgres.
"""

import logging

import pandas as pd
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def _qualify(table: str, schema: str = None) -> str:
    return f'"{schema}"."{table}"' if schema else table


def _ensure_schema(engine: Engine, schema: str) -> None:
    if schema and engine.dialect.name != "sqlite":
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def build_fact_orders(
    engine: Engine, source_schema: str = None, target_schema: str = None
) -> int:
    """Build fact_orders: one row per order, with order_items/payments
    aggregated (an order can have multiple line items and multiple payment
    entries) and reviews averaged. Uses LEFT JOINs deliberately — an order
    with no items/payments/reviews yet (e.g. just placed, or cancelled)
    should still appear with zeroed metrics, not be silently dropped.
    """
    _ensure_schema(engine, target_schema)

    orders = _qualify("orders", source_schema)
    items = _qualify("order_items", source_schema)
    payments = _qualify("order_payments", source_schema)
    reviews = _qualify("order_reviews", source_schema)

    query = f"""
        SELECT
            o.order_id,
            o.customer_id,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            COALESCE(oi.total_price, 0)   AS total_price,
            COALESCE(oi.total_freight, 0) AS total_freight,
            COALESCE(oi.item_count, 0)    AS item_count,
            COALESCE(p.total_payment, 0)  AS total_payment,
            r.avg_review_score
        FROM {orders} o
        LEFT JOIN (
            SELECT order_id,
                   SUM(price)         AS total_price,
                   SUM(freight_value) AS total_freight,
                   COUNT(*)           AS item_count
            FROM {items} GROUP BY order_id
        ) oi ON o.order_id = oi.order_id
        LEFT JOIN (
            SELECT order_id, SUM(payment_value) AS total_payment
            FROM {payments} GROUP BY order_id
        ) p ON o.order_id = p.order_id
        LEFT JOIN (
            SELECT order_id, AVG(review_score) AS avg_review_score
            FROM {reviews} GROUP BY order_id
        ) r ON o.order_id = r.order_id
    """

    df = pd.read_sql(query, engine)
    df.to_sql("fact_orders", con=engine, schema=target_schema, if_exists="replace", index=False)
    logger.info("Built fact_orders: %d rows", len(df))
    return len(df)


def build_dim_customers(
    engine: Engine, source_schema: str = None, target_schema: str = None
) -> int:
    """Build dim_customers: one row per customer_unique_id.

    Olist's customer_id is order-level (a new one per order); customer_unique_id
    is the actual repeat-customer identifier. Grouping by customer_unique_id
    collapses repeat customers to one row — without this, RFM/churn features
    would treat every order as a different customer.

    City/state are taken from the customer's MOST RECENT order, not picked
    arbitrarily. This is an explicit rule (assume their last known order
    reflects their current address), not a guess — see docs/DECISIONS.md.
    """
    _ensure_schema(engine, target_schema)

    customers = _qualify("customers", source_schema)
    orders = _qualify("orders", source_schema)

    query = f"""
        WITH ranked AS (
            SELECT
                c.customer_unique_id,
                c.customer_id,
                c.customer_city,
                c.customer_state,
                o.order_purchase_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY c.customer_unique_id
                    ORDER BY o.order_purchase_timestamp DESC
                ) AS rn
            FROM {customers} c
            LEFT JOIN {orders} o ON c.customer_id = o.customer_id
        ),
        counts AS (
            SELECT customer_unique_id,
                   COUNT(DISTINCT customer_id) AS distinct_order_accounts
            FROM {customers}
            GROUP BY customer_unique_id
        )
        SELECT
            r.customer_unique_id,
            r.customer_city,
            r.customer_state,
            co.distinct_order_accounts
        FROM ranked r
        JOIN counts co ON r.customer_unique_id = co.customer_unique_id
        WHERE r.rn = 1
    """

    df = pd.read_sql(query, engine)
    df.to_sql("dim_customers", con=engine, schema=target_schema, if_exists="replace", index=False)
    logger.info("Built dim_customers: %d rows", len(df))
    return len(df)
