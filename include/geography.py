"""
Order-level geography enrichment.

Attaches real lat/lng to each order via the customer's zip code prefix,
joined against Olist's actual geolocation table. Multiple raw geolocation
points can share the same prefix — we take the centroid (average lat/lng)
per prefix rather than picking one arbitrarily. Orders with no matching
geolocation prefix get NULL lat/lng (LEFT JOIN), not dropped.
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


def build_order_geography(
    engine: Engine, source_schema: str = None, target_schema: str = None
) -> int:
    """Build order_geography: one row per order with centroid lat/lng.

    Returns the number of rows written.
    """
    _ensure_schema(engine, target_schema)

    orders = _qualify("orders", source_schema)
    customers = _qualify("customers", source_schema)
    geolocation = _qualify("geolocation", source_schema)

    query = f"""
        WITH geo_centroid AS (
            SELECT
                geolocation_zip_code_prefix AS zip_prefix,
                AVG(geolocation_lat)         AS lat,
                AVG(geolocation_lng)         AS lng
            FROM {geolocation}
            GROUP BY geolocation_zip_code_prefix
        )
        SELECT
            o.order_id,
            c.customer_unique_id,
            c.customer_zip_code_prefix AS zip_prefix,
            c.customer_city,
            c.customer_state,
            g.lat,
            g.lng
        FROM {orders} o
        JOIN      {customers}  c ON o.customer_id = c.customer_id
        LEFT JOIN geo_centroid g ON c.customer_zip_code_prefix = g.zip_prefix
    """

    df = pd.read_sql(query, engine)
    df.to_sql(
        "order_geography",
        con=engine,
        schema=target_schema,
        if_exists="replace",
        index=False,
    )
    logger.info("Built order_geography: %d rows", len(df))
    return len(df)
