"""
Feature layer: time-split cutoff, RFM, delivery experience.

All feature functions accept a `cutoff` timestamp and operate only on data
on/before that boundary. This is the core leakage guard: features must not
see any information that postdates what would have been known at prediction
time.
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


def compute_time_split_cutoff(
    engine: Engine, source_schema: str = None, quantile: float = 0.8
) -> pd.Timestamp:
    """Return the `quantile`-th percentile timestamp of all order dates.

    Data-driven cutoff — not a hardcoded calendar date — derived from the
    actual span of the dataset. quantile=0.8 means ~80% of order history
    becomes the observation window (features), ~20% becomes the outcome
    window (churn label ground truth). See docs/DECISIONS.md.
    """
    orders = _qualify("orders", source_schema)
    df = pd.read_sql(f"SELECT order_purchase_timestamp FROM {orders}", engine)
    timestamps = pd.to_datetime(df["order_purchase_timestamp"])
    cutoff = timestamps.quantile(quantile)
    logger.info("Time-split cutoff at quantile=%.2f: %s", quantile, cutoff)
    return cutoff


def _load_orders_with_customers(
    engine: Engine, source_schema: str = None
) -> pd.DataFrame:
    orders = _qualify("orders", source_schema)
    customers = _qualify("customers", source_schema)
    query = f"""
        SELECT o.*, c.customer_unique_id
        FROM {orders} o
        JOIN {customers} c ON o.customer_id = c.customer_id
    """
    df = pd.read_sql(query, engine)
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    return df


def compute_rfm(
    engine: Engine,
    cutoff: pd.Timestamp,
    source_schema: str = None,
    target_schema: str = None,
) -> int:
    """Recency, Frequency, Monetary per customer_unique_id, using only orders
    on/before `cutoff`. Monetary comes from fact_orders.total_price (already
    aggregated across order_items — see include/transform.py).
    """
    _ensure_schema(engine, target_schema)

    fact = _qualify("fact_orders", target_schema)
    orders_df = _load_orders_with_customers(engine, source_schema)
    fact_df = pd.read_sql(f"SELECT order_id, total_price FROM {fact}", engine)

    merged = orders_df.merge(fact_df, on="order_id", how="left")
    obs = merged[merged["order_purchase_timestamp"] <= cutoff]

    rfm = obs.groupby("customer_unique_id").agg(
        last_order_date=("order_purchase_timestamp", "max"),
        frequency=("order_id", "nunique"),
        monetary=("total_price", "sum"),
    ).reset_index()
    rfm["recency_days"] = (cutoff - rfm["last_order_date"]).dt.days
    rfm = rfm.drop(columns=["last_order_date"])

    rfm.to_sql("customer_rfm", con=engine, schema=target_schema, if_exists="replace", index=False)
    logger.info("Computed RFM for %d customers (cutoff: %s)", len(rfm), cutoff)
    return len(rfm)


def compute_delivery_experience(
    engine: Engine,
    cutoff: pd.Timestamp,
    source_schema: str = None,
    target_schema: str = None,
) -> int:
    """Average delivery delay and late-delivery rate per customer, using only
    delivered orders on/before `cutoff`. Orders never delivered are excluded
    entirely, not counted as on-time — an undelivered order has no delay to
    measure; treating it as 0 would understate how often delivery fails.
    """
    _ensure_schema(engine, target_schema)

    df = _load_orders_with_customers(engine, source_schema)
    df = df[df["order_purchase_timestamp"] <= cutoff]
    df = df.dropna(subset=["order_delivered_customer_date"])

    if len(df) == 0:
        empty = pd.DataFrame(
            columns=["customer_unique_id", "avg_delay_days", "late_delivery_rate"]
        )
        empty.to_sql(
            "customer_delivery_experience",
            con=engine,
            schema=target_schema,
            if_exists="replace",
            index=False,
        )
        logger.warning("No delivered orders found on/before cutoff %s", cutoff)
        return 0

    df["order_delivered_customer_date"] = pd.to_datetime(df["order_delivered_customer_date"])
    df["order_estimated_delivery_date"] = pd.to_datetime(df["order_estimated_delivery_date"])
    df["delay_days"] = (
        df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]
    ).dt.days
    df["is_late"] = df["delay_days"] > 0

    result = df.groupby("customer_unique_id").agg(
        avg_delay_days=("delay_days", "mean"),
        late_delivery_rate=("is_late", "mean"),
    ).reset_index()

    result.to_sql(
        "customer_delivery_experience",
        con=engine,
        schema=target_schema,
        if_exists="replace",
        index=False,
    )
    logger.info(
        "Computed delivery experience for %d customers (cutoff: %s)", len(result), cutoff
    )
    return len(result)
