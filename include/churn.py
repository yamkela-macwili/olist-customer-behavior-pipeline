"""
Churn label: ground truth from actual future behavior, not a threshold rule.

churned = 1 if a customer ordered on/before `cutoff` but placed no order
after it. churned = 0 if they ordered again after the cutoff. Customers
with no orders on/before the cutoff are excluded entirely — there's no
observation-window history to predict from.

This is the standard approach for churn labeling when there's no explicit
subscription/cancellation signal in the data: a time-based holdout. It's
deliberately NOT a recency threshold — using recency to define the label
and recency as a model feature would be circular (the model would just be
restating its own label back at itself).
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


def compute_churn_label(
    engine: Engine,
    cutoff: pd.Timestamp,
    source_schema: str = None,
    target_schema: str = None,
) -> int:
    """Compute binary churn label for each customer in the observation window.

    Returns the number of labeled customers written to `customer_churn_label`.
    """
    _ensure_schema(engine, target_schema)

    orders = _qualify("orders", source_schema)
    customers = _qualify("customers", source_schema)

    query = f"""
        SELECT o.order_purchase_timestamp, c.customer_unique_id
        FROM {orders} o
        JOIN {customers} c ON o.customer_id = c.customer_id
    """
    df = pd.read_sql(query, engine)
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])

    obs_customers = set(
        df.loc[df["order_purchase_timestamp"] <= cutoff, "customer_unique_id"]
    )
    outcome_customers = set(
        df.loc[df["order_purchase_timestamp"] > cutoff, "customer_unique_id"]
    )

    labels = pd.DataFrame({"customer_unique_id": sorted(obs_customers)})
    labels["churned"] = labels["customer_unique_id"].apply(
        lambda cid: 0 if cid in outcome_customers else 1
    )

    labels.to_sql(
        "customer_churn_label",
        con=engine,
        schema=target_schema,
        if_exists="replace",
        index=False,
    )

    churn_rate = labels["churned"].mean()
    logger.info(
        "Computed churn labels for %d customers (cutoff: %s, churn rate: %.1f%%)",
        len(labels),
        cutoff,
        churn_rate * 100,
    )
    return len(labels)
