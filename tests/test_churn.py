"""
Tests for the churn label.
"""

import pandas as pd
import pytest
from sqlalchemy import create_engine

from include.churn import compute_churn_label


@pytest.fixture
def sqlite_engine():
    return create_engine("sqlite:///:memory:")


def _seed(engine, table_name, df):
    df.to_sql(table_name, con=engine, index=False, if_exists="replace")


class TestComputeChurnLabel:
    def test_customer_with_no_order_after_cutoff_is_churned(self, sqlite_engine):
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1"], "customer_id": ["c1"],
            "order_purchase_timestamp": ["2024-01-01"],
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
        }))

        cutoff = pd.Timestamp("2024-06-01")
        compute_churn_label(sqlite_engine, cutoff=cutoff, source_schema=None, target_schema=None)

        result = pd.read_sql_table("customer_churn_label", sqlite_engine)
        assert result.iloc[0]["churned"] == 1

    def test_customer_with_order_after_cutoff_is_not_churned(self, sqlite_engine):
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1", "o2"], "customer_id": ["c1", "c1"],
            "order_purchase_timestamp": ["2024-01-01", "2024-08-01"],
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
        }))

        cutoff = pd.Timestamp("2024-06-01")
        compute_churn_label(sqlite_engine, cutoff=cutoff, source_schema=None, target_schema=None)

        result = pd.read_sql_table("customer_churn_label", sqlite_engine)
        assert result.iloc[0]["churned"] == 0

    def test_customer_with_no_orders_before_cutoff_excluded_entirely(self, sqlite_engine):
        """A customer whose only order is after the cutoff has no observation-
        window history, there's nothing to predict from, so they're not part
        of the labeled set at all (not churned=0, just absent)."""
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1"], "customer_id": ["c1"],
            "order_purchase_timestamp": ["2024-08-01"],
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
        }))

        cutoff = pd.Timestamp("2024-06-01")
        compute_churn_label(sqlite_engine, cutoff=cutoff, source_schema=None, target_schema=None)

        result = pd.read_sql_table("customer_churn_label", sqlite_engine)
        assert len(result) == 0