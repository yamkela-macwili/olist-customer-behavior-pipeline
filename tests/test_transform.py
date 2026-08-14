"""
Tests for the transform layer: fact_orders and dim_customers.
"""

import pandas as pd
import pytest
from sqlalchemy import create_engine

from include.transform import build_fact_orders, build_dim_customers


@pytest.fixture
def sqlite_engine():
    return create_engine("sqlite:///:memory:")


def _seed(engine, table_name, df):
    df.to_sql(table_name, con=engine, index=False, if_exists="replace")


class TestBuildFactOrders:
    def test_aggregates_multiple_order_items_correctly(self, sqlite_engine):
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1"],
            "customer_id": ["c1"],
            "order_status": ["delivered"],
            "order_purchase_timestamp": ["2024-01-01"],
            "order_delivered_customer_date": ["2024-01-05"],
            "order_estimated_delivery_date": ["2024-01-06"],
        }))
        # o1 has two line items: prices 50 + 30, freight 5 + 5
        _seed(sqlite_engine, "order_items", pd.DataFrame({
            "order_id": ["o1", "o1"],
            "price": [50.0, 30.0],
            "freight_value": [5.0, 5.0],
        }))
        _seed(sqlite_engine, "order_payments", pd.DataFrame({
            "order_id": ["o1"],
            "payment_value": [90.0],
        }))
        _seed(sqlite_engine, "order_reviews", pd.DataFrame({
            "order_id": ["o1"],
            "review_score": [5],
        }))

        build_fact_orders(sqlite_engine, source_schema=None, target_schema=None)

        result = pd.read_sql_table("fact_orders", sqlite_engine)
        row = result.iloc[0]
        assert row["total_price"] == 80.0
        assert row["total_freight"] == 10.0
        assert row["item_count"] == 2
        assert row["total_payment"] == 90.0
        assert row["avg_review_score"] == 5.0

    def test_order_with_no_items_gets_zero_not_null(self, sqlite_engine):
        """An order with no matching order_items should still appear, with 0s,
        not be silently dropped by an inner join."""
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o_empty"],
            "customer_id": ["c1"],
            "order_status": ["canceled"],
            "order_purchase_timestamp": ["2024-01-01"],
            "order_delivered_customer_date": [None],
            "order_estimated_delivery_date": ["2024-01-06"],
        }))
        _seed(sqlite_engine, "order_items", pd.DataFrame({
            "order_id": [], "price": [], "freight_value": [],
        }))
        _seed(sqlite_engine, "order_payments", pd.DataFrame({
            "order_id": [], "payment_value": [],
        }))
        _seed(sqlite_engine, "order_reviews", pd.DataFrame({
            "order_id": [], "review_score": [],
        }))

        build_fact_orders(sqlite_engine, source_schema=None, target_schema=None)

        result = pd.read_sql_table("fact_orders", sqlite_engine)
        assert len(result) == 1
        assert result.iloc[0]["total_price"] == 0.0

    def test_row_count_matches_order_count_not_item_count(self, sqlite_engine):
        """Multiple order_items must not fan out into multiple fact_orders rows."""
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1", "o2"],
            "customer_id": ["c1", "c2"],
            "order_status": ["delivered", "delivered"],
            "order_purchase_timestamp": ["2024-01-01", "2024-01-02"],
            "order_delivered_customer_date": ["2024-01-05", "2024-01-06"],
            "order_estimated_delivery_date": ["2024-01-06", "2024-01-07"],
        }))
        _seed(sqlite_engine, "order_items", pd.DataFrame({
            "order_id": ["o1", "o1", "o2"],
            "price": [10.0, 20.0, 15.0],
            "freight_value": [1.0, 1.0, 1.0],
        }))
        _seed(sqlite_engine, "order_payments", pd.DataFrame({
            "order_id": ["o1", "o2"], "payment_value": [30.0, 15.0],
        }))
        _seed(sqlite_engine, "order_reviews", pd.DataFrame({
            "order_id": ["o1", "o2"], "review_score": [4, 5],
        }))

        build_fact_orders(sqlite_engine, source_schema=None, target_schema=None)

        result = pd.read_sql_table("fact_orders", sqlite_engine)
        assert len(result) == 2  # not 3


class TestBuildDimCustomers:
    def test_dedupes_repeat_customer_to_one_row(self, sqlite_engine):
        """Same customer_unique_id across two order-level customer_ids should
        collapse to one dim_customers row, not two."""
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["order_acct_1", "order_acct_2"],
            "customer_unique_id": ["person_a", "person_a"],
            "customer_city": ["cape town", "cape town"],
            "customer_state": ["WC", "WC"],
        }))
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "customer_id": ["order_acct_1", "order_acct_2"],
            "order_purchase_timestamp": ["2024-01-01", "2024-02-01"],
        }))

        build_dim_customers(sqlite_engine, source_schema=None, target_schema=None)

        result = pd.read_sql_table("dim_customers", sqlite_engine)
        assert len(result) == 1
        assert result.iloc[0]["distinct_order_accounts"] == 2

    def test_distinct_customers_stay_separate(self, sqlite_engine):
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["a1", "b1"],
            "customer_unique_id": ["person_a", "person_b"],
            "customer_city": ["cape town", "durban"],
            "customer_state": ["WC", "KZN"],
        }))
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "customer_id": ["a1", "b1"],
            "order_purchase_timestamp": ["2024-01-01", "2024-01-01"],
        }))

        build_dim_customers(sqlite_engine, source_schema=None, target_schema=None)

        result = pd.read_sql_table("dim_customers", sqlite_engine)
        assert len(result) == 2

    def test_city_state_taken_from_most_recent_order_not_first_alphabetically(self, sqlite_engine):
        """Customer moved: first order from Cape Town, most recent order from
        Durban. dim_customers should report Durban (their current address),
        not Cape Town (which alphabetical MIN() would have picked)."""
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["order_acct_1", "order_acct_2"],
            "customer_unique_id": ["person_a", "person_a"],
            "customer_city": ["cape town", "durban"],
            "customer_state": ["WC", "KZN"],
        }))
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "customer_id": ["order_acct_1", "order_acct_2"],
            "order_purchase_timestamp": ["2024-01-01", "2024-06-01"],  # order_acct_2 is more recent
        }))

        build_dim_customers(sqlite_engine, source_schema=None, target_schema=None)

        result = pd.read_sql_table("dim_customers", sqlite_engine)
        assert result.iloc[0]["customer_city"] == "durban"
        assert result.iloc[0]["customer_state"] == "KZN"
