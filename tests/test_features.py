"""
Tests for feature-layer functions: time-split cutoff, RFM, delivery experience.
"""

import pandas as pd
import pytest
from sqlalchemy import create_engine

from include.features import compute_time_split_cutoff, compute_rfm, compute_delivery_experience
from include.transform import build_fact_orders


@pytest.fixture
def sqlite_engine():
    return create_engine("sqlite:///:memory:")


def _seed(engine, table_name, df):
    df.to_sql(table_name, con=engine, index=False, if_exists="replace")


class TestComputeTimeSplitCutoff:
    def test_cutoff_is_a_quantile_of_order_dates_not_hardcoded(self, sqlite_engine):
        # 10 evenly spaced order dates, Jan 1 to Jan 10 2024
        dates = [f"2024-01-{d:02d}" for d in range(1, 11)]
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": [f"o{i}" for i in range(10)],
            "order_purchase_timestamp": dates,
        }))

        cutoff = compute_time_split_cutoff(sqlite_engine, source_schema=None, quantile=0.8)

        # 80th percentile of 10 evenly spaced days should land around day 8
        assert pd.Timestamp("2024-01-07") <= cutoff <= pd.Timestamp("2024-01-09")


class TestComputeRfm:
    def test_recency_frequency_monetary_computed_correctly(self, sqlite_engine):
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1", "o2"],
            "customer_id": ["c1", "c1"],
            "order_status": ["delivered", "delivered"],
            "order_purchase_timestamp": ["2024-01-01", "2024-01-10"],
            "order_delivered_customer_date": ["2024-01-05", "2024-01-14"],
            "order_estimated_delivery_date": ["2024-01-06", "2024-01-15"],
        }))
        _seed(sqlite_engine, "order_items", pd.DataFrame({
            "order_id": ["o1", "o2"], "price": [50.0, 30.0], "freight_value": [5.0, 5.0],
        }))
        _seed(sqlite_engine, "order_payments", pd.DataFrame({
            "order_id": ["o1", "o2"], "payment_value": [55.0, 35.0],
        }))
        _seed(sqlite_engine, "order_reviews", pd.DataFrame({
            "order_id": ["o1", "o2"], "review_score": [5, 4],
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
        }))

        build_fact_orders(sqlite_engine, source_schema=None, target_schema=None)

        cutoff = pd.Timestamp("2024-01-10")
        compute_rfm(sqlite_engine, cutoff=cutoff, source_schema=None, target_schema=None)

        result = pd.read_sql_table("customer_rfm", sqlite_engine)
        row = result.iloc[0]
        assert row["customer_unique_id"] == "u1"
        assert row["frequency"] == 2
        assert row["monetary"] == 80.0  # 50 + 30
        assert row["recency_days"] == 0  # last order IS the cutoff date

    def test_orders_after_cutoff_excluded_from_features(self, sqlite_engine):
        """This is the leakage guard: an order placed after the cutoff must not
        affect RFM, RFM is meant to reflect only what's known as of the cutoff."""
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1", "o_future"],
            "customer_id": ["c1", "c1"],
            "order_status": ["delivered", "delivered"],
            "order_purchase_timestamp": ["2024-01-01", "2024-06-01"],
            "order_delivered_customer_date": ["2024-01-05", "2024-06-05"],
            "order_estimated_delivery_date": ["2024-01-06", "2024-06-06"],
        }))
        _seed(sqlite_engine, "order_items", pd.DataFrame({
            "order_id": ["o1", "o_future"], "price": [50.0, 999.0], "freight_value": [5.0, 5.0],
        }))
        _seed(sqlite_engine, "order_payments", pd.DataFrame({
            "order_id": ["o1", "o_future"], "payment_value": [55.0, 1004.0],
        }))
        _seed(sqlite_engine, "order_reviews", pd.DataFrame({
            "order_id": ["o1", "o_future"], "review_score": [5, 1],
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
        }))

        build_fact_orders(sqlite_engine, source_schema=None, target_schema=None)

        cutoff = pd.Timestamp("2024-01-10")  # before o_future
        compute_rfm(sqlite_engine, cutoff=cutoff, source_schema=None, target_schema=None)

        result = pd.read_sql_table("customer_rfm", sqlite_engine)
        row = result.iloc[0]
        assert row["frequency"] == 1  # only o1 counts
        assert row["monetary"] == 50.0  # not 999


class TestComputeDeliveryExperience:
    def test_late_delivery_detected_correctly(self, sqlite_engine):
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1", "o2"],
            "customer_id": ["c1", "c1"],
            "order_purchase_timestamp": ["2024-01-01", "2024-01-02"],
            "order_delivered_customer_date": ["2024-01-10", "2024-01-05"],
            "order_estimated_delivery_date": ["2024-01-08", "2024-01-05"],  # o1 late by 2, o2 on time
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
        }))

        cutoff = pd.Timestamp("2024-12-31")
        compute_delivery_experience(sqlite_engine, cutoff=cutoff, source_schema=None, target_schema=None)

        result = pd.read_sql_table("customer_delivery_experience", sqlite_engine)
        row = result.iloc[0]
        assert row["late_delivery_rate"] == 0.5  # 1 of 2 orders late
        assert row["avg_delay_days"] == pytest.approx(1.0)  # (2 + 0) / 2

    def test_undelivered_orders_excluded_not_treated_as_ontime(self, sqlite_engine):
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1"],
            "customer_id": ["c1"],
            "order_purchase_timestamp": ["2024-01-01"],
            "order_delivered_customer_date": [None],  # never delivered
            "order_estimated_delivery_date": ["2024-01-08"],
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
        }))

        cutoff = pd.Timestamp("2024-12-31")
        compute_delivery_experience(sqlite_engine, cutoff=cutoff, source_schema=None, target_schema=None)

        result = pd.read_sql_table("customer_delivery_experience", sqlite_engine)
        assert len(result) == 0  # no delivered orders, no delivery-experience row
