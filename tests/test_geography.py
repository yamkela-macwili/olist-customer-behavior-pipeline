"""
Tests for order-level geography enrichment using real Olist geolocation data.
"""

import pandas as pd
import pytest
from sqlalchemy import create_engine

from include.geography import build_order_geography


@pytest.fixture
def sqlite_engine():
    return create_engine("sqlite:///:memory:")


def _seed(engine, table_name, df):
    df.to_sql(table_name, con=engine, index=False, if_exists="replace")


class TestBuildOrderGeography:
    def test_order_gets_centroid_of_its_zip_prefix(self, sqlite_engine):
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1"], "customer_id": ["c1"],
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
            "customer_zip_code_prefix": ["1000"],
            "customer_city": ["cape town"], "customer_state": ["WC"],
        }))
        # two raw geolocation points under the same zip prefix, centroid = average
        _seed(sqlite_engine, "geolocation", pd.DataFrame({
            "geolocation_zip_code_prefix": ["1000", "1000"],
            "geolocation_lat": [-33.9, -34.1],
            "geolocation_lng": [18.4, 18.6],
        }))

        build_order_geography(sqlite_engine, source_schema=None, target_schema=None)

        result = pd.read_sql_table("order_geography", sqlite_engine)
        row = result.iloc[0]
        assert row["lat"] == pytest.approx(-34.0)
        assert row["lng"] == pytest.approx(18.5)

    def test_order_with_no_matching_geolocation_gets_null_not_dropped(self, sqlite_engine):
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1"], "customer_id": ["c1"],
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
            "customer_zip_code_prefix": ["9999"],
            "customer_city": ["nowhere"], "customer_state": ["XX"],
        }))
        _seed(sqlite_engine, "geolocation", pd.DataFrame({
            "geolocation_zip_code_prefix": ["1000"],
            "geolocation_lat": [-33.9], "geolocation_lng": [18.4],
        }))

        build_order_geography(sqlite_engine, source_schema=None, target_schema=None)

        result = pd.read_sql_table("order_geography", sqlite_engine)
        assert len(result) == 1  # order still present
        assert pd.isna(result.iloc[0]["lat"])

    def test_row_count_matches_order_count(self, sqlite_engine):
        _seed(sqlite_engine, "orders", pd.DataFrame({
            "order_id": ["o1", "o2"], "customer_id": ["c1", "c1"],
        }))
        _seed(sqlite_engine, "customers", pd.DataFrame({
            "customer_id": ["c1"], "customer_unique_id": ["u1"],
            "customer_zip_code_prefix": ["1000"],
            "customer_city": ["cape town"], "customer_state": ["WC"],
        }))
        _seed(sqlite_engine, "geolocation", pd.DataFrame({
            "geolocation_zip_code_prefix": ["1000"],
            "geolocation_lat": [-33.9], "geolocation_lng": [18.4],
        }))

        build_order_geography(sqlite_engine, source_schema=None, target_schema=None)

        result = pd.read_sql_table("order_geography", sqlite_engine)
        assert len(result) == 2  # one row per order, not per customer