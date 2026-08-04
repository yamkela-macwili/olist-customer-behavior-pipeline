"""
olist_pipeline_dag.py

Example Airflow 3.x DAG for the Olist customer-behavior pipeline.
Stages: ingest (raw) -> data quality -> transform (staging/marts) -> features -> serve

This is a SKELETON, not a finished pipeline. Fill in the actual pandas/SQL logic
per task. Structure matters more than the stub content right now, get the DAG
shape and dependency graph right first, then flesh out each function.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_TABLES = [
    "customers",
    "orders",
    "order_items",
    "payments",
    "reviews",
    "products",
    "sellers",
    "geolocation",
    "product_category_name_translation",
]

DATA_DIR = "/opt/airflow/data/olist"  # mount your Kaggle CSVs here
POSTGRES_CONN_ID = "olist_postgres"


@dag(
    dag_id="olist_customer_behavior_pipeline",
    schedule=None,  # trigger manually or set to "@daily" once you simulate incremental loads
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data-engineering", "elective", "olist"],
)
def olist_pipeline():

    @task_group(group_id="ingest")
    def ingest():
        @task
        def load_and_verify(table_name: str):
            """
            Load one raw CSV into the `raw` schema, then verify the loaded
            row count matches the source. Both functions are implemented and
            tested in include/ingestion.py (see tests/test_ingestion.py),
            this task is a thin Airflow wrapper around already-proven logic.
            """
            from src.ingestion.ingestion import load_csv_to_raw, verify_row_count

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()

            csv_path = f"{DATA_DIR}/olist_{table_name}_dataset.csv"
            table = f"raw_{table_name}"

            load_csv_to_raw(csv_path, table, engine, schema="raw", chunk_size=5000)
            verify_row_count(csv_path, table, engine, schema="raw")

        # dynamic task mapping: one mapped task instance per table
        load_and_verify.expand(table_name=RAW_TABLES)

    @task_group(group_id="data_quality")
    def data_quality():
        @task
        def check_referential_integrity():
            """
            E.g. every order_id in raw_order_items must exist in raw_orders.
            Raise (fail the task) on violation rather than silently continuing.
            you want the DAG to fail loudly, not load garbage downstream.
            """
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            orphaned = hook.get_first(
                """
                SELECT COUNT(*) FROM raw.raw_order_items oi
                LEFT JOIN raw.raw_orders o ON oi.order_id = o.order_id
                WHERE o.order_id IS NULL
                """
            )[0]
            if orphaned > 0:
                raise ValueError(f"{orphaned} order_items with no matching order")

        @task
        def check_null_thresholds():
            """Fail if key columns exceed an acceptable null percentage."""
            pass  # implement per-table checks

        check_referential_integrity()
        check_null_thresholds()

    @task_group(group_id="transform")
    def transform():
        @task
        def build_fact_orders():
            """
            Join orders + order_items + payments + customers into a clean
            fact table in the `staging` or `marts` schema. Plain SQL via
            PostgresHook is fine here, swap for dbt run (BashOperator/Cosmos)
            once you want that on your CV.
            """
            pass

        @task
        def build_dim_customers():
            pass

        [build_fact_orders(), build_dim_customers()]

    @task_group(group_id="features")
    def features():
        @task
        def compute_rfm():
            """Recency / Frequency / Monetary per customer -> features.rfm"""
            pass

        @task
        def compute_delivery_experience():
            """Avg delivery delay, late-delivery rate per customer."""
            pass

        @task
        def compute_churn_proxy():
            """
            Define churn explicitly, e.g. no repeat order within 90 days of
            last purchase. Document the definition in the README, it's a
            proxy, not ground truth, and you should say so.
            """
            pass

        [compute_rfm(), compute_delivery_experience(), compute_churn_proxy()]

    @task
    def refresh_serving_layer():
        """
        Materialize the final analytics table(s) that Metabase/Streamlit
        reads from, or export the feature set for the churn model.
        """
        pass

    ingest() >> data_quality() >> transform() >> features() >> refresh_serving_layer()


olist_pipeline()