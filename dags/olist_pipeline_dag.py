"""
olist_pipeline_dag.py

Example Airflow 3.x DAG for the Olist customer-behavior pipeline.
Stages: ingest (raw) -> data quality -> transform (staging/marts) -> features -> serve
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_TABLES = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "geolocation",
    "product_category_name_translation",
]

FILENAME_MAP = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    # this file breaks the "olist_{name}_dataset.csv" pattern the other 8 follow
    "product_category_name_translation": "product_category_name_translation.csv",
}


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
            from include.ingestion import load_csv_to_raw, verify_row_count

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()

            csv_path = f"{DATA_DIR}/{FILENAME_MAP[table_name]}"
            table = f"raw_{table_name}"

            load_csv_to_raw(csv_path, table, engine, schema="raw", chunk_size=5000)
            verify_row_count(csv_path, table, engine, schema="raw")

        # dynamic task mapping: one mapped task instance per table
        load_and_verify.expand(table_name=RAW_TABLES)

    @task_group(group_id="data_quality")
    def data_quality():
        FK_CHECKS = [
            # (child_table, child_fk_column, parent_table, parent_pk_column)
            ("raw_order_items", "order_id", "raw_orders", "order_id"),
            ("raw_order_payments", "order_id", "raw_orders", "order_id"),
            ("raw_order_reviews", "order_id", "raw_orders", "order_id"),
            ("raw_orders", "customer_id", "raw_customers", "customer_id"),
        ]

        NULL_CHECKS = [
            # (table, column, max_null_fraction)
            ("raw_orders", "order_id", 0.0),
            ("raw_orders", "customer_id", 0.0),
            ("raw_orders", "order_purchase_timestamp", 0.0),
        ]

        @task
        def check_fk(check: tuple):
            """
            Referential integrity check, implemented and tested in
            include/data_quality.py (see tests/test_data_quality.py).
            Fails the task on violation, does not log and continue.
            """
            from include.data_quality import check_referential_integrity

            child_table, child_fk, parent_table, parent_pk = check
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()

            check_referential_integrity(
                engine=engine,
                child_table=child_table,
                child_fk_column=child_fk,
                parent_table=parent_table,
                parent_pk_column=parent_pk,
                child_schema="raw",
                parent_schema="raw",
            )

        @task
        def check_nulls(check: tuple):
            """Null-threshold check, implemented and tested in include/data_quality.py."""
            from include.data_quality import check_null_thresholds

            table, column, max_fraction = check
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()

            check_null_thresholds(
                engine=engine,
                table=table,
                column=column,
                max_null_fraction=max_fraction,
                schema="raw",
            )

        check_fk.expand(check=FK_CHECKS)
        check_nulls.expand(check=NULL_CHECKS)

    @task_group(group_id="transform")
    def transform():
        @task
        def run_build_fact_orders():
            """
            Builds fact_orders in the marts schema. Implemented and tested
            in include/transform.py (see tests/test_transform.py), this
            task is a thin Airflow wrapper.
            """
            from include.transform import build_fact_orders

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            build_fact_orders(engine, source_schema="raw", target_schema="marts")

        @task
        def run_build_dim_customers():
            """Builds dim_customers in the marts schema. See include/transform.py."""
            from include.transform import build_dim_customers

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            build_dim_customers(engine, source_schema="raw", target_schema="marts")

        [run_build_fact_orders(), run_build_dim_customers()]

    @task_group(group_id="features")
    def features():
        @task
        def get_cutoff():
            """
            Data-driven time-split cutoff (80th percentile of order dates),
            not a hardcoded date. Everything downstream in this group respects
            this boundary: features only see orders on/before it, the churn
            label only looks at orders after it. See include/features.py.
            """
            from include.features import compute_time_split_cutoff

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            cutoff = compute_time_split_cutoff(engine, source_schema="raw", quantile=0.8)
            return cutoff.isoformat()

        @task
        def run_compute_rfm(cutoff_iso: str):
            import pandas as pd
            from include.features import compute_rfm

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            compute_rfm(engine, cutoff=pd.Timestamp(cutoff_iso), source_schema="raw", target_schema="marts")

        @task
        def run_compute_delivery_experience(cutoff_iso: str):
            import pandas as pd
            from include.features import compute_delivery_experience

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            compute_delivery_experience(engine, cutoff=pd.Timestamp(cutoff_iso), source_schema="raw", target_schema="marts")

        @task
        def run_compute_churn_label(cutoff_iso: str):
            """
            Ground-truth churn label from real future purchase behavior, not
            a threshold rule. See include/churn.py for why this avoids the
            leakage a recency-threshold label would have caused.
            """
            import pandas as pd
            from include.churn import compute_churn_label

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            compute_churn_label(engine, cutoff=pd.Timestamp(cutoff_iso), source_schema="raw", target_schema="marts")

        @task
        def run_build_order_geography():
            """Real lat/lng per order via Olist's geolocation table. See include/geography.py."""
            from include.geography import build_order_geography

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            engine = hook.get_sqlalchemy_engine()
            build_order_geography(engine, source_schema="raw", target_schema="marts")

        cutoff_iso = get_cutoff()
        [
            run_compute_rfm(cutoff_iso),
            run_compute_delivery_experience(cutoff_iso),
            run_compute_churn_label(cutoff_iso),
            run_build_order_geography(),
        ]

    @task
    def refresh_serving_layer():
        """
        Materialize the final analytics table(s) that Metabase/Streamlit
        reads from, or export the feature set for the churn model.
        """
        pass

    ingest() >> data_quality() >> transform() >> features() >> refresh_serving_layer()


olist_pipeline()
