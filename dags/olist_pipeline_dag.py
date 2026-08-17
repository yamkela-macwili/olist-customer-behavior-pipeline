"""
olist_pipeline_dag.py

Airflow DAG for the Olist customer-behavior pipeline.
"""

from __future__ import annotations

import logging
import os

import pendulum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task, task_group

logger = logging.getLogger(__name__)


# ── Configuration ─────────────────────────────────────────────────────────────

POSTGRES_CONN_ID: str = os.environ.get(
    "POSTGRES_CONN_ID",
    "olist_postgres",
)

DATA_DIR: str = os.environ.get(
    "DATA_DIR",
    "/opt/airflow/data",
)

CHUNK_SIZE: int = int(
    os.environ.get(
        "CHUNK_SIZE",
        "5000",
    )
)


# ── Source tables ─────────────────────────────────────────────────────────────

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


# Logical table name → source CSV filename

FILENAME_MAP = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "product_category_name_translation": ("product_category_name_translation.csv"),
}


# ── Data quality configuration ───────────────────────────────────────────────

FK_CHECKS = [
    # child_table, child_fk, parent_table, parent_pk
    (
        "raw_order_items",
        "order_id",
        "raw_orders",
        "order_id",
    ),
    (
        "raw_order_payments",
        "order_id",
        "raw_orders",
        "order_id",
    ),
    (
        "raw_order_reviews",
        "order_id",
        "raw_orders",
        "order_id",
    ),
    (
        "raw_orders",
        "customer_id",
        "raw_customers",
        "customer_id",
    ),
]


NULL_CHECKS = [
    # table, column, maximum null fraction
    (
        "raw_orders",
        "order_id",
        0.0,
    ),
    (
        "raw_orders",
        "customer_id",
        0.0,
    ),
    (
        "raw_orders",
        "order_purchase_timestamp",
        0.0,
    ),
]


# ── DAG ───────────────────────────────────────────────────────────────────────


@dag(
    dag_id="olist_customer_behavior_pipeline",
    schedule=None,  # Manual trigger only.
    start_date=pendulum.datetime(
        2026, 1, 1,
        tz="UTC",
    ),
    catchup=False,
    tags=[
        "data-engineering",
        "olist",
        "customer-behavior",
        "ml",
    ],
    doc_md="""
        # Olist Customer Behavior Pipeline

        End-to-end data engineering and ML pipeline using the
        Olist Brazilian e-commerce dataset.

        ## Pipeline

        1. **Ingest**
        - Load all 9 source CSV files.
        - Store them in the `raw` schema.
        - Verify row counts.

        2. **Data Quality**
        - Referential integrity checks.
        - Null-threshold checks.
        - Fail loudly when data quality rules are violated.

        3. **Transform**
        - Build `fact_orders`.
        - Build `dim_customers`.

        4. **Features**
        - Compute RFM features.
        - Compute delivery experience.
        - Compute churn labels.
        - Compute order geography.

        5. **Serve**
        - Final serving-layer hook.
        - Metabase reads directly from the `marts` schema.

        ## Architecture

        All business logic lives in `include/`.

        The DAG itself is intentionally thin and is responsible for:

        - orchestration
        - dependencies
        - retries
        - logging
        - database connections
        - task mapping
        """,
)
def olist_pipeline():

    # =========================================================================
    # Stage 1: INGEST
    # =========================================================================

    @task_group(group_id="ingest")
    def ingest():

        @task(
            retries=2,
            retry_delay=pendulum.duration(minutes=1),
        )
        def load_and_verify(table_name: str) -> None:
            """
            Load one CSV into raw schema and verify its row count.
            """

            from include.ingestion import (
                load_csv_to_raw,
                verify_row_count,
            )

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            csv_path = os.path.join(
                DATA_DIR,
                FILENAME_MAP[table_name],
            )

            table = f"raw_{table_name}"

            logger.info(
                "Starting ingestion: %s → raw.%s",
                csv_path,
                table,
            )

            load_csv_to_raw(
                csv_path,
                table,
                engine,
                schema="raw",
                chunk_size=CHUNK_SIZE,
            )

            verify_row_count(
                csv_path,
                table,
                engine,
                schema="raw",
            )

            logger.info(
                "Completed ingestion: raw.%s",
                table,
            )

        load_and_verify.expand(
            table_name=RAW_TABLES,
        )

    # =========================================================================
    # Stage 2: DATA QUALITY
    # =========================================================================

    @task_group(group_id="data_quality")
    def data_quality():

        @task(retries=1)
        def check_fk(check: tuple) -> None:
            """
            Check referential integrity.

            Task fails if orphaned foreign-key records are found.
            """

            from include.data_quality import (
                check_referential_integrity,
            )

            (
                child_table,
                child_fk,
                parent_table,
                parent_pk,
            ) = check

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            logger.info(
                "FK check: %s.%s → %s.%s",
                child_table,
                child_fk,
                parent_table,
                parent_pk,
            )

            check_referential_integrity(
                engine=engine,
                child_table=child_table,
                child_fk_column=child_fk,
                parent_table=parent_table,
                parent_pk_column=parent_pk,
                child_schema="raw",
                parent_schema="raw",
            )

        @task(retries=1)
        def check_nulls(check: tuple) -> None:
            """
            Check whether a column exceeds its allowed null fraction.
            """

            from include.data_quality import (
                check_null_thresholds,
            )

            (
                table,
                column,
                max_fraction,
            ) = check

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            logger.info(
                "Null check: %s.%s (max %.2f%%)",
                table,
                column,
                max_fraction * 100,
            )

            check_null_thresholds(
                engine=engine,
                table=table,
                column=column,
                max_null_fraction=max_fraction,
                schema="raw",
            )

        check_fk.expand(
            check=FK_CHECKS,
        )

        check_nulls.expand(
            check=NULL_CHECKS,
        )

    # =========================================================================
    # Stage 3 — TRANSFORM
    # =========================================================================

    @task_group(group_id="transform")
    def transform():

        @task
        def run_build_fact_orders() -> None:
            """
            Build marts.fact_orders.
            """

            from include.transform import (
                build_fact_orders,
            )

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            rows = build_fact_orders(
                engine,
                source_schema="raw",
                target_schema="marts",
            )

            logger.info(
                "fact_orders built successfully: %d rows",
                rows,
            )

        @task
        def run_build_dim_customers() -> None:
            """
            Build marts.dim_customers.
            """

            from include.transform import (
                build_dim_customers,
            )

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            rows = build_dim_customers(
                engine,
                source_schema="raw",
                target_schema="marts",
            )

            logger.info(
                "dim_customers built successfully: %d rows",
                rows,
            )

        fact_orders = run_build_fact_orders()

        dim_customers = run_build_dim_customers()

        [
            fact_orders,
            dim_customers,
        ]

    # =========================================================================
    # Stage 4 — FEATURES
    # =========================================================================

    @task_group(group_id="features")
    def features():

        @task
        def get_cutoff() -> str:
            """
            Compute the 80th percentile order date.

            The resulting date is used as the time-split cutoff
            for feature engineering and churn labeling.
            """

            from include.features import (
                compute_time_split_cutoff,
            )

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            cutoff = compute_time_split_cutoff(
                engine,
                source_schema="raw",
                quantile=0.8,
            )

            logger.info(
                "Time-split cutoff: %s",
                cutoff,
            )

            return cutoff.isoformat()

        @task
        def run_compute_rfm(
            cutoff_iso: str,
        ) -> None:
            """
            Compute customer RFM features.
            """

            import pandas as pd

            from include.features import (
                compute_rfm,
            )

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            rows = compute_rfm(
                engine,
                cutoff=pd.Timestamp(cutoff_iso),
                source_schema="raw",
                target_schema="marts",
            )

            logger.info(
                "customer_rfm built successfully: %d rows",
                rows,
            )

        @task
        def run_compute_delivery_experience(
            cutoff_iso: str,
        ) -> None:
            """
            Compute delivery experience features.
            """

            import pandas as pd

            from include.features import (
                compute_delivery_experience,
            )

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            rows = compute_delivery_experience(
                engine,
                cutoff=pd.Timestamp(cutoff_iso),
                source_schema="raw",
                target_schema="marts",
            )

            logger.info(
                "customer_delivery_experience built successfully: %d rows",
                rows,
            )

        @task
        def run_compute_churn_label(
            cutoff_iso: str,
        ) -> None:
            """
            Compute the binary churn label.

            The label is based on future purchase behavior after
            the time-split cutoff.
            """

            import pandas as pd

            from include.churn import (
                compute_churn_label,
            )

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            rows = compute_churn_label(
                engine,
                cutoff=pd.Timestamp(cutoff_iso),
                source_schema="raw",
                target_schema="marts",
            )

            logger.info(
                "customer_churn_label built successfully: %d rows",
                rows,
            )

        @task
        def run_build_order_geography() -> None:
            """
            Attach geographic centroid information to orders.
            """

            from include.geography import (
                build_order_geography,
            )

            hook = PostgresHook(
                postgres_conn_id=POSTGRES_CONN_ID,
            )

            engine = hook.get_sqlalchemy_engine()

            rows = build_order_geography(
                engine,
                source_schema="raw",
                target_schema="marts",
            )

            logger.info(
                "order_geography built successfully: %d rows",
                rows,
            )

        cutoff_iso = get_cutoff()

        run_compute_rfm(
            cutoff_iso,
        )

        run_compute_delivery_experience(
            cutoff_iso,
        )

        run_compute_churn_label(
            cutoff_iso,
        )

        run_build_order_geography()

    # =========================================================================
    # Stage 5 — SERVE
    # =========================================================================

    @task
    def refresh_serving_layer() -> None:
        """
        Refresh the serving layer.

        Metabase currently reads directly from the marts schema,
        so there is no physical materialization step required.
        """

        logger.info(
            "Serving layer refresh complete. "
            "Metabase reads directly from the marts schema."
        )

    # =========================================================================
    # DAG DEPENDENCIES
    # =========================================================================

    ingest_tasks = ingest()

    quality_tasks = data_quality()

    transform_tasks = transform()

    feature_tasks = features()

    serving_task = refresh_serving_layer()

    ingest_tasks >> quality_tasks
    quality_tasks >> transform_tasks
    transform_tasks >> feature_tasks
    feature_tasks >> serving_task


olist_pipeline()
