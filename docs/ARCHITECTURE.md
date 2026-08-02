# Architecture

## Overview

```
Kaggle CSVs (9 files)
        │
        ▼
┌─────────────────┐
│   RAW LAYER     │  Airflow: dynamic task mapping, one task per CSV
│  (schema: raw)  │  Chunked load, no transformation, row-count sanity check
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  DATA QUALITY   │  Great Expectations: referential integrity, null thresholds
│  (gate)         │  Fails the DAG on violation — does not log-and-continue
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  STAGING/MARTS  │  SQL via PostgresHook
│  (schema: marts)│  fact_orders, dim_customers — joined, cleaned, deduped
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  FEATURES       │  Per-customer: RFM, delivery experience, churn proxy
│  (schema:       │
│   features)     │
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  SERVING        │  Metabase (Docker), connected directly to `features` schema
└─────────────────┘
```

## Layer detail

### Raw layer (`raw` schema)
- One Postgres table per source CSV: `raw_customers`, `raw_orders`, `raw_order_items`, `raw_order_payments`, `raw_order_reviews`, `raw_products`, `raw_sellers`, `raw_geolocation`, `raw_product_category_name_translation`.
- Loaded via Airflow dynamic task mapping (`load_csv_to_raw.expand(table_name=RAW_TABLES)`).
- Chunked inserts (5000 rows/batch) to simulate incremental batch loading rather than one bulk load.
- No cleaning, no joins, no type coercion beyond what pandas infers on read. This layer is a faithful copy of the source — if something looks wrong downstream, you should be able to trace it back here and rule raw ingestion in or out.
- Row-count sanity check after load: source CSV row count must equal loaded row count.

### Data quality gate
- Runs after raw load, before any transform touches the data.
- Great Expectations suites define expectations per table (see `docs/DECISIONS.md` for what's actually checked and why).
- On failure: the DAG task fails and the pipeline stops. This is deliberate — a DQ check that logs a warning and lets bad data flow into `marts` isn't a data quality check, it's a data quality suggestion.

### Staging / marts (`marts` schema)
- `fact_orders`: orders joined with order_items, payments, and reviews — one row per order.
- `dim_customers`: deduplicated customer dimension.
- Built with plain SQL via `PostgresHook` (see `docs/DECISIONS.md` for why not dbt).

### Features (`features` schema)
- `customer_rfm`: Recency, Frequency, Monetary per customer.
- `customer_delivery_experience`: average delivery delay, late-delivery rate per customer.
- `customer_churn_proxy`: churn label per customer, computed against a proxy definition (documented in `docs/DECISIONS.md`, not yet finalized).
- This is the layer that actually answers "customer behavior" questions — everything upstream exists to make this layer correct and trustworthy.

### Serving
- Metabase container, connected directly to the `features` schema (and `marts` if drill-down is useful).
- Dashboards: RFM segment distribution, churn rate over time, delivery performance vs churn correlation.

## Orchestration

Single Airflow DAG, `olist_customer_behavior_pipeline`, structured as task groups:

```
ingest >> data_quality >> transform >> features >> refresh_serving_layer
```

Each task group corresponds to one layer above. See `dags/olist_pipeline_dag.py`.

## Testing strategy

- Pure logic (loaders, DQ check functions, join/transform functions, feature calculations) is developed test-first against fixture data in `tests/`.
- DAG structure and wiring are verified with integration checks (`airflow dags test`), not unit tests — the DAG itself is glue, not logic.
