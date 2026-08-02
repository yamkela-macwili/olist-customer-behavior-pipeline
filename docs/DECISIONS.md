# Design Decisions

Each entry: the decision, the alternatives considered, and why. Updated as decisions
are made or revisited, not written retroactively at the end of the project.

---

## Dataset selection: Olist Brazilian E-Commerce

**Alternatives considered:** Fully synthetic customer data (generated, not observed); market/price time-series data (e.g. OHLC candles) with a manually invented mapping onto customer actions.

**Why Olist:** A market-data approach would require inventing a semantic link between price movement and customer behavior (e.g. "bullish candle = purchase") that has no real basis, that's not a transform, it's a fabricated label, and it wouldn't hold up to scrutiny. Purely synthetic data avoids that problem but doesn't carry real-world messiness (missing values, cancelled orders, inconsistent delivery timing) that a pipeline should be built to handle. Olist gives real, relational, appropriately messy e-commerce data where RFM, churn, and delivery-experience analysis are genuinely supported by what's in the tables — no invented mapping required.

---

## Orchestration: Apache Airflow 3.x, TaskFlow API

**Why:** TaskFlow API + dynamic task mapping (`expand()`) avoids hand-writing near-identical tasks per CSV and is the current idiomatic pattern, not the older `PythonOperator`-heavy style.

---

## Storage: PostgreSQL

**Why:** Relational by nature (matches the Olist schema's foreign keys), well-supported by Airflow's `PostgresHook`/`PostgresOperator`, and free to run locally via Docker.

---

## Transform: plain SQL via `PostgresHook` (not dbt)

**Alternatives considered:** dbt (via `BashOperator` or Cosmos).

**Why plain SQL:** dbt is the more CV-impressive choice, but it's another tool to learn correctly under a hard deadline (25 Sept 2026), and the added abstraction (models, `ref()`, materializations) isn't necessary at this project's scale — a handful of join/aggregate queries. Plain SQL keeps the transform logic testable and explainable without a second framework's mental model layered on top.

**Revisit if:** there's real time buffer left after the feature and serving layers are working end-to-end.

---

## Data quality: Great Expectations

**What's actually checked:**
- Referential integrity - every `order_id` in `order_items`/`payments`/`reviews` must exist in `orders`; every `customer_id` in `orders` must exist in `customers`.
- Null thresholds - key columns (`order_id`, `customer_id`, `order_purchase_timestamp`) must not exceed a defined null percentage.
- [TODO: finalize the specific expectation suite once the raw schema is loaded and real data quality issues are visible — Olist is known to have some missing delivery dates and cancelled orders, which need explicit handling, not silent filtering.]

**Why fail loudly instead of logging and continuing:** A DQ check that logs a warning and lets bad data flow into `marts` is decorative. If referential integrity breaks, every downstream join and feature calculation built on it is untrustworthy, better to stop and fix the raw load than build features on broken joins.

---

## Serving: Metabase 

**Alternatives considered:** Power BI (originally in the README), Streamlit.

**Why not Power BI:** Power BI Desktop is Windows-only with no free, container-friendly server component. Connecting it to a local Dockerized Postgres instance requires an ODBC/gateway bridge, real setup friction for a solo project on a deadline, and it doesn't demo cleanly in a screen-recorded video the way "docker-compose up, open localhost" does.

**Why not Streamlit:** More control and more "custom code" credit, but costs real app-building time that's better spent on the pipeline itself.

**Why Metabase:** Ships as a Docker container, connects to Postgres with no extra configuration, matches the rest of the stack (everything else is already Dockerized), and demos in two commands.

---

## Churn proxy definition

**Status:** Not finalized. [TODO before Milestone 4 (#25): define explicitly — e.g. "no repeat order within 90 days of the customer's last purchase" — and state clearly that this is an operational proxy, not verified churn, since Olist has no subscription/cancellation signal. State it here before implementing #25, not after.]
