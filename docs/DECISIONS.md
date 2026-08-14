# Design Decisions

Each entry: the decision, the alternatives considered, and why. Updated as decisions
are made or revisited, not written retroactively at the end of the project.

---

## Dataset selection: Olist Brazilian E-Commerce

**Alternatives considered:** Fully synthetic customer data (generated, not observed); market/price time-series data (e.g. OHLC candles) with a manually invented mapping onto customer actions.

**Why Olist:** A market-data approach would require inventing a semantic link between price movement and customer behavior (e.g. "bullish candle = purchase") that has no real basis. That's not a transform, it's a fabricated label, and it wouldn't hold up to scrutiny. Purely synthetic data avoids that problem but doesn't carry real-world messiness (missing values, cancelled orders, inconsistent delivery timing) that a pipeline should be built to handle. Olist gives real, relational, appropriately messy e-commerce data where RFM, churn, and delivery-experience analysis are genuinely supported by what's in the tables, no invented mapping required.

---

## Orchestration: Apache Airflow 3.x, TaskFlow API

**Why:** TaskFlow API + dynamic task mapping (`expand()`) avoids hand-writing near-identical tasks per CSV and is the current idiomatic pattern, not the older `PythonOperator`-heavy style.

---

## Storage: PostgreSQL

**Why:** Relational by nature (matches the Olist schema's foreign keys), well-supported by Airflow's `PostgresHook`/`PostgresOperator`, and free to run locally via Docker.

---

## Transform: plain SQL via `PostgresHook` (not dbt)

**Alternatives considered:** dbt (via `BashOperator` or Cosmos).

**Why plain SQL:** dbt is the more CV-impressive choice, but it's another tool to learn correctly under a hard deadline (25 Sept 2026), and the added abstraction (models, `ref()`, materializations) isn't necessary at this project's scale: a handful of join/aggregate queries. Plain SQL keeps the transform logic testable and explainable without a second framework's mental model layered on top.

**Revisit if:** there's real time buffer left after the feature and serving layers are working end-to-end.

---

## Data quality: Great Expectations

**What's actually checked:**
- Referential integrity: every `order_id` in `order_items`/`payments`/`reviews` must exist in `orders`; every `customer_id` in `orders` must exist in `customers`.
- Null thresholds: key columns (`order_id`, `customer_id`, `order_purchase_timestamp`) must not exceed a defined null percentage.
- [TODO: finalize the specific expectation suite once the raw schema is loaded and real data quality issues are visible. Olist is known to have some missing delivery dates and cancelled orders, which need explicit handling, not silent filtering.]

**Why fail loudly instead of logging and continuing:** A DQ check that logs a warning and lets bad data flow into `marts` is decorative. If referential integrity breaks, every downstream join and feature calculation built on it is untrustworthy. Better to stop and fix the raw load than build features on broken joins.

---

## Serving: Metabase (not Power BI, not Streamlit)

**Alternatives considered:** Power BI (originally in the README), Streamlit.

**Why not Power BI:** Power BI Desktop is Windows-only with no free, container-friendly server component. Connecting it to a local Dockerized Postgres instance requires an ODBC/gateway bridge, real setup friction for a solo project on a deadline, and it doesn't demo cleanly in a screen-recorded video the way "docker-compose up, open localhost" does.

**Why not Streamlit:** More control and more "custom code" credit, but costs real app-building time that's better spent on the pipeline itself.

**Why Metabase:** Ships as a Docker container, connects to Postgres with no extra configuration, matches the rest of the stack (everything else is already Dockerized), and demos in two commands.

---

## Customer address resolution in dim_customers

**Problem:** A customer_unique_id can have multiple order-level customer_id records with different recorded city/state values (they moved between orders). Something has to pick one for the dimension table.

**Decision:** City and state are taken from the customer's **most recent order** (`ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp DESC)`), not an arbitrary or alphabetical pick. The rule is: assume the most recent order reflects the customer's current address.

**Why this is defensible where an arbitrary pick isn't:** it's an explicit, explainable assumption tied to real-world reasoning (people's most recent address is more likely to be current than an old one), not a coincidence of how SQL happens to sort strings. `tests/test_transform.py::test_city_state_taken_from_most_recent_order_not_first_alphabetically` encodes this rule directly and would fail if the logic regressed to picking arbitrarily.



## Churn label: time-based holdout, not a recency threshold

**Problem:** Olist has no subscription or cancellation signal, so churn has to be defined from purchase behavior. The obvious first idea, "no repeat order within N days", is a threshold pulled from nowhere, and worse, it's circular: RFM's Recency feature would be measuring the same thing the label is defined by, so a model trained on it would just be restating its own label back, not predicting anything.

**Decision:** Use a time-based holdout split, the standard approach for churn labeling without an explicit churn event:
- A cutoff timestamp is computed from the data itself (80th percentile of order dates across the dataset), not a hardcoded calendar date.
- Every feature (RFM, delivery experience) is computed using only orders on/before the cutoff (the observation window).
- `churned = 1` if a customer ordered on/before the cutoff but placed no order after it (the outcome window). `churned = 0` if they ordered again after the cutoff. This is ground truth from what actually happened, not a guess.
- Customers with no orders on/before the cutoff are excluded entirely, there's no observation-window history to predict from.

**Why this isn't circular:** the label depends on real future behavior (did they order again), not on a property (recency) that's also used as an input feature. `recency_days` is deliberately excluded from the trained feature set for this reason, see `scripts/train_churn_model.py`.

**Model:** Logistic Regression and Random Forest are both trained and compared on ROC-AUC; the better one is kept. The training script raises an error if the best model's AUC doesn't exceed 0.5 (i.e. doesn't beat random guessing), that's a hard sanity gate, not an assumption that the model will work.

**Why accuracy-vs-baseline was rejected as the pass/fail metric:** it was the first approach tried, and it failed correctly, a synthetic test caught that accuracy against a majority-class baseline is unreliable under class imbalance (a model can score lower on raw accuracy than "always predict the majority class" while still having genuine discriminative power). ROC-AUC against 0.5 (random) is the metric that actually answers "did this model learn something."

---

## Order-level geography: real geolocation data, not an invented address

**Problem:** "Where does this customer currently live" isn't cleanly answerable from Olist's data, a customer's recorded city/state can differ across orders, and picking one requires either an arbitrary rule or an assumption about which order reflects "now."

**Decision:** Don't try to answer that question at all. Attach real lat/lng to each **order** instead (every order has one real, unambiguous zip code prefix), using Olist's actual `geolocation` table joined on zip prefix. The only aggregation involved is collapsing multiple raw lat/lng points that share a zip prefix into a centroid (simple average), that's cleaning duplicate measurements, not guessing customer behavior.

**Result:** `order_geography` supports mapping real order locations. There is no "customer's current address" concept in this pipeline, because the data doesn't support answering that question honestly.

---

## Model training kept out of the Airflow DAG

**Decision:** `scripts/train_churn_model.py` runs standalone, not as a DAG task.

**Why:** training a model is a different lifecycle concern than moving and cleaning data, it doesn't need to happen on every pipeline run, and conflating "run the ETL" with "retrain the model" makes both harder to reason about. The DAG's job ends at producing a clean, labeled feature table (`customer_rfm`, `customer_delivery_experience`, `customer_churn_label`); training reads from that table whenever it's actually needed.

**Revisit if:** scheduled retraining becomes a real requirement, at that point it would be its own DAG (or a task group) with its own schedule, separate from the ingestion pipeline's.