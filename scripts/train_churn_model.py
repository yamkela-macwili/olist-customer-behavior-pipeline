"""
Churn model training script.

Assembles a training table from the marts schema (customer_rfm,
customer_delivery_experience, customer_churn_label), trains a logistic
regression and a random forest, evaluates both, and returns the results.

Note: recency_days is intentionally excluded from features — it is derived
from the same cutoff used to define the churn label, so including it would
be circular (the model would be learning from the label definition itself).
See docs/DECISIONS.md for the full reasoning.
"""

import logging
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

FEATURE_COLS = [
    "frequency",
    "monetary",
    "avg_delay_days",
    "late_delivery_rate",
]


def assemble_training_table(engine: Engine, schema: str = None) -> pd.DataFrame:
    """Join RFM, delivery experience, and churn label into one training table.

    Customers missing from any of the three tables are dropped (inner join).
    The label column is `churned` (0 = retained, 1 = churned).
    `recency_days` is present in the output for reference but must NOT be
    passed to the model — see FEATURE_COLS.
    """

    def qualify(table):
        return f'"{schema}"."{table}"' if schema else table

    rfm = pd.read_sql(f"SELECT * FROM {qualify('customer_rfm')}", engine)
    delivery = pd.read_sql(
        f"SELECT * FROM {qualify('customer_delivery_experience')}", engine
    )
    churn = pd.read_sql(
        f"SELECT customer_unique_id, churned FROM {qualify('customer_churn_label')}", engine
    )

    df = (
        churn.merge(rfm, on="customer_unique_id", how="inner")
        .merge(delivery, on="customer_unique_id", how="inner")
    )
    logger.info("Training table assembled: %d rows, %d columns", len(df), len(df.columns))
    return df


def train_and_evaluate(
    df: pd.DataFrame,
    test_size: float = 0.2,
    random_state: int = 42,
) -> tuple[dict[str, Any], Any, Any, list[str]]:
    """Train logistic regression and random forest, return evaluation results.

    Returns:
        results   – dict with model metrics and best_model key
        model     – trained best-model estimator
        scaler    – fitted StandardScaler
        feature_cols – list of feature column names used
    """
    X = df[FEATURE_COLS].fillna(0)
    y = df["churned"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    baseline_accuracy = max(y_test.mean(), 1 - y_test.mean())

    candidates = {
        "logistic_regression": LogisticRegression(max_iter=1000, random_state=random_state),
        "random_forest": RandomForestClassifier(n_estimators=100, random_state=random_state),
    }

    model_results: dict[str, dict] = {}
    for name, clf in candidates.items():
        X_tr = X_train_scaled if name == "logistic_regression" else X_train
        X_te = X_test_scaled if name == "logistic_regression" else X_test
        clf.fit(X_tr, y_train)
        preds = clf.predict(X_te)
        proba = clf.predict_proba(X_te)[:, 1]
        model_results[name] = {
            "accuracy": accuracy_score(y_test, preds),
            "roc_auc": roc_auc_score(y_test, proba),
            "estimator": clf,
        }
        logger.info(
            "%s — accuracy: %.3f, AUC: %.3f",
            name,
            model_results[name]["accuracy"],
            model_results[name]["roc_auc"],
        )

    best_name = max(model_results, key=lambda k: model_results[k]["roc_auc"])
    best_estimator = model_results[best_name]["estimator"]

    results = {
        "models": {k: {m: v for m, v in info.items() if m != "estimator"}
                   for k, info in model_results.items()},
        "best_model": best_name,
        "baseline_accuracy": baseline_accuracy,
    }
    logger.info("Best model: %s (AUC %.3f)", best_name, model_results[best_name]["roc_auc"])
    return results, best_estimator, scaler, FEATURE_COLS
