"""
Integration test for the churn training pipeline.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from train_churn_model import assemble_training_table, train_and_evaluate


@pytest.fixture
def sqlite_engine_with_signal():
    """Build 200 synthetic customers with genuine, learnable signal:
    low frequency/monetary + high late-delivery rate -> more likely churned.
    This isn't the real Olist data, it's a controlled fixture to prove the
    training code path works and can find signal when signal exists.
    """
    rng = np.random.default_rng(42)
    n = 200

    frequency = rng.integers(1, 10, n)
    monetary = frequency * rng.uniform(20, 100, n)
    late_rate = rng.uniform(0, 1, n)
    avg_delay = rng.uniform(-5, 15, n)

    # churn driven by real signal (low frequency, high late-delivery rate),
    # median-split to guarantee a roughly balanced label regardless of the
    # exact coefficients, avoids the test being fragile to class imbalance
    z = -0.3 * frequency + 2.0 * late_rate + rng.normal(0, 0.5, n)
    churned = (z > np.median(z)).astype(int)

    engine = create_engine("sqlite:///:memory:")

    pd.DataFrame({
        "customer_unique_id": [f"u{i}" for i in range(n)],
        "frequency": frequency,
        "monetary": monetary,
        "recency_days": rng.integers(0, 365, n),
    }).to_sql("customer_rfm", engine, index=False)

    pd.DataFrame({
        "customer_unique_id": [f"u{i}" for i in range(n)],
        "avg_delay_days": avg_delay,
        "late_delivery_rate": late_rate,
    }).to_sql("customer_delivery_experience", engine, index=False)

    pd.DataFrame({
        "customer_unique_id": [f"u{i}" for i in range(n)],
        "churned": churned,
    }).to_sql("customer_churn_label", engine, index=False)

    return engine


class TestTrainingPipeline:
    def test_assembles_training_table_with_expected_columns(self, sqlite_engine_with_signal):
        df = assemble_training_table(sqlite_engine_with_signal, schema=None)
        assert len(df) == 200
        assert set(["customer_unique_id", "churned", "frequency", "monetary",
                     "avg_delay_days", "late_delivery_rate"]).issubset(df.columns)

    def test_trained_model_beats_baseline_on_signal_bearing_data(self, sqlite_engine_with_signal):
        df = assemble_training_table(sqlite_engine_with_signal, schema=None)
        results, model, scaler, feature_cols = train_and_evaluate(df)

        best = results["models"][results["best_model"]]
        assert best["accuracy"] > results["baseline_accuracy"]
        assert best["roc_auc"] > 0.5

    def test_recency_days_not_used_as_a_feature(self, sqlite_engine_with_signal):
        """Guards against reintroducing the leakage this design avoided,
        recency_days must never end up in the trained feature set."""
        df = assemble_training_table(sqlite_engine_with_signal, schema=None)
        _, _, _, feature_cols = train_and_evaluate(df)
        assert "recency_days" not in feature_cols