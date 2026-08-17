"""
Tests for the raw-layer CSV loader.

Written before include/ingestion.py exists. These should fail on first run
with ImportError, then pass once the module is implemented.

Uses an in-memory SQLite engine to test loading logic without requiring a
running Postgres instance, schema-qualified writes are exercised separately
where SQLite's limitations matter (SQLite has no real schema support, so
schema-specific behavior is tested by asserting the function accepts and
passes through a schema argument, not by inspecting SQLite internals).
"""

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect

from include.ingestion import load_csv_to_raw, verify_row_count
from include.utils import read_csv_chunks


@pytest.fixture
def sqlite_engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def sample_csv(tmp_path):
    """12 rows, used to test chunking behavior against chunk_size=5."""
    csv_path = tmp_path / "sample.csv"
    df = pd.DataFrame({
        "id": range(1, 13),
        "value": [f"row_{i}" for i in range(1, 13)],
    })
    df.to_csv(csv_path, index=False)
    return csv_path


class TestReadCsvChunks:
    def test_yields_correct_number_of_chunks(self, sample_csv):
        chunks = list(read_csv_chunks(sample_csv, chunk_size=5))
        # 12 rows at chunk_size=5 -> chunks of 5, 5, 2
        assert len(chunks) == 3
        assert [len(c) for c in chunks] == [5, 5, 2]

    def test_chunks_preserve_all_rows(self, sample_csv):
        chunks = list(read_csv_chunks(sample_csv, chunk_size=5))
        total_rows = sum(len(c) for c in chunks)
        assert total_rows == 12

    def test_missing_file_raises(self, tmp_path):
        missing_path = tmp_path / "does_not_exist.csv"
        with pytest.raises(FileNotFoundError):
            list(read_csv_chunks(missing_path, chunk_size=5))


class TestLoadCsvToRaw:
    def test_loads_all_rows_into_target_table(self, sample_csv, sqlite_engine):
        load_csv_to_raw(
            csv_path=sample_csv,
            table_name="raw_sample",
            engine=sqlite_engine,
            chunk_size=5,
        )
        result = pd.read_sql_table("raw_sample", sqlite_engine)
        assert len(result) == 12

    def test_table_exists_after_load(self, sample_csv, sqlite_engine):
        load_csv_to_raw(
            csv_path=sample_csv,
            table_name="raw_sample",
            engine=sqlite_engine,
            chunk_size=5,
        )
        inspector = inspect(sqlite_engine)
        assert "raw_sample" in inspector.get_table_names()

    def test_missing_file_raises(self, tmp_path, sqlite_engine):
        missing_path = tmp_path / "does_not_exist.csv"
        with pytest.raises(FileNotFoundError):
            load_csv_to_raw(
                csv_path=missing_path,
                table_name="raw_sample",
                engine=sqlite_engine,
                chunk_size=5,
            )

    def test_second_load_replaces_not_duplicates(self, sample_csv, sqlite_engine):
        """Re-running a load (e.g. DAG retry) should not silently double the data."""
        load_csv_to_raw(sample_csv, "raw_sample", sqlite_engine, chunk_size=5)
        load_csv_to_raw(sample_csv, "raw_sample", sqlite_engine, chunk_size=5)
        result = pd.read_sql_table("raw_sample", sqlite_engine)
        assert len(result) == 12


class TestVerifyRowCount:
    def test_passes_when_counts_match(self, sample_csv, sqlite_engine):
        load_csv_to_raw(sample_csv, "raw_sample", sqlite_engine, chunk_size=5)
        # should not raise
        verify_row_count(sample_csv, "raw_sample", sqlite_engine)

    def test_raises_when_counts_mismatch(self, sample_csv, sqlite_engine):
        load_csv_to_raw(sample_csv, "raw_sample", sqlite_engine, chunk_size=5)
        # simulate a partial/corrupted load by deleting a row after load
        with sqlite_engine.connect() as conn:
            conn.exec_driver_sql("DELETE FROM raw_sample WHERE id = 1")
            conn.commit()
        with pytest.raises(ValueError):
            verify_row_count(sample_csv, "raw_sample", sqlite_engine)