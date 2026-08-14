"""
Raw-layer ingestion: read source CSVs in chunks and load into the raw schema.

Kept separate from the DAG file on purpose, this module has no Airflow
dependency, so it can be tested with plain pytest and imported into the DAG
as a thin wrapper. Testing DAG-decorated functions directly is awkward,
testing plain functions is not.
"""

from pathlib import Path
from typing import Iterator, Union

import pandas as pd
from sqlalchemy.engine import Engine

from src.config.settings import CHUNK_SIZE
from src.common.utils import read_csv_chunks


def load_csv_to_raw(
    csv_path: Union[str, Path],
    table_name: str,
    engine: Engine,
    schema: str = None,
    chunk_size: int = 5000,
) -> int:
    """Load a source CSV into a raw-layer table, chunked.

    The first chunk replaces the table (if_exists="replace"), subsequent
    chunks append. This makes reloads idempotent, rerunning against the same
    file produces the same row count, rather than duplicating rows on every
    retry, which matters for an Airflow task that may be retried.

    Returns the total number of rows loaded.
    """
    if schema and engine.dialect.name != "sqlite":
        with engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    total_rows = 0
    for i, chunk in enumerate(read_csv_chunks(csv_path, chunk_size=chunk_size)):
        chunk.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="replace" if i == 0 else "append",
            index=False,
        )
        total_rows += len(chunk)
    return total_rows


def verify_row_count(
    csv_path: Union[str, Path],
    table_name: str,
    engine: Engine,
    schema: str = None,
) -> None:
    """Raise ValueError if the loaded table's row count doesn't match the
    source CSV's row count.

    Deliberately a hard failure, not a logged warning, a row-count mismatch
    means the load is silently incomplete and nothing downstream should run
    against it.
    """
    source_rows = sum(len(chunk) for chunk in read_csv_chunks(csv_path))

    full_table_name = f'"{schema}"."{table_name}"' if schema else table_name
    with engine.connect() as conn:
        loaded_rows = conn.exec_driver_sql(
            f"SELECT COUNT(*) FROM {full_table_name}"
        ).scalar()

    if source_rows != loaded_rows:
        raise ValueError(
            f"Row count mismatch for {table_name}: "
            f"source CSV has {source_rows} rows, loaded table has {loaded_rows} rows"
        )


if __name__ == "__main__":
    import os
    import socket
    from pathlib import Path

    from sqlalchemy import create_engine

    # Use localhost if running this script on your machine.
    # Use "postgres" if running inside an Airflow container.
    db_uri = os.getenv("DATABASE_URL")
    if not db_uri:
        try:
            socket.gethostbyname("postgres")
            db_uri = "postgresql+psycopg2://airflow:airflow@postgres:5432/olist"
        except socket.gaierror:
            db_uri = "postgresql+psycopg2://airflow:airflow@localhost:5435/olist"

    engine = create_engine(db_uri)

    csv_path = Path("data/olist_customers_dataset.csv")
    table_name = "customers"
    schema = "raw"

    rows = load_csv_to_raw(
        csv_path=csv_path,
        table_name=table_name,
        engine=engine,
        schema=schema,
        chunk_size=CHUNK_SIZE,
    )

    print(f"Loaded {rows} rows into raw.{table_name}")

    verify_row_count(
        csv_path=csv_path,
        table_name=table_name,
        engine=engine,
        schema=schema,
    )

    print("✓ Row count verification passed.")