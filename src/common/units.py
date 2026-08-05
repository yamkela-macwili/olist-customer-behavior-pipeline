from typing import Iterator, Union
from pathlib import Path
import pandas as pd
from sqlalchemy.engine import Engine


def read_csv_chunks(
    csv_path: Union[str, Path], chunk_size: int = 5000
) -> Iterator[pd.DataFrame]:
    """Yield a source CSV in chunks of `chunk_size` rows.

    Raises FileNotFoundError immediately if the file doesn't exist, rather
    than failing lazily on first iteration, callers should be able to detect
    a bad path before committing to a load.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        yield chunk


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
