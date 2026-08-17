"""
Common utilities: CSV chunked reader used by the ingestion layer.
"""

import logging
from pathlib import Path
from typing import Iterator, Union

import pandas as pd

logger = logging.getLogger(__name__)


def read_csv_chunks(
    csv_path: Union[str, Path], chunk_size: int = 5000
) -> Iterator[pd.DataFrame]:
    """Yield a source CSV in chunks of `chunk_size` rows.

    Raises FileNotFoundError immediately if the file doesn't exist, rather
    than failing lazily on first iteration — callers should be able to detect
    a bad path before committing to a load.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    logger.debug("Reading %s in chunks of %d", csv_path.name, chunk_size)
    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        yield chunk
